from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"target_filename", "round", "game", "map", "mech", "match_id"}
OUTPUT_COLUMNS = [
    "filename",
    "title",
    "description",
    "tags",
    "round",
    "game",
    "map",
    "mech",
    "match_id",
]
DEFAULT_TAGS = "mwo, mechwarrior online, mwo competitive, mechwarrior, gaming"
ROMAN_NUMERALS = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a YouTube upload metadata CSV from output/rename_plan.csv."
    )
    parser.add_argument(
        "--rename_plan_csv",
        default="output/rename_plan.csv",
        help="Path to the rename plan CSV.",
    )
    parser.add_argument(
        "--output_csv",
        default="output/youtube_upload_plan.csv",
        help="Path to write the YouTube upload plan CSV.",
    )
    return parser.parse_args()


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        LOGGER.error("Missing required columns in %s: %s", source_name, ", ".join(missing_columns))
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def load_rename_plan(path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(path, dtype=str).fillna("")
    ensure_columns(dataframe, REQUIRED_COLUMNS, path.name)
    dataframe["round"] = pd.to_numeric(dataframe["round"], errors="raise").astype(int)
    dataframe["game"] = pd.to_numeric(dataframe["game"], errors="raise").astype(int)
    LOGGER.info("Loaded %s rename-plan rows from %s", len(dataframe), path)
    return dataframe


def title_case_map(map_name: str) -> str:
    words = []
    for word in str(map_name).strip().split():
        upper_word = word.upper()
        if upper_word in ROMAN_NUMERALS:
            words.append(upper_word)
        else:
            words.append(word.capitalize())
    return " ".join(words)


def normalize_mech(mech: str) -> str:
    mech_value = str(mech).strip()
    return mech_value.upper() if mech_value else "UNKNOWN_MECH"


def build_title(mech: str, map_name: str, round_number: int, game_number: int) -> str:
    return f"{mech} on {map_name} | Round {round_number} Game {game_number} | MWO Competitive"


def build_description(mech: str, map_name: str, round_number: int, game_number: int) -> str:
    return (
        "MWO Competitive Match\n\n"
        f"Round {round_number}, Game {game_number}\n"
        f"Map: {map_name}\n"
        f"Mech: {mech}\n\n"
        "Full competitive gameplay from organized MWO matches.\n\n"
        "#MWO #MechWarrior #MWOComp"
    )


def build_upload_plan(rename_plan_df: pd.DataFrame) -> pd.DataFrame:
    output_rows: list[dict[str, object]] = []
    for row in rename_plan_df.itertuples(index=False):
        mech = normalize_mech(row.mech)
        map_name = title_case_map(str(row.map))
        title = build_title(mech, map_name, int(row.round), int(row.game))
        description = build_description(mech, map_name, int(row.round), int(row.game))

        output_rows.append(
            {
                "filename": str(row.target_filename).strip(),
                "title": title,
                "description": description,
                "tags": DEFAULT_TAGS,
                "round": int(row.round),
                "game": int(row.game),
                "map": map_name,
                "mech": mech,
                "match_id": str(row.match_id).strip(),
            }
        )

    output_df = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)
    LOGGER.info("Built %s YouTube upload rows", len(output_df))
    return output_df


def write_output(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(dataframe), output_path)


def main() -> int:
    configure_logging()
    args = parse_args()

    rename_plan_csv = Path(args.rename_plan_csv).expanduser()
    output_csv = Path(args.output_csv).expanduser()

    rename_plan_df = load_rename_plan(rename_plan_csv)
    upload_plan_df = build_upload_plan(rename_plan_df)
    write_output(upload_plan_df, output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
