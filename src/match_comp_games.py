from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

EXPECTED_FILENAME_FORMAT = "%Y-%m-%d %H-%M-%S"
SUPPORTED_EXTENSIONS = {".mp4", ".mkv"}
COMPETITIVE_DATES = {
    date(2026, 2, 21): 1,
    date(2026, 2, 28): 2,
    date(2026, 3, 7): 3,
    date(2026, 3, 14): 4,
    date(2026, 3, 21): 5,
}
OUTPUT_COLUMNS = [
    "obs_file",
    "full_path",
    "obs_datetime",
    "obs_date",
    "datetime_source",
    "round",
    "game",
    "map",
    "mech",
    "status",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a review CSV for MechWarrior Online competition OBS recordings."
    )
    parser.add_argument(
        "--video_dir",
        required=True,
        help=(
            r'Path to the directory containing OBS recordings, for example '
            r'"G:\Videos\aasecond wave".'
        ),
    )
    return parser.parse_args()


def parse_obs_datetime_from_filename(video_path: Path) -> datetime | None:
    try:
        return datetime.strptime(video_path.stem, EXPECTED_FILENAME_FORMAT)
    except ValueError:
        return None


def get_obs_datetime_from_metadata(video_path: Path) -> datetime:
    stats = video_path.stat()
    timestamp = stats.st_ctime or stats.st_mtime
    return datetime.fromtimestamp(timestamp)


def get_obs_datetime(video_path: Path) -> tuple[datetime, str]:
    obs_datetime = parse_obs_datetime_from_filename(video_path)
    if obs_datetime is not None:
        return obs_datetime, "filename"

    obs_datetime = get_obs_datetime_from_metadata(video_path)
    LOGGER.warning(
        "Using metadata datetime for renamed or non-standard file: %s -> %s",
        video_path.name,
        obs_datetime.isoformat(sep=" "),
    )
    return obs_datetime, "metadata"


def scan_video_records(video_dir: Path) -> list[dict[str, object]]:
    if not video_dir.exists():
        raise FileNotFoundError(f"Video directory does not exist: {video_dir}")
    if not video_dir.is_dir():
        raise NotADirectoryError(f"Video directory is not a directory: {video_dir}")

    records: list[dict[str, object]] = []
    video_paths = sorted(
        path for path in video_dir.iterdir() if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    for video_path in video_paths:
        obs_datetime, datetime_source = get_obs_datetime(video_path)

        obs_date = obs_datetime.date()
        round_number = COMPETITIVE_DATES.get(obs_date)
        if round_number is None:
            continue

        records.append(
            {
                "obs_file": video_path.name,
                "full_path": str(video_path.resolve()),
                "obs_datetime": obs_datetime,
                "obs_date": obs_date,
                "datetime_source": datetime_source,
                "round": round_number,
            }
        )

    return records


def assign_game_numbers(records: list[dict[str, object]]) -> list[dict[str, object]]:
    records_by_date: dict[date, list[dict[str, object]]] = {}
    for record in records:
        records_by_date.setdefault(record["obs_date"], []).append(record)

    finalized_records: list[dict[str, object]] = []
    for obs_date in sorted(records_by_date):
        dated_records = sorted(records_by_date[obs_date], key=lambda item: item["obs_datetime"])
        if len(dated_records) > 5:
            LOGGER.warning(
                "Found %s recordings for %s; assigning game numbers beyond 5 in chronological order.",
                len(dated_records),
                obs_date.isoformat(),
            )
        for game_number, record in enumerate(dated_records, start=1):
            enriched_record = dict(record)
            enriched_record["game"] = game_number
            enriched_record["map"] = ""
            enriched_record["mech"] = ""
            enriched_record["status"] = "auto"
            finalized_records.append(enriched_record)

    return finalized_records


def build_matches_dataframe(records: list[dict[str, object]]) -> pd.DataFrame:
    finalized_records = assign_game_numbers(records)
    if not finalized_records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    dataframe = pd.DataFrame(finalized_records)
    dataframe["obs_datetime"] = pd.to_datetime(dataframe["obs_datetime"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    dataframe["obs_date"] = pd.to_datetime(dataframe["obs_date"]).dt.strftime("%Y-%m-%d")
    return dataframe.loc[:, OUTPUT_COLUMNS]


def write_output_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(dataframe), output_path)


def main() -> int:
    configure_logging()
    args = parse_args()

    video_dir = Path(args.video_dir).expanduser()
    output_path = Path(__file__).resolve().parents[1] / "data" / "matches_review.csv"

    records = scan_video_records(video_dir)
    dataframe = build_matches_dataframe(records)
    write_output_csv(dataframe, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
