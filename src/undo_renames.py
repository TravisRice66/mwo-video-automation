from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"new_path", "original_path"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Undo or preview video renames from output/undo_rename_plan.csv."
    )
    parser.add_argument(
        "--undo_csv",
        default="output/undo_rename_plan.csv",
        help="Path to the undo rename plan CSV.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the undo renames. Without this flag the script runs in dry-run mode.",
    )
    return parser.parse_args()


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        LOGGER.error("Missing required columns in %s: %s", source_name, ", ".join(missing_columns))
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def load_plan(undo_csv: Path) -> pd.DataFrame:
    if not undo_csv.exists():
        raise FileNotFoundError(f"Undo rename plan does not exist: {undo_csv}")
    dataframe = pd.read_csv(undo_csv, dtype=str).fillna("")
    ensure_columns(dataframe, REQUIRED_COLUMNS, undo_csv.name)
    LOGGER.info("Loaded %s undo rows from %s", len(dataframe), undo_csv)
    return dataframe


def process_plan(plan_df: pd.DataFrame, apply_changes: bool) -> tuple[int, int, int]:
    total_rows = len(plan_df)
    successful_renames = 0
    skipped_rows = 0

    for row in plan_df.itertuples(index=False):
        new_path = Path(str(row.new_path))
        original_path = Path(str(row.original_path))

        if not str(row.new_path).strip() or not str(row.original_path).strip():
            skipped_rows += 1
            LOGGER.info("Skipping row with blank undo path values.")
            continue

        if not new_path.exists():
            skipped_rows += 1
            LOGGER.warning("Skipping missing renamed file: %s", new_path)
            continue

        if original_path.exists():
            skipped_rows += 1
            LOGGER.warning("Skipping undo because original path already exists: %s", original_path)
            continue

        print(f"{new_path} -> {original_path}")

        if not apply_changes:
            successful_renames += 1
            continue

        original_path.parent.mkdir(parents=True, exist_ok=True)
        new_path.rename(original_path)
        LOGGER.info("Reverted %s -> %s", new_path, original_path)
        successful_renames += 1

    return total_rows, successful_renames, skipped_rows


def main() -> int:
    configure_logging()
    args = parse_args()

    undo_csv = Path(args.undo_csv).expanduser()
    try:
        plan_df = load_plan(undo_csv)
    except FileNotFoundError as error:
        LOGGER.warning("%s", error)
        LOGGER.info("Run apply_rename_plan.py with --apply first to generate %s.", undo_csv)
        return 1

    if args.apply:
        LOGGER.info("Apply mode enabled; undo renames will be performed.")
    else:
        LOGGER.info("Dry-run mode enabled; no files will be modified.")

    total_rows, successful_renames, skipped_rows = process_plan(plan_df, args.apply)

    LOGGER.info("Summary: total rows = %s", total_rows)
    LOGGER.info("Summary: successful renames = %s", successful_renames)
    LOGGER.info("Summary: skipped rows = %s", skipped_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
