from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"source_path", "target_filename", "status"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply or preview video renames from output/rename_plan.csv."
    )
    parser.add_argument(
        "--plan_csv",
        default="output/rename_plan.csv",
        help="Path to the rename plan CSV.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the renames. Without this flag the script runs in dry-run mode.",
    )
    return parser.parse_args()


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        LOGGER.error("Missing required columns in %s: %s", source_name, ", ".join(missing_columns))
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def load_plan(plan_csv: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(plan_csv, dtype=str).fillna("")
    ensure_columns(dataframe, REQUIRED_COLUMNS, plan_csv.name)
    LOGGER.info("Loaded %s rename-plan rows from %s", len(dataframe), plan_csv)
    return dataframe


def resolve_target_path(source_path: Path, target_filename: str) -> Path:
    return source_path.with_name(target_filename)


def resolve_collision(target_path: Path) -> Path:
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    counter = 1
    while True:
        candidate = target_path.with_name(f"{stem}_dup{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def should_skip_row(source_path_raw: str, target_filename: str, status: str) -> str | None:
    if not source_path_raw.strip():
        return "missing source_path in rename plan"
    if not target_filename.strip():
        return "missing target_filename in rename plan"
    if status.strip() != "auto_matched":
        return f"status={status or 'blank'}"
    return None


def process_plan(plan_df: pd.DataFrame, apply_changes: bool) -> tuple[int, int, int]:
    total_files = len(plan_df)
    renamed_files = 0
    skipped_files = 0

    for row in plan_df.itertuples(index=False):
        source_path_raw = str(row.source_path)
        target_filename = str(row.target_filename).strip()
        status = str(row.status).strip()

        skip_reason = should_skip_row(source_path_raw, target_filename, status)
        if skip_reason:
            skipped_files += 1
            LOGGER.info("Skipping row: %s", skip_reason)
            continue

        source_path = Path(source_path_raw)
        if not source_path.exists():
            skipped_files += 1
            LOGGER.warning("Skipping missing source file: %s", source_path)
            continue

        desired_target = resolve_target_path(source_path, target_filename)
        final_target = resolve_collision(desired_target)

        if source_path.resolve() == final_target.resolve():
            skipped_files += 1
            LOGGER.info("Skipping unchanged file: %s", source_path)
            continue

        if not apply_changes:
            print(f"{source_path} -> {final_target}")
            renamed_files += 1
            continue

        final_target.parent.mkdir(parents=True, exist_ok=True)
        source_path.rename(final_target)
        LOGGER.info("Renamed %s -> %s", source_path, final_target)
        renamed_files += 1

    return total_files, renamed_files, skipped_files


def main() -> int:
    configure_logging()
    args = parse_args()

    plan_csv = Path(args.plan_csv).expanduser()
    plan_df = load_plan(plan_csv)
    if args.apply:
        LOGGER.info("Apply mode enabled; files will be renamed.")
    else:
        LOGGER.info("Dry-run mode enabled; no files will be modified.")
    total_files, renamed_files, skipped_files = process_plan(plan_df, args.apply)

    LOGGER.info("Summary: total files = %s", total_files)
    LOGGER.info("Summary: renamed files = %s", renamed_files)
    LOGGER.info("Summary: skipped files = %s", skipped_files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
