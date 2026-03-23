from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"current_title", "new_title"}
OUTPUT_COLUMNS = [
    "video_id",
    "current_title",
    "new_title",
    "description",
    "tags",
    "playlist",
    "audience",
    "visibility",
    "thumbnail_path",
    "publish_at_local",
    "publish_at_iso",
    "publish_timezone",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add scheduled publish timestamps to youtube_metadata_updates.csv.",
    )
    parser.add_argument(
        "--input_csv",
        default="output/youtube_metadata_updates.csv",
        help="Metadata update CSV to enrich with publish schedule columns.",
    )
    parser.add_argument(
        "--output_csv",
        default="output/youtube_metadata_updates.csv",
        help="Output CSV path. Defaults to updating the input file in place.",
    )
    parser.add_argument(
        "--start_at",
        default="",
        help="First publish time in local time, for example '2026-03-23 15:00'. Defaults to the next top-of-hour.",
    )
    parser.add_argument(
        "--interval_hours",
        type=int,
        default=9,
        help="Hours between publishes.",
    )
    parser.add_argument(
        "--timezone",
        default="America/Chicago",
        help="IANA timezone label used for schedule timestamps.",
    )
    return parser.parse_args()


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def parse_start_datetime(start_at: str, timezone_name: str) -> datetime:
    timezone = ZoneInfo(timezone_name)
    if start_at.strip():
        normalized = start_at.strip().replace("T", " ")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:  # noqa: BLE001
            raise ValueError(f"Unsupported --start_at value: {start_at}") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone)
        return parsed.astimezone(timezone)

    now_local = datetime.now(timezone)
    rounded = now_local.replace(minute=0, second=0, microsecond=0)
    if rounded <= now_local:
        rounded += timedelta(hours=1)
    return rounded


def build_schedule_rows(count: int, start_at: datetime, interval_hours: int) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index in range(count):
        scheduled = start_at + timedelta(hours=index * interval_hours)
        rows.append(
            {
                "publish_at_local": scheduled.strftime("%Y-%m-%d %H:%M"),
                "publish_at_iso": scheduled.isoformat(timespec="minutes"),
                "publish_timezone": str(scheduled.tzinfo),
            }
        )
    return rows


def apply_schedule(dataframe: pd.DataFrame, start_at: datetime, interval_hours: int) -> pd.DataFrame:
    if interval_hours < 1:
        raise ValueError("interval_hours must be >= 1")

    scheduled_rows = build_schedule_rows(len(dataframe), start_at=start_at, interval_hours=interval_hours)
    scheduled_df = pd.DataFrame(scheduled_rows)
    merged = dataframe.copy()
    for column in scheduled_df.columns:
        merged[column] = scheduled_df[column]

    for column in OUTPUT_COLUMNS:
        if column not in merged.columns:
            merged[column] = ""
    return merged[OUTPUT_COLUMNS]


def main() -> int:
    configure_logging()
    args = parse_args()

    input_csv = Path(args.input_csv).expanduser().resolve()
    output_csv = Path(args.output_csv).expanduser().resolve()

    dataframe = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    ensure_columns(dataframe, REQUIRED_COLUMNS, input_csv.name)

    start_at = parse_start_datetime(args.start_at, timezone_name=args.timezone)
    scheduled_df = apply_schedule(dataframe, start_at=start_at, interval_hours=args.interval_hours)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    scheduled_df.to_csv(output_csv, index=False)

    LOGGER.info(
        "Scheduled %s row(s) every %s hour(s) starting at %s -> %s",
        len(scheduled_df),
        args.interval_hours,
        start_at.isoformat(timespec="minutes"),
        output_csv,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
