from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

TEMPLATE_COLUMNS = [
    "video_id",
    "current_title",
    "new_title",
    "description",
    "tags",
    "playlist",
    "audience",
    "visibility",
    "thumbnail_path",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a YouTube metadata update template CSV from youtube_upload_plan.csv.",
    )
    parser.add_argument(
        "--input_csv",
        default="output/youtube_upload_plan.csv",
        help="Path to existing upload metadata plan CSV.",
    )
    parser.add_argument(
        "--output_csv",
        default="output/youtube_metadata_updates.csv",
        help="Path to write update template CSV.",
    )
    parser.add_argument(
        "--default_audience",
        default="not_made_for_kids",
        help="Audience value to prefill in template.",
    )
    return parser.parse_args()


def main() -> int:
    configure_logging()
    args = parse_args()

    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)

    dataframe = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    missing = [column for column in ["filename", "title", "description", "tags"] if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    upload_lookup_titles = dataframe["filename"].map(lambda value: Path(str(value).strip()).stem)

    template = pd.DataFrame(
        {
            "video_id": "",
            "current_title": upload_lookup_titles,
            "new_title": dataframe["title"],
            "description": dataframe["description"],
            "tags": dataframe["tags"],
            "playlist": "",
            "audience": args.default_audience,
            "visibility": "",
            "thumbnail_path": "",
        }
    )

    template = template[TEMPLATE_COLUMNS]
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(output_csv, index=False)

    LOGGER.info("Wrote %s template rows to %s", len(template), output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

