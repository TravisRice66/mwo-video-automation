from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

EXPECTED_VIDEO_FILENAME_FORMAT = "%Y-%m-%d %H-%M-%S"
ROUND_TO_COMP_DATE = {
    1: "2026-02-21",
    2: "2026-02-28",
    3: "2026-03-07",
    4: "2026-03-14",
    5: "2026-03-21",
}
REQUIRED_MATCH_COLUMNS = {
    "match_id",
    "game_time",
    "player_name",
    "mech",
    "round",
    "game",
    "map",
}
VIDEO_REVIEW_COLUMNS = [
    "obs_file",
    "full_path",
    "obs_datetime",
    "datetime_source",
    "match_id",
    "game_time",
    "mech",
    "round",
    "game",
    "map",
    "confidence",
    "match_status",
    "notes",
]
RENAME_PLAN_COLUMNS = [
    "source_path",
    "target_filename",
    "round",
    "game",
    "map",
    "mech",
    "match_id",
    "status",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match competitive MWO recordings to enriched match rows and build a rename plan."
    )
    parser.add_argument(
        "--video_dir",
        required=True,
        help=r'Root directory containing OBS .mkv recordings, for example "G:\Videos\aasecond wave".',
    )
    parser.add_argument(
        "--matches_csv",
        default="data/comp_matches_enriched.csv",
        help="Path to comp_matches_enriched.csv.",
    )
    parser.add_argument(
        "--review_output",
        default="data/video_matches_review.csv",
        help="Path for the review CSV output.",
    )
    parser.add_argument(
        "--rename_output",
        default="output/rename_plan.csv",
        help="Path for the rename plan CSV output.",
    )
    return parser.parse_args()


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        LOGGER.error("Missing required columns in %s: %s", source_name, ", ".join(missing_columns))
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def parse_datetime_from_filename(video_path: Path) -> datetime | None:
    try:
        return datetime.strptime(video_path.stem, EXPECTED_VIDEO_FILENAME_FORMAT)
    except ValueError:
        return None


def scan_videos(video_dir: Path) -> pd.DataFrame:
    if not video_dir.exists():
        raise FileNotFoundError(f"Video directory does not exist: {video_dir}")
    if not video_dir.is_dir():
        raise NotADirectoryError(f"Video directory is not a directory: {video_dir}")

    records: list[dict[str, object]] = []
    for video_path in sorted(video_dir.rglob("*.mkv")):
        obs_datetime = parse_datetime_from_filename(video_path)
        datetime_source = "filename"
        if obs_datetime is None:
            obs_datetime = datetime.fromtimestamp(video_path.stat().st_mtime)
            datetime_source = "metadata"

        records.append(
            {
                "obs_file": video_path.name,
                "full_path": str(video_path.resolve()),
                "obs_datetime": obs_datetime,
                "datetime_source": datetime_source,
                "obs_date": obs_datetime.date().isoformat(),
            }
        )

    dataframe = pd.DataFrame(records)
    if dataframe.empty:
        return pd.DataFrame(columns=["obs_file", "full_path", "obs_datetime", "datetime_source", "obs_date"])

    dataframe = dataframe.sort_values(["obs_datetime", "full_path"]).reset_index(drop=True)
    LOGGER.info("Scanned %s .mkv videos from %s", len(dataframe), video_dir)
    return dataframe


def load_comp_matches(path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(path)
    ensure_columns(dataframe, REQUIRED_MATCH_COLUMNS, path.name)

    dataframe = dataframe.copy()
    dataframe["match_id"] = dataframe["match_id"].astype(str).str.strip()
    dataframe["game_time"] = pd.to_datetime(dataframe["game_time"], errors="coerce")
    dataframe["round"] = pd.to_numeric(dataframe["round"], errors="raise").astype(int)
    dataframe["game"] = pd.to_numeric(dataframe["game"], errors="raise").astype(int)
    dataframe["mech"] = dataframe["mech"].fillna("").astype(str).str.strip()
    dataframe["map"] = dataframe["map"].fillna("").astype(str).str.strip()
    dataframe["comp_date"] = dataframe["round"].map(ROUND_TO_COMP_DATE)

    missing_dates = dataframe["comp_date"].isna().sum()
    if missing_dates:
        LOGGER.warning("Found %s match rows with no round-to-date mapping.", int(missing_dates))

    dataframe = dataframe.sort_values(["game_time", "round", "game"]).reset_index(drop=True)
    LOGGER.info("Loaded %s enriched competitive matches from %s", len(dataframe), path)
    return dataframe


def minutes_from_first(datetimes: list[datetime]) -> list[float]:
    if not datetimes:
        return []
    first_dt = datetimes[0]
    return [(value - first_dt).total_seconds() / 60.0 for value in datetimes]


def score_window(match_times: list[datetime], video_times: list[datetime]) -> float:
    match_minutes = minutes_from_first(match_times)
    video_minutes = minutes_from_first(video_times)
    spacing_score = sum(abs(match_minute - video_minute) for match_minute, video_minute in zip(match_minutes, video_minutes))

    offsets = [
        (match_time - video_time).total_seconds() / 60.0
        for match_time, video_time in zip(match_times, video_times)
    ]
    median_offset = sorted(offsets)[len(offsets) // 2]
    offset_score = sum(abs(offset - median_offset) for offset in offsets)
    return spacing_score + (0.25 * offset_score)


def choose_candidate_window(date_matches: pd.DataFrame, date_videos: pd.DataFrame) -> tuple[list[int], float, float | None]:
    match_count = len(date_matches)
    video_count = len(date_videos)

    if match_count == 0 or video_count == 0:
        return [], float("inf"), None

    if video_count <= match_count:
        positions = list(range(video_count))
        score = score_window(
            date_matches["game_time"].tolist()[:video_count],
            date_videos["obs_datetime"].tolist(),
        )
        return positions, score, None

    match_times = date_matches["game_time"].tolist()
    window_scores: list[tuple[float, list[int]]] = []
    for start in range(0, video_count - match_count + 1):
        window = date_videos.iloc[start : start + match_count]
        score = score_window(match_times, window["obs_datetime"].tolist())
        window_scores.append((score, list(range(start, start + match_count))))

    window_scores.sort(key=lambda item: (item[0], item[1][0]))
    best_score, best_indices = window_scores[0]
    second_best_score = window_scores[1][0] if len(window_scores) > 1 else None
    return best_indices, best_score, second_best_score


def determine_confidence(
    date_video_count: int,
    date_match_count: int,
    best_score: float,
    second_best_score: float | None,
    datetime_source: str,
) -> tuple[str, str]:
    ambiguous_window = second_best_score is not None and (second_best_score - best_score) <= 5.0
    has_extra_videos = date_video_count > date_match_count

    if ambiguous_window:
        return "low", "window alignment close to another candidate set"

    if datetime_source == "metadata" and has_extra_videos:
        return "medium", "matched using metadata timestamp with extra same-day videos present"

    if best_score <= 15.0 and datetime_source == "filename":
        return "high", "strong chronological alignment"

    if best_score <= 30.0:
        return "medium", "reasonable chronological alignment"

    return "low", "weak chronological alignment"


def build_review_row(
    video_row: pd.Series | None,
    match_row: pd.Series | None,
    confidence: str,
    match_status: str,
    notes: str,
) -> dict[str, object]:
    return {
        "obs_file": "" if video_row is None else video_row["obs_file"],
        "full_path": "" if video_row is None else video_row["full_path"],
        "obs_datetime": "" if video_row is None else video_row["obs_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        "datetime_source": "" if video_row is None else video_row["datetime_source"],
        "match_id": "" if match_row is None else str(match_row["match_id"]),
        "game_time": "" if match_row is None else match_row["game_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "mech": "" if match_row is None else match_row["mech"],
        "round": "" if match_row is None else int(match_row["round"]),
        "game": "" if match_row is None else int(match_row["game"]),
        "map": "" if match_row is None else match_row["map"],
        "confidence": confidence,
        "match_status": match_status,
        "notes": notes,
    }


def sanitize_filename_part(value: object, default_value: str) -> str:
    cleaned = str(value).strip().lower() if pd.notna(value) else ""
    cleaned = cleaned or default_value
    cleaned = re.sub(r'[\\/:*?"<>|]+', " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or default_value


def build_target_filename(match_row: pd.Series, video_row: pd.Series | None) -> str:
    mech = sanitize_filename_part(match_row["mech"], "unknown_mech")
    map_name = sanitize_filename_part(match_row["map"], "unknown_map")
    matched_date = ""
    if video_row is not None:
        matched_date = video_row["obs_datetime"].strftime("%Y-%m-%d")
    else:
        matched_date = str(match_row["comp_date"])

    return (
        f"mwocomp - {mech} - {map_name} - "
        f"r{int(match_row['round'])}g{int(match_row['game'])} - {matched_date}.mkv"
    )


def match_videos_to_matches(matches_df: pd.DataFrame, videos_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    review_rows: list[dict[str, object]] = []
    rename_rows: list[dict[str, object]] = []

    candidate_videos = videos_df[videos_df["obs_date"].isin(set(ROUND_TO_COMP_DATE.values()))].copy()
    LOGGER.info("Found %s candidate competition-date videos", len(candidate_videos))

    for comp_date, date_matches in matches_df.groupby("comp_date", sort=True):
        date_matches = date_matches.sort_values(["round", "game", "game_time"]).reset_index(drop=True)
        date_videos = candidate_videos[candidate_videos["obs_date"] == comp_date].copy()
        date_videos = date_videos.sort_values(["obs_datetime", "full_path"]).reset_index(drop=True)

        LOGGER.info(
            "Matching %s comp rows to %s videos on %s",
            len(date_matches),
            len(date_videos),
            comp_date,
        )

        chosen_positions, best_score, second_best_score = choose_candidate_window(date_matches, date_videos)
        chosen_video_rows = date_videos.iloc[chosen_positions].copy() if chosen_positions else date_videos.iloc[0:0].copy()
        chosen_video_rows = chosen_video_rows.sort_values(["obs_datetime", "full_path"]).reset_index(drop=True)

        matched_count = min(len(date_matches), len(chosen_video_rows))
        for position in range(matched_count):
            match_row = date_matches.iloc[position]
            video_row = chosen_video_rows.iloc[position]
            confidence, confidence_note = determine_confidence(
                date_video_count=len(date_videos),
                date_match_count=len(date_matches),
                best_score=best_score,
                second_best_score=second_best_score,
                datetime_source=str(video_row["datetime_source"]),
            )
            match_status = "auto_matched" if confidence != "low" else "review_needed"
            notes = (
                f"comp_date={comp_date}; window_score={best_score:.2f}; "
                f"candidate_videos={len(date_videos)}; {confidence_note}"
            )

            review_rows.append(build_review_row(video_row, match_row, confidence, match_status, notes))
            rename_rows.append(
                {
                    "source_path": video_row["full_path"],
                    "target_filename": build_target_filename(match_row, video_row),
                    "round": int(match_row["round"]),
                    "game": int(match_row["game"]),
                    "map": match_row["map"],
                    "mech": match_row["mech"],
                    "match_id": str(match_row["match_id"]),
                    "status": match_status,
                }
            )

        if len(date_matches) > matched_count:
            for position in range(matched_count, len(date_matches)):
                match_row = date_matches.iloc[position]
                notes = f"comp_date={comp_date}; no candidate video available for this match"
                LOGGER.warning(
                    "Missing candidate video for round=%s game=%s on %s",
                    int(match_row["round"]),
                    int(match_row["game"]),
                    comp_date,
                )
                review_rows.append(build_review_row(None, match_row, "low", "review_needed", notes))
                rename_rows.append(
                    {
                        "source_path": "",
                        "target_filename": build_target_filename(match_row, None),
                        "round": int(match_row["round"]),
                        "game": int(match_row["game"]),
                        "map": match_row["map"],
                        "mech": match_row["mech"],
                        "match_id": str(match_row["match_id"]),
                        "status": "review_needed",
                    }
                )

        used_paths = set(chosen_video_rows["full_path"])
        extra_videos = date_videos[~date_videos["full_path"].isin(used_paths)].copy()
        for _, extra_video in extra_videos.iterrows():
            notes = f"comp_date={comp_date}; left unmatched as extra same-day recording"
            review_rows.append(build_review_row(extra_video, None, "", "extra_unmatched", notes))

    review_df = pd.DataFrame(review_rows)
    if review_df.empty:
        review_df = pd.DataFrame(columns=VIDEO_REVIEW_COLUMNS)
    else:
        review_df = review_df.loc[:, VIDEO_REVIEW_COLUMNS]

    rename_df = pd.DataFrame(rename_rows)
    if rename_df.empty:
        rename_df = pd.DataFrame(columns=RENAME_PLAN_COLUMNS)
    else:
        rename_df = rename_df.loc[:, RENAME_PLAN_COLUMNS]

    return review_df, rename_df


def write_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(dataframe), output_path)


def log_summary(videos_df: pd.DataFrame, review_df: pd.DataFrame, matches_df: pd.DataFrame) -> None:
    candidate_video_count = int(videos_df["obs_date"].isin(set(ROUND_TO_COMP_DATE.values())).sum()) if not videos_df.empty else 0
    auto_matches = int((review_df["match_status"] == "auto_matched").sum()) if not review_df.empty else 0
    review_needed = int((review_df["match_status"] == "review_needed").sum()) if not review_df.empty else 0
    unmatched_extras = int((review_df["match_status"] == "extra_unmatched").sum()) if not review_df.empty else 0

    LOGGER.info("Summary: total videos scanned = %s", len(videos_df))
    LOGGER.info("Summary: comp matches found = %s", len(matches_df))
    LOGGER.info("Summary: candidate comp-date videos = %s", candidate_video_count)
    LOGGER.info("Summary: auto matches = %s", auto_matches)
    LOGGER.info("Summary: review needed = %s", review_needed)
    LOGGER.info("Summary: unmatched extras = %s", unmatched_extras)


def main() -> int:
    configure_logging()
    args = parse_args()

    video_dir = Path(args.video_dir).expanduser()
    matches_csv = Path(args.matches_csv).expanduser()
    review_output = Path(args.review_output).expanduser()
    rename_output = Path(args.rename_output).expanduser()

    videos_df = scan_videos(video_dir)
    matches_df = load_comp_matches(matches_csv)
    review_df, rename_df = match_videos_to_matches(matches_df, videos_df)

    write_csv(review_df, review_output)
    write_csv(rename_df, rename_output)
    log_summary(videos_df, review_df, matches_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
