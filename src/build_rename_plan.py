from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
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
    "row_type",
    "obs_file",
    "full_path",
    "obs_datetime",
    "datetime_source",
    "duration_seconds",
    "candidate_status",
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
    parser.add_argument(
        "--min_duration",
        type=float,
        default=120.0,
        help="Minimum duration in seconds for a video to be considered a match candidate.",
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


def resolve_ffprobe_path() -> str | None:
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path:
        return ffprobe_path

    local_appdata = os.environ.get("LOCALAPPDATA", "")
    fallback_paths = [
        Path(local_appdata) / "Microsoft" / "WinGet" / "Links" / "ffprobe.exe",
        Path(local_appdata) / "Microsoft" / "WindowsApps" / "ffprobe.exe",
    ]
    for fallback_path in fallback_paths:
        try:
            if fallback_path.exists():
                return str(fallback_path)
            if fallback_path.is_symlink():
                resolved_path = fallback_path.resolve(strict=False)
                if resolved_path.exists():
                    return str(resolved_path)
        except OSError:
            continue

    winget_packages_root = Path(local_appdata) / "Microsoft" / "WinGet" / "Packages"
    if winget_packages_root.exists():
        package_matches = sorted(winget_packages_root.glob("Gyan.FFmpeg_*"))
        for package_dir in package_matches:
            for ffprobe_candidate in package_dir.glob("**/bin/ffprobe.exe"):
                if ffprobe_candidate.exists():
                    return str(ffprobe_candidate)

    return None


def probe_duration_seconds(ffprobe_path: str, video_path: Path) -> float | None:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        LOGGER.warning("ffprobe failed for %s: %s", video_path.name, result.stderr.strip())
        return None

    output = result.stdout.strip()
    if not output:
        return None

    try:
        return round(float(output), 3)
    except ValueError:
        LOGGER.warning("Could not parse ffprobe duration for %s: %s", video_path.name, output)
        return None


def determine_candidate_status(obs_datetime: datetime | None, duration_seconds: float | None, min_duration: float) -> str:
    if obs_datetime is None:
        return "excluded_no_datetime"
    if duration_seconds is not None and duration_seconds < min_duration:
        return "excluded_short_clip"
    return "candidate"


def scan_videos(video_dir: Path, min_duration: float) -> pd.DataFrame:
    if not video_dir.exists():
        raise FileNotFoundError(f"Video directory does not exist: {video_dir}")
    if not video_dir.is_dir():
        raise NotADirectoryError(f"Video directory is not a directory: {video_dir}")

    ffprobe_path = resolve_ffprobe_path()
    if ffprobe_path:
        LOGGER.info("Using ffprobe for duration checks: %s", ffprobe_path)
    else:
        LOGGER.warning("ffprobe is unavailable; duration_seconds will be blank and short clips cannot be excluded automatically.")

    records: list[dict[str, object]] = []
    for video_path in sorted(video_dir.rglob("*.mkv")):
        obs_datetime = parse_datetime_from_filename(video_path)
        datetime_source = "filename"
        if obs_datetime is None:
            try:
                obs_datetime = datetime.fromtimestamp(video_path.stat().st_mtime)
                datetime_source = "metadata"
            except OSError:
                obs_datetime = None
                datetime_source = ""

        duration_seconds = probe_duration_seconds(ffprobe_path, video_path) if ffprobe_path else None
        candidate_status = determine_candidate_status(obs_datetime, duration_seconds, min_duration)

        records.append(
            {
                "obs_file": video_path.name,
                "full_path": str(video_path.resolve()),
                "obs_datetime": obs_datetime,
                "datetime_source": datetime_source,
                "duration_seconds": duration_seconds,
                "candidate_status": candidate_status,
                "obs_date": "" if obs_datetime is None else obs_datetime.date().isoformat(),
            }
        )

    dataframe = pd.DataFrame(records)
    if dataframe.empty:
        return pd.DataFrame(
            columns=[
                "obs_file",
                "full_path",
                "obs_datetime",
                "datetime_source",
                "duration_seconds",
                "candidate_status",
                "obs_date",
            ]
        )

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
    row_type: str,
    video_row: pd.Series | None,
    match_row: pd.Series | None,
    confidence: str,
    match_status: str,
    notes: str,
) -> dict[str, object]:
    return {
        "row_type": row_type,
        "obs_file": "" if video_row is None else video_row["obs_file"],
        "full_path": "" if video_row is None else video_row["full_path"],
        "obs_datetime": (
            ""
            if video_row is None or pd.isna(video_row["obs_datetime"])
            else video_row["obs_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        ),
        "datetime_source": "" if video_row is None else video_row["datetime_source"],
        "duration_seconds": "" if video_row is None or pd.isna(video_row["duration_seconds"]) else video_row["duration_seconds"],
        "candidate_status": "" if video_row is None else video_row["candidate_status"],
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

    candidate_videos = videos_df[
        (videos_df["obs_date"].isin(set(ROUND_TO_COMP_DATE.values())))
        & (videos_df["candidate_status"] == "candidate")
    ].copy()
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

            review_rows.append(
                build_review_row(
                    "matched_comp_video",
                    video_row,
                    match_row,
                    confidence,
                    match_status,
                    notes,
                )
            )
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
                review_rows.append(
                    build_review_row(
                        "matched_comp_video",
                        None,
                        match_row,
                        "low",
                        "review_needed",
                        notes,
                    )
                )
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
            review_rows.append(
                build_review_row(
                    "unmatched_extra",
                    extra_video,
                    None,
                    "",
                    "extra_unmatched",
                    notes,
                )
            )

    excluded_same_day_videos = videos_df[
        (videos_df["obs_date"].isin(set(ROUND_TO_COMP_DATE.values())))
        & (videos_df["candidate_status"] != "candidate")
    ].copy()
    for _, excluded_video in excluded_same_day_videos.sort_values(["obs_datetime", "full_path"]).iterrows():
        notes = f"comp_date={excluded_video['obs_date']}; excluded from matching because candidate_status={excluded_video['candidate_status']}"
        row_type = "excluded_short_clip"
        if excluded_video["candidate_status"] != "excluded_short_clip":
            row_type = "unmatched_extra"
        review_rows.append(
            build_review_row(
                row_type,
                excluded_video,
                None,
                "",
                "excluded",
                notes,
            )
        )

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
    candidate_video_count = int(
        (
            videos_df["obs_date"].isin(set(ROUND_TO_COMP_DATE.values()))
            & (videos_df["candidate_status"] == "candidate")
        ).sum()
    ) if not videos_df.empty else 0
    auto_matches = int((review_df["match_status"] == "auto_matched").sum()) if not review_df.empty else 0
    review_needed = int((review_df["match_status"] == "review_needed").sum()) if not review_df.empty else 0
    matched_comp_video_rows = int((review_df["row_type"] == "matched_comp_video").sum()) if not review_df.empty else 0
    unmatched_extra_rows = int((review_df["row_type"] == "unmatched_extra").sum()) if not review_df.empty else 0
    excluded_short_clip_rows = int((review_df["row_type"] == "excluded_short_clip").sum()) if not review_df.empty else 0
    review_row_count = len(review_df)
    excluded_short_clips = int((videos_df["candidate_status"] == "excluded_short_clip").sum()) if not videos_df.empty else 0

    LOGGER.info("Summary: total videos scanned = %s", len(videos_df))
    LOGGER.info("Summary: comp matches found = %s", len(matches_df))
    LOGGER.info("Summary: candidate comp-date videos = %s", candidate_video_count)
    LOGGER.info("Summary: auto matches = %s", auto_matches)
    LOGGER.info("Summary: review needed = %s", review_needed)
    LOGGER.info("Summary: matched_comp_video rows = %s", matched_comp_video_rows)
    LOGGER.info("Summary: unmatched_extra rows = %s", unmatched_extra_rows)
    LOGGER.info("Summary: excluded_short_clip rows = %s", excluded_short_clip_rows)
    LOGGER.info("Summary: excluded short clips = %s", excluded_short_clips)
    LOGGER.info(
        "Summary: review CSV rows = %s (= matched_comp_video %s + unmatched_extra %s + excluded_short_clip %s)",
        review_row_count,
        matched_comp_video_rows,
        unmatched_extra_rows,
        excluded_short_clip_rows,
    )
    LOGGER.info(
        "Review CSV row count can exceed matched comp rows because it also includes unmatched extras and excluded rows for auditability."
    )


def main() -> int:
    configure_logging()
    args = parse_args()

    video_dir = Path(args.video_dir).expanduser()
    matches_csv = Path(args.matches_csv).expanduser()
    review_output = Path(args.review_output).expanduser()
    rename_output = Path(args.rename_output).expanduser()

    videos_df = scan_videos(video_dir, args.min_duration)
    matches_df = load_comp_matches(matches_csv)
    review_df, rename_df = match_videos_to_matches(matches_df, videos_df)

    write_csv(review_df, review_output)
    write_csv(rename_df, rename_output)
    log_summary(videos_df, review_df, matches_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
