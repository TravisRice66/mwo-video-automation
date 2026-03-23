from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.sync_api import BrowserContext, Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from youtube_studio_selectors import (
    CONTENT_NAV_SELECTORS,
    DESCRIPTION_INPUT_SELECTORS,
    PLAYLIST_DIALOG_SELECTORS,
    PLAYLIST_DONE_BUTTON_SELECTORS,
    PLAYLIST_DROPDOWN_SELECTORS,
    PLAYLIST_SEARCH_INPUT_SELECTORS,
    SAVE_BUTTON_SELECTORS,
    SEARCH_INPUT_SELECTORS,
    SHOW_MORE_BUTTON_SELECTORS,
    TAGS_INPUT_SELECTORS,
    THUMBNAIL_FILE_INPUT_SELECTORS,
    TITLE_INPUT_SELECTORS,
    VIDEO_ROW_TITLE_LINK_SELECTORS,
    VISIBILITY_DROPDOWN_SELECTORS,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_USER_DATA_DIR = ".playwright/youtube-studio-profile"
SUPPORTED_INPUT_SUFFIXES = {".csv", ".json"}

AUDIENCE_MADE_FOR_KIDS = "made_for_kids"
AUDIENCE_NOT_MADE_FOR_KIDS = "not_made_for_kids"

VISIBILITY_PUBLIC = "public"
VISIBILITY_UNLISTED = "unlisted"
VISIBILITY_PRIVATE = "private"

YOUTUBE_STUDIO_HOME = "https://studio.youtube.com"
YOUTUBE_STUDIO_CONTENT = "https://studio.youtube.com/channel/UC/videos"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

TRANSIENT_ERROR_PATTERNS = (
    "timeout",
    "timed out",
    "target closed",
    "execution context was destroyed",
    "navigation failed",
    "net::err",
    "context closed",
    "detached",
)

OPENAI_RECOVERY_ACTIONS = {
    "retry_same_page",
    "reopen_content_area",
    "reload_editor",
    "abort_row",
}

SELECTOR_GLOBAL_NAMES = {
    "content_nav_selectors": "CONTENT_NAV_SELECTORS",
    "description_input_selectors": "DESCRIPTION_INPUT_SELECTORS",
    "playlist_dialog_selectors": "PLAYLIST_DIALOG_SELECTORS",
    "playlist_done_button_selectors": "PLAYLIST_DONE_BUTTON_SELECTORS",
    "playlist_dropdown_selectors": "PLAYLIST_DROPDOWN_SELECTORS",
    "playlist_search_input_selectors": "PLAYLIST_SEARCH_INPUT_SELECTORS",
    "save_button_selectors": "SAVE_BUTTON_SELECTORS",
    "search_input_selectors": "SEARCH_INPUT_SELECTORS",
    "show_more_button_selectors": "SHOW_MORE_BUTTON_SELECTORS",
    "tags_input_selectors": "TAGS_INPUT_SELECTORS",
    "thumbnail_file_input_selectors": "THUMBNAIL_FILE_INPUT_SELECTORS",
    "title_input_selectors": "TITLE_INPUT_SELECTORS",
    "video_row_title_link_selectors": "VIDEO_ROW_TITLE_LINK_SELECTORS",
    "visibility_dropdown_selectors": "VISIBILITY_DROPDOWN_SELECTORS",
}


@dataclass(frozen=True)
class VideoJob:
    row_number: int
    row_data: dict[str, Any]
    identifier_label: str
    video_id: str | None
    lookup_text: str | None
    updates: dict[str, Any]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_json_object(path: Path, label: str) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return {str(key).strip().lower(): value for key, value in payload.items()}


def load_run_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"run_config_json file does not exist: {path}")
    config = load_json_object(path, label="run_config_json")
    LOGGER.info("Loaded run config from %s", path)
    return config


def parse_args() -> argparse.Namespace:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--run_config_json", default="")
    pre_args, remaining_argv = pre_parser.parse_known_args(sys.argv[1:])

    run_config_path = Path(pre_args.run_config_json).resolve() if pre_args.run_config_json else None
    run_config = load_run_config(run_config_path)

    parser = argparse.ArgumentParser(
        description="Bulk update metadata for already-uploaded videos in YouTube Studio.",
    )
    parser.add_argument(
        "--run_config_json",
        default=pre_args.run_config_json,
        help="Optional JSON file containing run settings for reproducible execution.",
    )
    parser.add_argument(
        "--input",
        default="",
        help="Path to CSV or JSON with one row/object per video.",
    )
    parser.add_argument(
        "--defaults_json",
        default="",
        help="Optional JSON file containing reusable default fields.",
    )
    parser.add_argument(
        "--user_data_dir",
        default=DEFAULT_USER_DATA_DIR,
        help="Path to persistent Chromium profile for keeping YouTube login.",
    )
    parser.add_argument(
        "--results_csv",
        default="output/youtube_metadata_update_results.csv",
        help="Where to write success/failure status for each input row.",
    )
    parser.add_argument(
        "--max_retries",
        type=int,
        default=2,
        help="Retries per video when UI actions fail.",
    )
    parser.add_argument(
        "--login_timeout_seconds",
        type=int,
        default=240,
        help="How long to allow manual login when session is not authenticated.",
    )
    parser.add_argument(
        "--action_timeout_ms",
        type=int,
        default=10000,
        help="Per-action timeout in milliseconds.",
    )
    parser.add_argument(
        "--start_row",
        type=int,
        default=1,
        help="1-based row number to start from in the input file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max rows to process after start_row; 0 means no limit.",
    )
    parser.add_argument(
        "--tags_mode",
        choices=["replace", "append"],
        default="replace",
        help="Whether tags should replace existing tags or append to them.",
    )
    parser.add_argument(
        "--playlist_mode",
        choices=["replace", "append"],
        default="replace",
        help="Whether playlist selection should replace or append.",
    )
    parser.add_argument(
        "--selectors_json",
        default="",
        help="Optional JSON file overriding Studio selector sets for UI drift recovery.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless. Leave off for manual login and debugging.",
    )
    parser.add_argument(
        "--checkpoint_every",
        type=int,
        default=10,
        help="Write running results CSV every N processed rows (set 1 for safest resumability).",
    )
    parser.add_argument(
        "--refresh_every",
        type=int,
        default=50,
        help="Re-open Studio Content page every N processed rows to keep long runs stable. 0 disables refresh.",
    )
    parser.add_argument(
        "--resume_from_results",
        default="",
        help="Optional prior results CSV. Rows already marked ok/skipped will be skipped automatically.",
    )
    parser.add_argument(
        "--stop_after_consecutive_failures",
        type=int,
        default=0,
        help="Abort run after this many consecutive failures. 0 means never auto-stop.",
    )
    parser.add_argument(
        "--retry_base_delay_seconds",
        type=float,
        default=1.2,
        help="Base delay used for retry backoff (seconds).",
    )
    parser.add_argument(
        "--retry_max_delay_seconds",
        type=float,
        default=20.0,
        help="Maximum delay cap used for retry backoff (seconds).",
    )
    parser.add_argument(
        "--retry_jitter_seconds",
        type=float,
        default=0.8,
        help="Random jitter added to each retry delay (seconds).",
    )
    parser.add_argument(
        "--retry_transient_only",
        action="store_true",
        help="Retry only transient failures; non-transient exceptions fail immediately.",
    )
    parser.add_argument(
        "--capture_failure_artifacts",
        action="store_true",
        help="Capture screenshot and HTML snapshot when a row attempt fails.",
    )
    parser.add_argument(
        "--failure_artifacts_dir",
        default="output/youtube_metadata_failure_artifacts/{timestamp}",
        help="Output directory for failure artifacts. Supports {timestamp} token.",
    )
    parser.add_argument(
        "--save_failure_html",
        action="store_true",
        help="When capturing failure artifacts, also save page HTML for diagnosis.",
    )
    parser.add_argument(
        "--trace_on_failure",
        action="store_true",
        help="Capture Playwright trace zip per failed row.",
    )
    parser.add_argument(
        "--effective_config_json",
        default="",
        help="Optional path to write the final effective run configuration JSON. Supports {timestamp} token.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Do everything except clicking Save.",
    )
    parser.add_argument(
        "--openai_recovery",
        action="store_true",
        help="On row failures, ask the OpenAI Responses API for a safe recovery recommendation before retrying.",
    )
    parser.add_argument(
        "--openai_model",
        default="gpt-5-mini",
        help="Model used for optional OpenAI recovery guidance.",
    )
    parser.add_argument(
        "--openai_timeout_seconds",
        type=float,
        default=20.0,
        help="Timeout for each optional OpenAI recovery API call.",
    )

    if run_config:
        valid_destinations = {action.dest for action in parser._actions}
        recognized = {key: value for key, value in run_config.items() if key in valid_destinations}
        ignored = sorted(key for key in run_config if key not in valid_destinations)
        if ignored:
            LOGGER.warning("Ignoring unknown run_config_json keys: %s", ignored)
        if recognized:
            parser.set_defaults(**recognized)

    return parser.parse_args(remaining_argv)


def normalize_selector_values(value: Any, key: str) -> tuple[str, ...]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, Sequence):
        items = [to_clean_string(item) for item in value]
    else:
        raise ValueError(f"selectors_json value for '{key}' must be a string or array of strings.")

    selectors = tuple(item for item in items if item)
    if not selectors:
        raise ValueError(f"selectors_json value for '{key}' cannot be empty.")
    return selectors


def load_selector_overrides(path: Path | None) -> dict[str, tuple[str, ...]]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"selectors_json file does not exist: {path}")

    payload = load_json_object(path, label="selectors_json")
    overrides: dict[str, tuple[str, ...]] = {}
    unknown_keys = sorted(key for key in payload if key not in SELECTOR_GLOBAL_NAMES)
    if unknown_keys:
        LOGGER.warning("Ignoring unknown selectors_json keys: %s", unknown_keys)

    for key, value in payload.items():
        if key not in SELECTOR_GLOBAL_NAMES:
            continue
        overrides[key] = normalize_selector_values(value, key=key)

    LOGGER.info("Loaded %s selector override set(s) from %s", len(overrides), path)
    return overrides


def apply_selector_overrides(overrides: dict[str, tuple[str, ...]]) -> None:
    for key, selectors in overrides.items():
        global_name = SELECTOR_GLOBAL_NAMES[key]
        globals()[global_name] = selectors
        LOGGER.info("Applied selector override: %s (%s selectors)", global_name, len(selectors))


def is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, (PlaywrightTimeoutError, TimeoutError)):
        return True
    message = to_clean_string(exc).lower()
    return any(pattern in message for pattern in TRANSIENT_ERROR_PATTERNS)


def compute_retry_delay_seconds(
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
    jitter_seconds: float,
) -> float:
    bounded_base = max(0.0, base_delay_seconds)
    bounded_cap = max(bounded_base, max_delay_seconds)
    bounded_jitter = max(0.0, jitter_seconds)

    exponential = bounded_base * (2 ** max(0, attempt - 1))
    clamped = min(exponential, bounded_cap)
    jitter = random.uniform(0.0, bounded_jitter) if bounded_jitter > 0 else 0.0
    return clamped + jitter


def safe_slug(value: str, max_length: int = 64) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", to_clean_string(value)).strip("_")
    if not slug:
        slug = "item"
    return slug[:max_length]


def normalize_keyed_record(record: dict[str, Any]) -> dict[str, Any]:
    return {str(key).strip().lower(): value for key, value in record.items()}


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    return False


def to_clean_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def first_non_blank(record: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in record and not is_blank(record[key]):
            return record[key]
    return None


def load_input_records(input_path: Path) -> list[dict[str, Any]]:
    suffix = input_path.suffix.lower()
    if suffix not in SUPPORTED_INPUT_SUFFIXES:
        raise ValueError(f"Unsupported input format: {suffix}. Supported: {sorted(SUPPORTED_INPUT_SUFFIXES)}")

    if suffix == ".csv":
        dataframe = pd.read_csv(input_path, dtype=str, keep_default_na=False)
        records = dataframe.to_dict(orient="records")
        return [normalize_keyed_record(record) for record in records]

    with input_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict) and isinstance(payload.get("videos"), list):
        raw_records = payload["videos"]
    elif isinstance(payload, list):
        raw_records = payload
    else:
        raise ValueError("JSON input must be either a list of objects or an object with a 'videos' list.")

    normalized: list[dict[str, Any]] = []
    for item in raw_records:
        if not isinstance(item, dict):
            raise ValueError("Each JSON entry must be an object.")
        normalized.append(normalize_keyed_record(item))
    return normalized


def load_defaults(defaults_path: Path | None) -> dict[str, Any]:
    if defaults_path is None:
        return {}
    with defaults_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Defaults JSON must be an object.")
    return normalize_keyed_record(payload)


def parse_video_id(value: str) -> str | None:
    candidate = to_clean_string(value)
    if not candidate:
        return None

    direct_match = re.fullmatch(r"[A-Za-z0-9_-]{8,}", candidate)
    if direct_match:
        return candidate

    patterns = [
        r"studio\.youtube\.com/video/([A-Za-z0-9_-]{8,})",
        r"youtu\.be/([A-Za-z0-9_-]{8,})",
        r"[?&]v=([A-Za-z0-9_-]{8,})",
        r"/shorts/([A-Za-z0-9_-]{8,})",
    ]
    for pattern in patterns:
        match = re.search(pattern, candidate)
        if match:
            return match.group(1)
    return None


def parse_tags(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        tags = [to_clean_string(tag) for tag in value]
        tags = [tag for tag in tags if tag]
        return tags if tags else None

    text = to_clean_string(value)
    if not text:
        return None
    tags = [tag.strip() for tag in text.split(",")]
    tags = [tag for tag in tags if tag]
    return tags if tags else None


def normalize_audience(value: Any) -> str | None:
    text = to_clean_string(value).lower().replace("-", " ").replace("_", " ")
    if not text:
        return None
    if text in {"made for kids", "yes", "kids", "true", "1"}:
        return AUDIENCE_MADE_FOR_KIDS
    if text in {"not made for kids", "no", "not for kids", "false", "0"}:
        return AUDIENCE_NOT_MADE_FOR_KIDS
    raise ValueError(f"Unsupported audience value: {value}")


def normalize_visibility(value: Any) -> str | None:
    text = to_clean_string(value).lower()
    if not text:
        return None
    if text in {"public", "unlisted", "private"}:
        return text
    raise ValueError(f"Unsupported visibility value: {value}")


def build_updates(record: dict[str, Any], defaults: dict[str, Any], input_dir: Path) -> dict[str, Any]:
    combined = dict(defaults)
    combined.update({key: value for key, value in record.items() if not is_blank(value)})

    title_value = first_non_blank(combined, ["new_title", "title"])
    description_value = first_non_blank(combined, ["description"])
    tags_value = parse_tags(first_non_blank(combined, ["tags"]))
    playlist_value = first_non_blank(combined, ["playlist"])
    audience_value = normalize_audience(first_non_blank(combined, ["audience"])) if first_non_blank(combined, ["audience"]) is not None else None
    visibility_source = first_non_blank(combined, ["visibility", "publish_status"])
    visibility_value = normalize_visibility(visibility_source) if visibility_source is not None else None

    thumbnail_raw = first_non_blank(combined, ["thumbnail_path"])
    thumbnail_path: str | None = None
    if thumbnail_raw is not None:
        thumb_candidate = Path(to_clean_string(thumbnail_raw))
        if not thumb_candidate.is_absolute():
            thumb_candidate = (input_dir / thumb_candidate).resolve()
        thumbnail_path = str(thumb_candidate)

    updates: dict[str, Any] = {}
    if title_value is not None:
        updates["title"] = to_clean_string(title_value)
    if description_value is not None:
        updates["description"] = str(description_value)
    if tags_value is not None:
        updates["tags"] = tags_value
    if playlist_value is not None:
        updates["playlist"] = to_clean_string(playlist_value)
    if audience_value is not None:
        updates["audience"] = audience_value
    if visibility_value is not None:
        updates["visibility"] = visibility_value
    if thumbnail_path is not None:
        updates["thumbnail_path"] = thumbnail_path
    return updates


def build_jobs(
    records: list[dict[str, Any]],
    defaults: dict[str, Any],
    input_dir: Path,
    start_row: int,
    limit: int,
) -> list[VideoJob]:
    if start_row < 1:
        raise ValueError("start_row must be >= 1")

    jobs: list[VideoJob] = []
    for row_number, record in enumerate(records, start=1):
        if row_number < start_row:
            continue
        if limit > 0 and len(jobs) >= limit:
            break

        video_id = parse_video_id(to_clean_string(first_non_blank(record, ["video_id", "video_url", "url"])))
        lookup_text = to_clean_string(
            first_non_blank(record, ["current_title", "lookup_title", "current_filename", "filename", "title"])
        )
        updates = build_updates(record, defaults, input_dir=input_dir)

        identifier_label = ""
        if video_id:
            identifier_label = f"video_id={video_id}"
        elif lookup_text:
            identifier_label = f"lookup={lookup_text}"
        else:
            identifier_label = "missing_identifier"

        jobs.append(
            VideoJob(
                row_number=row_number,
                row_data=record,
                identifier_label=identifier_label,
                video_id=video_id,
                lookup_text=lookup_text if lookup_text else None,
                updates=updates,
            )
        )

    LOGGER.info("Prepared %s job(s) from %s input row(s)", len(jobs), len(records))
    return jobs


def find_first_visible(scope: Page | Locator, selectors: tuple[str, ...], timeout_ms: int) -> Locator | None:
    per_selector_timeout = max(600, min(timeout_ms, 2000))
    for selector in selectors:
        locator = scope.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=per_selector_timeout)
            return locator
        except PlaywrightTimeoutError:
            continue
    return None


def set_textbox_value(page: Page, locator: Locator, value: str) -> None:
    locator.click()
    page.keyboard.press("Control+A")
    page.keyboard.press("Backspace")
    page.keyboard.type(value, delay=4)


def wait_for_manual_login(page: Page, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current_url = page.url.lower()
        if "studio.youtube.com" in current_url and "accounts.google.com" not in current_url:
            return
        page.wait_for_timeout(1000)
    raise TimeoutError(
        "YouTube Studio login did not complete in time. "
        "Launch again and complete login in the browser window."
    )


def open_content_area(page: Page, action_timeout_ms: int, login_timeout_seconds: int) -> None:
    page.goto(YOUTUBE_STUDIO_HOME, wait_until="domcontentloaded", timeout=60000)
    wait_for_manual_login(page, timeout_seconds=login_timeout_seconds)

    content_nav = find_first_visible(page, CONTENT_NAV_SELECTORS, timeout_ms=action_timeout_ms)
    if content_nav is not None:
        content_nav.click()
    else:
        page.goto(YOUTUBE_STUDIO_CONTENT, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(700)


def open_video_editor(page: Page, job: VideoJob, action_timeout_ms: int, login_timeout_seconds: int) -> str:
    if job.video_id:
        page.goto(f"https://studio.youtube.com/video/{job.video_id}/edit", wait_until="domcontentloaded", timeout=60000)
        wait_for_manual_login(page, timeout_seconds=login_timeout_seconds)
        page.wait_for_url(re.compile(r"https://studio\.youtube\.com/video/.+/edit"), timeout=action_timeout_ms * 2)
        return job.video_id

    if not job.lookup_text:
        raise ValueError("No identifier found. Provide video_id, video_url, current_title, filename, or lookup_title.")

    open_content_area(page, action_timeout_ms=action_timeout_ms, login_timeout_seconds=login_timeout_seconds)

    search_input = find_first_visible(page, SEARCH_INPUT_SELECTORS, timeout_ms=action_timeout_ms)
    if search_input is None:
        raise RuntimeError("Could not find YouTube Studio search input in Content page.")

    search_input.click()
    search_input.fill("")
    search_input.type(job.lookup_text, delay=3)
    search_input.press("Enter")
    page.wait_for_timeout(1200)

    video_row = page.locator("ytcp-video-row", has_text=job.lookup_text).first
    try:
        video_row.wait_for(state="visible", timeout=action_timeout_ms)
    except PlaywrightTimeoutError:
        video_link = None
        for selector in VIDEO_ROW_TITLE_LINK_SELECTORS:
            candidate = page.locator(selector, has_text=job.lookup_text).first
            if candidate.count() > 0:
                video_link = candidate
                break
        if video_link is None:
            raise RuntimeError(f"No video row matched lookup text: {job.lookup_text}")
        video_link.click()
    else:
        row_link = video_row.locator("a[href*='/video/']").first
        if row_link.count() == 0:
            row_link = video_row.locator("a").first
        row_link.click()

    page.wait_for_url(re.compile(r"https://studio\.youtube\.com/video/.+/edit"), timeout=action_timeout_ms * 2)

    parsed_video_id = parse_video_id(page.url)
    return parsed_video_id or ""


def ensure_show_more(page: Page, action_timeout_ms: int) -> None:
    show_more = find_first_visible(page, SHOW_MORE_BUTTON_SELECTORS, timeout_ms=1500)
    if show_more is not None:
        show_more.click()
        page.wait_for_timeout(min(800, action_timeout_ms // 10))


def remove_existing_tags_if_possible(page: Page) -> None:
    delete_tag_selectors = (
        "#tags-container ytcp-chip button[aria-label*='Remove']",
        "#tags-container tp-yt-paper-chip tp-yt-paper-icon-button[aria-label*='Remove']",
        "#tags-container ytcp-chip span[aria-label*='Remove']",
    )
    for _ in range(80):
        deleted = False
        for selector in delete_tag_selectors:
            delete_button = page.locator(selector).first
            if delete_button.count() == 0:
                continue
            try:
                if delete_button.is_visible(timeout=250):
                    delete_button.click()
                    page.wait_for_timeout(80)
                    deleted = True
                    break
            except PlaywrightTimeoutError:
                continue
        if not deleted:
            return


def set_title(page: Page, title: str, action_timeout_ms: int) -> None:
    title_input = find_first_visible(page, TITLE_INPUT_SELECTORS, timeout_ms=action_timeout_ms)
    if title_input is None:
        raise RuntimeError("Could not find title input in video editor.")
    set_textbox_value(page, title_input, title)


def set_description(page: Page, description: str, action_timeout_ms: int) -> None:
    description_input = find_first_visible(page, DESCRIPTION_INPUT_SELECTORS, timeout_ms=action_timeout_ms)
    if description_input is None:
        raise RuntimeError("Could not find description input in video editor.")
    set_textbox_value(page, description_input, description)


def set_tags(page: Page, tags: list[str], action_timeout_ms: int, tags_mode: str) -> None:
    ensure_show_more(page, action_timeout_ms=action_timeout_ms)
    tags_input = find_first_visible(page, TAGS_INPUT_SELECTORS, timeout_ms=action_timeout_ms)
    if tags_input is None:
        raise RuntimeError("Could not find tags input in video editor.")

    if tags_mode == "replace":
        remove_existing_tags_if_possible(page)

    for tag in tags:
        tags_input.click()
        tags_input.fill(tag)
        tags_input.press("Enter")
        page.wait_for_timeout(60)


def set_playlist(page: Page, playlist_name: str, action_timeout_ms: int, playlist_mode: str) -> None:
    ensure_show_more(page, action_timeout_ms=action_timeout_ms)

    dropdown = find_first_visible(page, PLAYLIST_DROPDOWN_SELECTORS, timeout_ms=action_timeout_ms)
    if dropdown is None:
        raise RuntimeError("Could not find playlist dropdown in video editor.")
    dropdown.click()

    dialog = find_first_visible(page, PLAYLIST_DIALOG_SELECTORS, timeout_ms=action_timeout_ms)
    if dialog is None:
        raise RuntimeError("Playlist dialog did not open.")

    if playlist_mode == "replace":
        for _ in range(50):
            checked_items = dialog.locator("tp-yt-paper-checkbox[checked], tp-yt-paper-checkbox[aria-checked='true']")
            if checked_items.count() == 0:
                break
            checked_items.first.click()
            page.wait_for_timeout(60)

    playlist_search = find_first_visible(dialog, PLAYLIST_SEARCH_INPUT_SELECTORS, timeout_ms=1200)
    if playlist_search is not None:
        playlist_search.fill("")
        playlist_search.type(playlist_name, delay=2)

    option = dialog.locator("tp-yt-paper-checkbox", has_text=playlist_name).first
    if option.count() == 0:
        option = dialog.get_by_text(playlist_name, exact=False).first
    if option.count() == 0:
        raise RuntimeError(f"Playlist not found in dialog: {playlist_name}")
    option.click()

    done_button = find_first_visible(dialog, PLAYLIST_DONE_BUTTON_SELECTORS, timeout_ms=action_timeout_ms)
    if done_button is None:
        done_button = find_first_visible(page, PLAYLIST_DONE_BUTTON_SELECTORS, timeout_ms=action_timeout_ms)
    if done_button is None:
        raise RuntimeError("Could not find Done/Save button in playlist dialog.")
    done_button.click()
    page.wait_for_timeout(250)


def set_audience(page: Page, audience: str, action_timeout_ms: int) -> None:
    if audience == AUDIENCE_NOT_MADE_FOR_KIDS:
        selectors = (
            "tp-yt-paper-radio-button:has-text(\"No, it's not made for kids\")",
            "label:has-text(\"No, it's not made for kids\")",
        )
    elif audience == AUDIENCE_MADE_FOR_KIDS:
        selectors = (
            "tp-yt-paper-radio-button:has-text(\"Yes, it's made for kids\")",
            "label:has-text(\"Yes, it's made for kids\")",
        )
    else:
        raise ValueError(f"Unsupported audience value: {audience}")

    target = find_first_visible(page, selectors, timeout_ms=action_timeout_ms)
    if target is None:
        raise RuntimeError("Could not find audience radio option in video editor.")
    target.click()


def set_visibility(page: Page, visibility: str, action_timeout_ms: int) -> None:
    dropdown = find_first_visible(page, VISIBILITY_DROPDOWN_SELECTORS, timeout_ms=1500)
    if dropdown is None:
        raise RuntimeError("Could not find visibility dropdown.")
    dropdown.click()

    label_map = {
        VISIBILITY_PUBLIC: "Public",
        VISIBILITY_UNLISTED: "Unlisted",
        VISIBILITY_PRIVATE: "Private",
    }
    option_label = label_map[visibility]
    option = page.locator("tp-yt-paper-item", has_text=option_label).first
    if option.count() == 0:
        option = page.get_by_text(option_label, exact=True).first
    if option.count() == 0:
        raise RuntimeError(f"Visibility option not found: {option_label}")
    option.click()
    page.wait_for_timeout(150)


def set_thumbnail(page: Page, thumbnail_path: str, action_timeout_ms: int) -> None:
    ensure_show_more(page, action_timeout_ms=action_timeout_ms)
    resolved_thumbnail = Path(thumbnail_path)
    if not resolved_thumbnail.exists():
        raise FileNotFoundError(f"Thumbnail does not exist: {resolved_thumbnail}")

    file_input = find_first_visible(page, THUMBNAIL_FILE_INPUT_SELECTORS, timeout_ms=action_timeout_ms)
    if file_input is None:
        raise RuntimeError("Could not find thumbnail upload input.")
    file_input.set_input_files(str(resolved_thumbnail))
    page.wait_for_timeout(1200)


def click_save(page: Page, action_timeout_ms: int, dry_run: bool) -> str:
    save_button = find_first_visible(page, SAVE_BUTTON_SELECTORS, timeout_ms=action_timeout_ms)
    if save_button is None:
        raise RuntimeError("Could not find Save button in video editor.")

    if dry_run:
        return "dry_run_no_save"

    disabled = save_button.get_attribute("disabled")
    if disabled is not None:
        return "no_changes_detected"

    save_button.click()
    page.wait_for_timeout(1000)
    return "saved"


def apply_updates_to_video(
    page: Page,
    updates: dict[str, Any],
    action_timeout_ms: int,
    dry_run: bool,
    tags_mode: str,
    playlist_mode: str,
) -> tuple[str, list[str]]:
    applied_fields: list[str] = []

    if "title" in updates:
        set_title(page, updates["title"], action_timeout_ms=action_timeout_ms)
        applied_fields.append("title")
    if "description" in updates:
        set_description(page, updates["description"], action_timeout_ms=action_timeout_ms)
        applied_fields.append("description")
    if "tags" in updates:
        set_tags(page, updates["tags"], action_timeout_ms=action_timeout_ms, tags_mode=tags_mode)
        applied_fields.append("tags")
    if "playlist" in updates:
        set_playlist(page, updates["playlist"], action_timeout_ms=action_timeout_ms, playlist_mode=playlist_mode)
        applied_fields.append("playlist")
    if "audience" in updates:
        set_audience(page, updates["audience"], action_timeout_ms=action_timeout_ms)
        applied_fields.append("audience")
    if "visibility" in updates:
        set_visibility(page, updates["visibility"], action_timeout_ms=action_timeout_ms)
        applied_fields.append("visibility")
    if "thumbnail_path" in updates:
        set_thumbnail(page, updates["thumbnail_path"], action_timeout_ms=action_timeout_ms)
        applied_fields.append("thumbnail_path")

    save_result = click_save(page, action_timeout_ms=action_timeout_ms, dry_run=dry_run)
    return save_result, applied_fields


def build_artifact_paths(base_dir: Path, row_number: int, attempt: int, identifier: str) -> dict[str, Path]:
    slug = safe_slug(identifier, max_length=50)
    stem = f"row_{row_number:05d}_attempt_{attempt:02d}_{slug}"
    return {
        "screenshot": base_dir / f"{stem}.png",
        "html": base_dir / f"{stem}.html",
        "trace": base_dir / f"{stem}.zip",
    }


def capture_failure_artifacts(
    page: Page,
    artifact_paths: dict[str, Path],
    save_failure_html: bool,
) -> dict[str, str]:
    captured: dict[str, str] = {}

    screenshot_path = artifact_paths["screenshot"]
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        captured["screenshot_path"] = str(screenshot_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to capture screenshot artifact: %s", exc)

    if save_failure_html:
        html_path = artifact_paths["html"]
        try:
            html = page.content()
            html_path.write_text(html, encoding="utf-8")
            captured["html_path"] = str(html_path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to capture HTML artifact: %s", exc)

    return captured


def get_page_title_safe(page: Page) -> str:
    try:
        return to_clean_string(page.title())
    except Exception:  # noqa: BLE001
        return ""


def extract_openai_output_text(payload: dict[str, Any]) -> str:
    direct_text = to_clean_string(payload.get("output_text"))
    if direct_text:
        return direct_text

    output_parts: list[str] = []
    for item in payload.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            if content.get("type") != "output_text":
                continue
            text = to_clean_string(content.get("text"))
            if text:
                output_parts.append(text)
    return "\n".join(output_parts).strip()


def parse_json_object_text(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
        candidate = re.sub(r"\s*```$", "", candidate)

    if candidate.startswith("{") and candidate.endswith("}"):
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        parsed = json.loads(candidate[start : end + 1])
        if isinstance(parsed, dict):
            return parsed

    raise ValueError("Response did not contain a JSON object.")


def request_openai_recovery_guidance(
    api_key: str,
    model: str,
    timeout_seconds: float,
    job: VideoJob,
    page: Page,
    error_message: str,
    attempt: int,
    max_attempts: int,
) -> dict[str, str]:
    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You are assisting a Playwright automation that edits metadata in YouTube Studio. "
                            "Return a JSON object with keys action and guidance. "
                            "Allowed action values are: retry_same_page, reopen_content_area, reload_editor, abort_row. "
                            "Choose the safest immediate recovery step for this runtime failure. "
                            "Keep guidance under 240 characters and do not suggest code changes."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "row_number": job.row_number,
                                "identifier": job.identifier_label,
                                "lookup_text": job.lookup_text or "",
                                "video_id": job.video_id or "",
                                "update_fields": sorted(job.updates.keys()),
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "error": error_message,
                                "page_url": to_clean_string(page.url),
                                "page_title": get_page_title_safe(page),
                            },
                            ensure_ascii=True,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_object",
            }
        },
    }
    request_body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        OPENAI_RESPONSES_URL,
        data=request_body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response_payload = json.loads(response.read().decode("utf-8"))

    output_text = extract_openai_output_text(response_payload)
    structured = parse_json_object_text(output_text)

    action = to_clean_string(structured.get("action")).lower()
    guidance = to_clean_string(structured.get("guidance"))
    if action not in OPENAI_RECOVERY_ACTIONS:
        raise ValueError(f"Unsupported OpenAI recovery action: {action or '<blank>'}")
    if not guidance:
        raise ValueError("OpenAI recovery guidance was blank.")

    return {
        "action": action,
        "guidance": guidance,
    }


def apply_openai_recovery_action(
    page: Page,
    job: VideoJob,
    action: str,
    action_timeout_ms: int,
    login_timeout_seconds: int,
) -> None:
    if action == "retry_same_page":
        return
    if action == "reopen_content_area":
        open_content_area(page, action_timeout_ms=action_timeout_ms, login_timeout_seconds=login_timeout_seconds)
        return
    if action == "reload_editor":
        open_video_editor(
            page,
            job,
            action_timeout_ms=action_timeout_ms,
            login_timeout_seconds=login_timeout_seconds,
        )
        return
    if action == "abort_row":
        return
    raise ValueError(f"Unsupported recovery action: {action}")


def process_job(
    context: BrowserContext,
    page: Page,
    job: VideoJob,
    max_retries: int,
    action_timeout_ms: int,
    login_timeout_seconds: int,
    dry_run: bool,
    tags_mode: str,
    playlist_mode: str,
    retry_base_delay_seconds: float,
    retry_max_delay_seconds: float,
    retry_jitter_seconds: float,
    retry_transient_only: bool,
    capture_failure_artifacts_enabled: bool,
    failure_artifacts_dir: Path | None,
    save_failure_html: bool,
    trace_on_failure: bool,
    openai_recovery: bool,
    openai_model: str,
    openai_timeout_seconds: float,
    openai_api_key: str | None,
) -> dict[str, Any]:
    if not job.video_id and not job.lookup_text:
        return {
            "row_number": job.row_number,
            "identifier": job.identifier_label,
            "status": "failed",
            "attempts": 0,
            "video_id": "",
            "applied_fields": "",
            "save_result": "",
            "error": "Missing identifier. Provide video_id/video_url/current_title/lookup_title/filename.",
            "transient_error": "false",
            "screenshot_path": "",
            "html_path": "",
            "trace_path": "",
            "ai_guidance": "",
        }

    if not job.updates:
        return {
            "row_number": job.row_number,
            "identifier": job.identifier_label,
            "status": "skipped",
            "attempts": 0,
            "video_id": job.video_id or "",
            "applied_fields": "",
            "save_result": "",
            "error": "No metadata fields were provided for update.",
            "transient_error": "false",
            "screenshot_path": "",
            "html_path": "",
            "trace_path": "",
            "ai_guidance": "",
        }

    last_error = ""
    resolved_video_id = job.video_id or ""
    last_error_was_transient = False
    latest_artifacts: dict[str, str] = {}
    latest_ai_guidance = ""
    attempts_made = 0

    for attempt in range(1, max_retries + 2):
        attempts_made = attempt
        trace_started = False
        artifact_paths: dict[str, Path] | None = None
        if failure_artifacts_dir is not None:
            artifact_paths = build_artifact_paths(
                base_dir=failure_artifacts_dir,
                row_number=job.row_number,
                attempt=attempt,
                identifier=job.identifier_label,
            )

        if trace_on_failure:
            try:
                context.tracing.start(screenshots=True, snapshots=True, sources=False)
                trace_started = True
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Could not start trace for row %s attempt %s: %s", job.row_number, attempt, exc)

        try:
            resolved_video_id = open_video_editor(
                page,
                job,
                action_timeout_ms=action_timeout_ms,
                login_timeout_seconds=login_timeout_seconds,
            )
            save_result, applied_fields = apply_updates_to_video(
                page,
                job.updates,
                action_timeout_ms=action_timeout_ms,
                dry_run=dry_run,
                tags_mode=tags_mode,
                playlist_mode=playlist_mode,
            )
            if trace_started:
                try:
                    context.tracing.stop()
                except Exception:
                    pass
            return {
                "row_number": job.row_number,
                "identifier": job.identifier_label,
                "status": "ok",
                "attempts": attempt,
                "video_id": resolved_video_id,
                "applied_fields": ",".join(applied_fields),
                "save_result": save_result,
                "error": "",
                "transient_error": "false",
                "screenshot_path": "",
                "html_path": "",
                "trace_path": "",
                "ai_guidance": latest_ai_guidance,
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            last_error_was_transient = is_transient_error(exc)

            if capture_failure_artifacts_enabled and artifact_paths is not None:
                latest_artifacts = capture_failure_artifacts(
                    page,
                    artifact_paths=artifact_paths,
                    save_failure_html=save_failure_html,
                )

            if trace_started and artifact_paths is not None:
                try:
                    context.tracing.stop(path=str(artifact_paths["trace"]))
                    latest_artifacts["trace_path"] = str(artifact_paths["trace"])
                except Exception as trace_exc:  # noqa: BLE001
                    LOGGER.warning("Failed to save trace artifact: %s", trace_exc)
            elif trace_started:
                try:
                    context.tracing.stop()
                except Exception:
                    pass

            LOGGER.warning(
                "Row %s failed on attempt %s/%s (%s): %s [transient=%s]",
                job.row_number,
                attempt,
                max_retries + 1,
                job.identifier_label,
                last_error,
                last_error_was_transient,
            )

            recovery_action = "retry_same_page"
            if openai_recovery and openai_api_key:
                try:
                    recovery = request_openai_recovery_guidance(
                        api_key=openai_api_key,
                        model=openai_model,
                        timeout_seconds=openai_timeout_seconds,
                        job=job,
                        page=page,
                        error_message=last_error,
                        attempt=attempt,
                        max_attempts=max_retries + 1,
                    )
                    recovery_action = recovery["action"]
                    latest_ai_guidance = f"{recovery_action}: {recovery['guidance']}"
                    LOGGER.info(
                        "OpenAI recovery guidance for row %s: %s",
                        job.row_number,
                        latest_ai_guidance,
                    )
                    if recovery_action == "abort_row":
                        LOGGER.warning("OpenAI recovery requested abort for row %s.", job.row_number)
                        break
                    if attempt <= max_retries:
                        apply_openai_recovery_action(
                            page,
                            job,
                            action=recovery_action,
                            action_timeout_ms=action_timeout_ms,
                            login_timeout_seconds=login_timeout_seconds,
                        )
                except Exception as recovery_exc:  # noqa: BLE001
                    LOGGER.warning(
                        "OpenAI recovery guidance failed for row %s: %s",
                        job.row_number,
                        recovery_exc,
                    )

            if retry_transient_only and not last_error_was_transient:
                LOGGER.warning(
                    "Not retrying row %s because failure is non-transient and --retry_transient_only is enabled.",
                    job.row_number,
                )
                break

            if attempt <= max_retries:
                delay_seconds = compute_retry_delay_seconds(
                    attempt=attempt,
                    base_delay_seconds=retry_base_delay_seconds,
                    max_delay_seconds=retry_max_delay_seconds,
                    jitter_seconds=retry_jitter_seconds,
                )
                LOGGER.info("Retrying row %s after %.2f second(s)", job.row_number, delay_seconds)
                page.wait_for_timeout(int(delay_seconds * 1000))
        else:
            if trace_started:
                try:
                    context.tracing.stop()
                except Exception:
                    pass

    return {
        "row_number": job.row_number,
        "identifier": job.identifier_label,
        "status": "failed",
        "attempts": attempts_made,
        "video_id": resolved_video_id,
        "applied_fields": "",
        "save_result": "",
        "error": last_error,
        "transient_error": str(last_error_was_transient).lower(),
        "screenshot_path": latest_artifacts.get("screenshot_path", ""),
        "html_path": latest_artifacts.get("html_path", ""),
        "trace_path": latest_artifacts.get("trace_path", ""),
        "ai_guidance": latest_ai_guidance,
    }


def write_results(results: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe = pd.DataFrame(results)
    dataframe.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s result row(s) to %s", len(dataframe), output_path)


def load_resume_row_numbers(path: Path | None) -> set[int]:
    if path is None:
        return set()

    if not path.exists():
        raise FileNotFoundError(f"resume_from_results file does not exist: {path}")

    dataframe = pd.read_csv(path, dtype=str, keep_default_na=False)
    required_columns = {"row_number", "status"}
    if not required_columns.issubset(set(dataframe.columns)):
        raise ValueError(
            f"resume_from_results must include columns {sorted(required_columns)}. "
            f"Found: {list(dataframe.columns)}"
        )

    completed_rows: set[int] = set()
    for _, row in dataframe.iterrows():
        status = to_clean_string(row.get("status", "")).lower()
        if status not in {"ok", "skipped"}:
            continue
        row_number_raw = to_clean_string(row.get("row_number", ""))
        if not row_number_raw:
            continue
        try:
            completed_rows.add(int(row_number_raw))
        except ValueError:
            LOGGER.warning("Ignoring non-integer row_number in resume file: %s", row_number_raw)
            continue

    LOGGER.info("Loaded %s completed row(s) from %s", len(completed_rows), path)
    return completed_rows


def resolve_timestamped_path(raw_path: str, run_timestamp: str, default_suffix: str | None = None) -> Path:
    candidate = raw_path
    if "{timestamp}" in candidate:
        candidate = candidate.replace("{timestamp}", run_timestamp)
    path = Path(candidate)
    if default_suffix and path.suffix.lower() != default_suffix.lower():
        path = path.with_suffix(default_suffix)
    return path.resolve()


def write_effective_run_config(args: argparse.Namespace, output_path: Path) -> None:
    payload: dict[str, Any] = {}
    for key, value in vars(args).items():
        payload[key] = value

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    LOGGER.info("Wrote effective run config to %s", output_path)


def main() -> int:
    configure_logging()
    args = parse_args()

    if to_clean_string(args.input) == "":
        raise ValueError("--input is required. Provide it directly or via run_config_json.")

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    defaults_path = Path(args.defaults_json).resolve() if args.defaults_json else None
    if defaults_path is not None and not defaults_path.exists():
        raise FileNotFoundError(f"Defaults file does not exist: {defaults_path}")

    if args.checkpoint_every < 1:
        raise ValueError("checkpoint_every must be >= 1")
    if args.refresh_every < 0:
        raise ValueError("refresh_every must be >= 0")
    if args.stop_after_consecutive_failures < 0:
        raise ValueError("stop_after_consecutive_failures must be >= 0")
    if args.retry_base_delay_seconds < 0:
        raise ValueError("retry_base_delay_seconds must be >= 0")
    if args.retry_max_delay_seconds < 0:
        raise ValueError("retry_max_delay_seconds must be >= 0")
    if args.retry_jitter_seconds < 0:
        raise ValueError("retry_jitter_seconds must be >= 0")
    if args.openai_timeout_seconds <= 0:
        raise ValueError("openai_timeout_seconds must be > 0")

    resume_results_path = Path(args.resume_from_results).resolve() if args.resume_from_results else None
    completed_rows = load_resume_row_numbers(resume_results_path)

    selectors_path = Path(args.selectors_json).resolve() if args.selectors_json else None
    selector_overrides = load_selector_overrides(selectors_path)
    if selector_overrides:
        apply_selector_overrides(selector_overrides)

    records = load_input_records(input_path)
    defaults = load_defaults(defaults_path)
    jobs = build_jobs(
        records,
        defaults=defaults,
        input_dir=input_path.parent,
        start_row=args.start_row,
        limit=args.limit,
    )

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results_path = resolve_timestamped_path(
        raw_path=str(args.results_csv),
        run_timestamp=run_timestamp,
        default_suffix=".csv",
    )

    if to_clean_string(args.effective_config_json):
        effective_config_path = resolve_timestamped_path(
            raw_path=str(args.effective_config_json),
            run_timestamp=run_timestamp,
            default_suffix=".json",
        )
        write_effective_run_config(args, output_path=effective_config_path)

    failure_artifacts_dir: Path | None = None
    if args.capture_failure_artifacts or args.trace_on_failure:
        failure_artifacts_dir = resolve_timestamped_path(
            raw_path=str(args.failure_artifacts_dir),
            run_timestamp=run_timestamp,
            default_suffix=None,
        )
        failure_artifacts_dir.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Failure artifacts directory: %s", failure_artifacts_dir)

    openai_api_key: str | None = None
    if args.openai_recovery:
        openai_api_key = to_clean_string(os.getenv("OPENAI_API_KEY"))
        if not openai_api_key:
            LOGGER.warning(
                "--openai_recovery was enabled, but OPENAI_API_KEY is not set. Continuing without OpenAI recovery."
            )
        else:
            LOGGER.info("OpenAI recovery guidance enabled with model %s", args.openai_model)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(Path(args.user_data_dir).resolve()),
            headless=args.headless,
            viewport=None,
        )
        page = context.pages[0] if context.pages else context.new_page()

        open_content_area(
            page,
            action_timeout_ms=args.action_timeout_ms,
            login_timeout_seconds=args.login_timeout_seconds,
        )

        results: list[dict[str, Any]] = []
        processed_count = 0
        consecutive_failures = 0
        for job in jobs:
            if job.row_number in completed_rows:
                LOGGER.info("Skipping row %s due to resume_from_results (%s)", job.row_number, job.identifier_label)
                results.append(
                    {
                        "row_number": job.row_number,
                        "identifier": job.identifier_label,
                        "status": "resumed_skip",
                        "attempts": 0,
                        "video_id": job.video_id or "",
                        "applied_fields": "",
                        "save_result": "",
                        "error": "Skipped because prior results marked row as ok/skipped.",
                        "transient_error": "false",
                        "screenshot_path": "",
                        "html_path": "",
                        "trace_path": "",
                        "ai_guidance": "",
                    }
                )
                continue

            LOGGER.info("Processing row %s (%s)", job.row_number, job.identifier_label)
            result = process_job(
                context,
                page,
                job,
                max_retries=args.max_retries,
                action_timeout_ms=args.action_timeout_ms,
                login_timeout_seconds=args.login_timeout_seconds,
                dry_run=args.dry_run,
                tags_mode=args.tags_mode,
                playlist_mode=args.playlist_mode,
                retry_base_delay_seconds=args.retry_base_delay_seconds,
                retry_max_delay_seconds=args.retry_max_delay_seconds,
                retry_jitter_seconds=args.retry_jitter_seconds,
                retry_transient_only=args.retry_transient_only,
                capture_failure_artifacts_enabled=args.capture_failure_artifacts,
                failure_artifacts_dir=failure_artifacts_dir,
                save_failure_html=args.save_failure_html,
                trace_on_failure=args.trace_on_failure,
                openai_recovery=args.openai_recovery and bool(openai_api_key),
                openai_model=args.openai_model,
                openai_timeout_seconds=args.openai_timeout_seconds,
                openai_api_key=openai_api_key,
            )
            results.append(result)

            processed_count += 1
            if result["status"] == "failed":
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            if args.stop_after_consecutive_failures > 0 and consecutive_failures >= args.stop_after_consecutive_failures:
                LOGGER.error(
                    "Stopping early after %s consecutive failures (latest row=%s).",
                    consecutive_failures,
                    job.row_number,
                )
                break

            if args.refresh_every > 0 and processed_count % args.refresh_every == 0:
                LOGGER.info("Refreshing Studio page after %s processed row(s)", processed_count)
                open_content_area(
                    page,
                    action_timeout_ms=args.action_timeout_ms,
                    login_timeout_seconds=args.login_timeout_seconds,
                )

            if len(results) % args.checkpoint_every == 0:
                write_results(results, output_path=results_path)

        context.close()

    write_results(results, output_path=results_path)

    failed_count = sum(1 for row in results if row["status"] == "failed")
    skipped_count = sum(1 for row in results if row["status"] in {"skipped", "resumed_skip"})
    succeeded_count = sum(1 for row in results if row["status"] == "ok")

    LOGGER.info(
        "Completed metadata update run. ok=%s skipped=%s failed=%s",
        succeeded_count,
        skipped_count,
        failed_count,
    )
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

