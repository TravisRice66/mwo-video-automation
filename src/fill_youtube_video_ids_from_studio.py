from __future__ import annotations

import argparse
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from youtube_studio_selectors import CONTENT_NAV_SELECTORS, SEARCH_INPUT_SELECTORS


LOGGER = logging.getLogger(__name__)

DEFAULT_USER_DATA_DIR = ".playwright/youtube-studio-profile"
YOUTUBE_STUDIO_HOME = "https://studio.youtube.com"
YOUTUBE_STUDIO_CONTENT = "https://studio.youtube.com/channel/UC/videos"
VIDEO_ID_PATTERN = re.compile(r"/vi/([A-Za-z0-9_-]{8,})/")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill youtube_metadata_updates.csv video_id values from YouTube Studio search results.",
    )
    parser.add_argument(
        "--input_csv",
        default="output/youtube_metadata_updates.csv",
        help="Metadata update CSV to enrich.",
    )
    parser.add_argument(
        "--output_csv",
        default="output/youtube_metadata_updates.csv",
        help="Output CSV path. Defaults to updating the input file in place.",
    )
    parser.add_argument(
        "--search_text",
        default="mwocomp",
        help="Studio search text used to surface the upload rows.",
    )
    parser.add_argument(
        "--user_data_dir",
        default=DEFAULT_USER_DATA_DIR,
        help="Path to persistent Chromium profile for YouTube login reuse.",
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
        "--headless",
        action="store_true",
        help="Run browser headless. Leave off for first login/debugging.",
    )
    return parser.parse_args()


def to_clean_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_lookup_title(value: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9]+", " ", to_clean_string(value).lower())
    return " ".join(collapsed.split())


def find_first_visible(scope: Page | Locator, selectors: tuple[str, ...], timeout_ms: int) -> Locator | None:
    per_selector_timeout = max(500, min(timeout_ms, 2500))
    for selector in selectors:
        locator = scope.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=per_selector_timeout)
            return locator
        except PlaywrightTimeoutError:
            continue
    return None


def wait_for_manual_login(page: Page, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current_url = page.url.lower()
        if "studio.youtube.com" in current_url and "accounts.google.com" not in current_url:
            return
        page.wait_for_timeout(1000)
    raise TimeoutError("YouTube Studio login did not complete in time.")


def open_content_area(page: Page, action_timeout_ms: int, login_timeout_seconds: int) -> None:
    page.goto(YOUTUBE_STUDIO_HOME, wait_until="domcontentloaded", timeout=60000)
    wait_for_manual_login(page, timeout_seconds=login_timeout_seconds)

    content_nav = find_first_visible(page, CONTENT_NAV_SELECTORS, timeout_ms=action_timeout_ms)
    if content_nav is not None:
        content_nav.click()
    else:
        page.goto(YOUTUBE_STUDIO_CONTENT, wait_until="domcontentloaded", timeout=60000)

    table_row = page.locator("ytcp-video-row").first
    table_row.wait_for(state="visible", timeout=max(8000, action_timeout_ms * 2))
    page.wait_for_timeout(1000)


def extract_studio_rows(page: Page) -> list[dict[str, str]]:
    rows = page.locator("ytcp-video-row")
    count = rows.count()
    extracted: list[dict[str, str]] = []
    for index in range(count):
        row = rows.nth(index)
        data = row.evaluate(
            """
(el) => {
  const titleNode = el.querySelector('#video-title') || el.querySelector('a#video-title') || el.querySelector('a[title]');
  const img = el.querySelector("img[src*='/vi/']");
  const imgSrc = img ? (img.getAttribute('src') || '') : '';
  const match = imgSrc.match(/\\/vi\\/([A-Za-z0-9_-]{8,})\\//);
  return {
    title: titleNode ? ((titleNode.textContent || '').trim()) : '',
    row_text: (el.innerText || '').trim(),
    img_video_id: match ? match[1] : '',
  };
}
"""
        )
        title = to_clean_string(data.get("title"))
        img_video_id = to_clean_string(data.get("img_video_id"))
        if not title or not img_video_id:
            continue
        extracted.append(
            {
                "studio_title": title,
                "normalized_title": normalize_lookup_title(title),
                "row_text": to_clean_string(data.get("row_text")),
                "video_id": img_video_id,
            }
        )
    return extracted


def load_metadata(path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"current_title", "new_title"}
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")
    return dataframe


def fill_video_ids(dataframe: pd.DataFrame, extracted_rows: list[dict[str, str]]) -> tuple[pd.DataFrame, int]:
    preferred: dict[str, str] = {}
    for row in extracted_rows:
        normalized_title = row["normalized_title"]
        row_text = row["row_text"]
        video_id = row["video_id"]
        is_uploaded = "uploaded" in row_text.lower()
        if normalized_title not in preferred or is_uploaded:
            preferred[normalized_title] = video_id

    updated = dataframe.copy()
    fill_count = 0
    for index, row in updated.iterrows():
        current_video_id = to_clean_string(row.get("video_id", ""))
        if current_video_id:
            continue
        normalized_current = normalize_lookup_title(to_clean_string(row.get("current_title", "")))
        matched_video_id = preferred.get(normalized_current, "")
        if not matched_video_id:
            continue
        updated.at[index, "video_id"] = matched_video_id
        fill_count += 1
    return updated, fill_count


def main() -> int:
    configure_logging()
    args = parse_args()

    input_csv = Path(args.input_csv).expanduser().resolve()
    output_csv = Path(args.output_csv).expanduser().resolve()

    metadata_df = load_metadata(input_csv)

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

        search_input = find_first_visible(page, SEARCH_INPUT_SELECTORS, timeout_ms=args.action_timeout_ms)
        if search_input is None:
            raise RuntimeError("Could not find YouTube Studio search input.")
        search_input.click()
        search_input.fill("")
        search_input.type(args.search_text, delay=3)
        search_input.press("Enter")
        page.wait_for_timeout(3000)

        extracted_rows = extract_studio_rows(page)
        context.close()

    LOGGER.info("Extracted %s Studio row(s) with thumbnail-derived video IDs", len(extracted_rows))
    updated_df, fill_count = fill_video_ids(metadata_df, extracted_rows)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    updated_df.to_csv(output_csv, index=False)
    LOGGER.info("Filled %s missing video_id value(s) into %s", fill_count, output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
