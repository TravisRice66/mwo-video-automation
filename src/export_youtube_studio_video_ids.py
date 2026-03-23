from __future__ import annotations

import argparse
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from youtube_studio_selectors import CONTENT_NAV_SELECTORS


LOGGER = logging.getLogger(__name__)

DEFAULT_USER_DATA_DIR = ".playwright/youtube-studio-profile"
YOUTUBE_STUDIO_HOME = "https://studio.youtube.com"
YOUTUBE_STUDIO_CONTENT = "https://studio.youtube.com/channel/UC/videos"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export video IDs from YouTube Studio Content page (works for uploaded, draft, processing rows once listed).",
    )
    parser.add_argument(
        "--output_csv",
        default="output/youtube_studio_video_ids.csv",
        help="Path to write exported video rows.",
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
        "--max_scrolls",
        type=int,
        default=120,
        help="Maximum scroll iterations through the Content table.",
    )
    parser.add_argument(
        "--stall_scrolls",
        type=int,
        default=8,
        help="Stop after this many consecutive scrolls with no new video IDs found.",
    )
    parser.add_argument(
        "--scroll_pause_ms",
        type=int,
        default=900,
        help="Pause after each scroll to let rows load.",
    )
    parser.add_argument(
        "--target_rows",
        type=int,
        default=0,
        help="Optional target count to stop early. 0 means no target.",
    )
    parser.add_argument(
        "--include_row_text",
        action="store_true",
        help="Include full row text dump in output for troubleshooting.",
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
    page.wait_for_timeout(700)


def parse_video_id_from_href(href: str) -> str | None:
    href_clean = to_clean_string(href)
    if not href_clean:
        return None
    match = re.search(r"/video/([A-Za-z0-9_-]{8,})", href_clean)
    if match:
        return match.group(1)
    return None


def read_first_text(row: Locator, selectors: tuple[str, ...]) -> str:
    for selector in selectors:
        locator = row.locator(selector).first
        try:
            if locator.count() > 0:
                text = to_clean_string(locator.inner_text(timeout=500))
                if text:
                    return text
        except Exception:  # noqa: BLE001
            continue
    return ""


def extract_row_record(row: Locator, index: int, include_row_text: bool) -> dict[str, str] | None:
    link = row.locator("a[href*='/video/']").first
    if link.count() == 0:
        return None

    href = to_clean_string(link.get_attribute("href"))
    video_id = parse_video_id_from_href(href)
    if not video_id:
        return None

    studio_edit_url = href
    if studio_edit_url.startswith("/"):
        studio_edit_url = f"https://studio.youtube.com{studio_edit_url}"

    title = read_first_text(
        row,
        selectors=(
            "#video-title",
            "a#video-title",
            "a[title]",
        ),
    )
    visibility = read_first_text(row, selectors=("#visibility", "#visibility-text", "[id*='visibility']"))
    restrictions = read_first_text(row, selectors=("#restrictions", "[id*='restriction']"))
    date_text = read_first_text(row, selectors=("#date", "[id*='date']"))
    status = read_first_text(row, selectors=("#status", "[id*='status']", "[aria-label*='Status']"))

    record: dict[str, str] = {
        "discovery_index": str(index),
        "video_id": video_id,
        "studio_title": title,
        "status": status,
        "visibility": visibility,
        "restrictions": restrictions,
        "date": date_text,
        "studio_edit_url": studio_edit_url,
    }

    if include_row_text:
        try:
            record["row_text"] = to_clean_string(row.inner_text(timeout=800))
        except Exception:  # noqa: BLE001
            record["row_text"] = ""

    return record


def write_results(records: list[dict[str, str]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    dataframe = pd.DataFrame(records)
    dataframe.to_csv(output_csv, index=False)
    LOGGER.info("Wrote %s row(s) to %s", len(dataframe), output_csv)


def main() -> int:
    configure_logging()
    args = parse_args()

    if args.max_scrolls < 1:
        raise ValueError("max_scrolls must be >= 1")
    if args.stall_scrolls < 1:
        raise ValueError("stall_scrolls must be >= 1")
    if args.scroll_pause_ms < 100:
        raise ValueError("scroll_pause_ms must be >= 100")
    if args.target_rows < 0:
        raise ValueError("target_rows must be >= 0")

    output_csv = Path(args.output_csv).resolve()
    found_by_id: dict[str, dict[str, str]] = {}
    discovery_counter = 0
    no_new_scrolls = 0

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

        for scroll_index in range(1, args.max_scrolls + 1):
            visible_rows = page.locator("ytcp-video-row")
            row_count = visible_rows.count()
            new_this_scroll = 0

            for i in range(row_count):
                row = visible_rows.nth(i)
                record = extract_row_record(
                    row,
                    index=discovery_counter + 1,
                    include_row_text=args.include_row_text,
                )
                if record is None:
                    continue

                video_id = record["video_id"]
                if video_id in found_by_id:
                    continue

                discovery_counter += 1
                record["discovery_index"] = str(discovery_counter)
                found_by_id[video_id] = record
                new_this_scroll += 1

            LOGGER.info(
                "Scroll %s/%s | visible_rows=%s | total_found=%s | new=%s",
                scroll_index,
                args.max_scrolls,
                row_count,
                len(found_by_id),
                new_this_scroll,
            )

            if args.target_rows > 0 and len(found_by_id) >= args.target_rows:
                LOGGER.info("Reached target_rows=%s", args.target_rows)
                break

            if new_this_scroll == 0:
                no_new_scrolls += 1
            else:
                no_new_scrolls = 0

            if no_new_scrolls >= args.stall_scrolls:
                LOGGER.info("Stopping after %s consecutive no-new scrolls", no_new_scrolls)
                break

            page.mouse.wheel(0, 6000)
            if scroll_index % 10 == 0:
                page.keyboard.press("End")
            page.wait_for_timeout(args.scroll_pause_ms)

        context.close()

    records = sorted(found_by_id.values(), key=lambda row: int(row["discovery_index"]))
    write_results(records, output_csv=output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

