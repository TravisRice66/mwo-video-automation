from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
]
SUPPORTED_INPUT_SUFFIXES = {".csv", ".json"}
RESULT_COLUMNS = [
    "row_number",
    "identifier",
    "status",
    "attempts",
    "video_id",
    "applied_fields",
    "save_result",
    "error",
]
CLEAR_SENTINEL = "__CLEAR__"


@dataclass(frozen=True)
class VideoJob:
    row_number: int
    identifier: str
    video_id: str
    updates: dict[str, Any]


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk update YouTube video metadata via the YouTube Data API.",
    )
    parser.add_argument("--input", default="", help="Path to CSV or JSON with one row/object per video.")
    parser.add_argument("--defaults_json", default="", help="Optional JSON file containing reusable default fields.")
    parser.add_argument(
        "--credentials_json",
        default="config/youtube_client_secret.json",
        help="OAuth client secrets JSON downloaded from Google Cloud.",
    )
    parser.add_argument(
        "--token_json",
        default="config/youtube_oauth_token.json",
        help="Path to store the reusable OAuth token.",
    )
    parser.add_argument(
        "--results_csv",
        default="output/youtube_metadata_update_results.csv",
        help="Where to write per-row update results.",
    )
    parser.add_argument("--start_row", "--start-row", type=int, default=1, help="1-based input row number to start from.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows to process after start_row. 0 means all.")
    parser.add_argument(
        "--checkpoint_every",
        type=int,
        default=10,
        help="Write running results CSV every N processed rows.",
    )
    parser.add_argument(
        "--resume_from_results",
        "--resume-from-results",
        default="",
        help="Optional previous results CSV. ok/skipped rows are auto-skipped.",
    )
    parser.add_argument(
        "--tags_mode",
        "--tags-mode",
        choices=["replace", "append"],
        default="replace",
        help="Whether supplied tags replace or append to existing tags.",
    )
    parser.add_argument("--dry_run", "--dry-run", action="store_true", help="Validate and preview without updating.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def load_google_clients() -> tuple[Any, Any, Any, Any, Any]:
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Missing Google API dependencies. Install: "
            "pip install google-api-python-client google-auth-oauthlib google-auth-httplib2"
        ) from exc
    return Request, Credentials, InstalledAppFlow, build, MediaFileUpload


def to_clean_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    return False


def normalize_key(key: str) -> str:
    value = key.lstrip("\ufeff").strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = value.lower()
    return re.sub(r"[\s\-]+", "_", value)


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in record.items():
        if key is None:
            continue
        normalized[normalize_key(str(key))] = value
    return normalized


def load_input_records(input_path: Path) -> list[dict[str, Any]]:
    suffix = input_path.suffix.lower()
    if suffix not in SUPPORTED_INPUT_SUFFIXES:
        raise ValueError(f"Unsupported input format: {suffix}. Supported: {sorted(SUPPORTED_INPUT_SUFFIXES)}")

    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"Input CSV has no headers: {input_path}")
            rows = [normalize_record(row) for row in reader]
        return [row for row in rows if any(not is_blank(value) for value in row.values())]

    with input_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict) and isinstance(payload.get("videos"), list):
        raw_rows = payload["videos"]
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raise ValueError("JSON input must be either a list of objects or an object with a 'videos' list.")

    if not all(isinstance(row, dict) for row in raw_rows):
        raise ValueError("Each JSON input item must be an object.")
    return [normalize_record(row) for row in raw_rows]


def load_defaults(defaults_path: Path | None) -> dict[str, Any]:
    if defaults_path is None:
        return {}
    if not defaults_path.exists():
        raise FileNotFoundError(f"Defaults file does not exist: {defaults_path}")
    with defaults_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("defaults_json must be a JSON object.")
    return normalize_record(payload)


def parse_video_id(value: Any) -> str | None:
    text = to_clean_string(value)
    if not text:
        return None
    if re.fullmatch(r"[A-Za-z0-9_-]{8,}", text):
        return text
    for pattern in [
        r"studio\.youtube\.com/video/([A-Za-z0-9_-]{8,})",
        r"youtu\.be/([A-Za-z0-9_-]{8,})",
        r"[?&]v=([A-Za-z0-9_-]{8,})",
        r"/shorts/([A-Za-z0-9_-]{8,})",
    ]:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def parse_visibility(value: Any) -> str | None:
    text = to_clean_string(value).lower()
    if not text:
        return None
    if text in {"public", "unlisted", "private"}:
        return text
    raise ValueError(f"Unsupported visibility value: {value}")


def parse_audience(value: Any) -> str | None:
    text = to_clean_string(value).lower().replace("-", " ").replace("_", " ")
    if not text:
        return None
    if text in {"made for kids", "yes", "kids", "true", "1"}:
        return "made_for_kids"
    if text in {"not made for kids", "no", "not for kids", "false", "0"}:
        return "not_made_for_kids"
    raise ValueError(f"Unsupported audience value: {value}")


def parse_publish_at(value: Any) -> str:
    text = to_clean_string(value)
    if text == CLEAR_SENTINEL:
        return CLEAR_SENTINEL
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(
            "publish_at must be an ISO-8601 timestamp like 2026-03-23T15:00:00-05:00 or 2026-03-23T20:00:00Z"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError("publish_at must include a timezone offset.")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_tags(value: Any) -> list[str] | str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [to_clean_string(tag) for tag in value if to_clean_string(tag)]
    text = to_clean_string(value)
    if not text:
        return None
    if text == CLEAR_SENTINEL:
        return CLEAR_SENTINEL
    if text.startswith("["):
        payload = json.loads(text)
        if not isinstance(payload, list):
            raise ValueError("tags JSON value must be an array.")
        return [to_clean_string(tag) for tag in payload if to_clean_string(tag)]
    return [tag.strip() for tag in text.split(",") if tag.strip()]


def parse_playlist_reference(value: Any) -> str | None:
    text = to_clean_string(value)
    if not text:
        return None
    if text == CLEAR_SENTINEL:
        raise ValueError("Clearing playlist membership is not supported by this script.")
    return text


def build_updates(record: dict[str, Any], defaults: dict[str, Any], input_dir: Path) -> dict[str, Any]:
    combined = dict(defaults)
    for key, value in record.items():
        if not is_blank(value):
            combined[key] = value

    updates: dict[str, Any] = {}

    title_value = combined.get("new_title", combined.get("title"))
    if not is_blank(title_value):
        title_text = to_clean_string(title_value)
        if title_text == CLEAR_SENTINEL:
            raise ValueError("Title cannot be cleared.")
        updates["title"] = title_text

    if not is_blank(combined.get("description")):
        description_text = str(combined["description"])
        updates["description"] = "" if to_clean_string(description_text) == CLEAR_SENTINEL else description_text

    tags_value = parse_tags(combined.get("tags"))
    if tags_value is not None:
        updates["tags"] = tags_value

    playlist_value = parse_playlist_reference(combined.get("playlist"))
    if playlist_value is not None:
        updates["playlist"] = playlist_value

    audience_value = parse_audience(combined.get("audience"))
    if audience_value is not None:
        updates["audience"] = audience_value

    visibility_value = parse_visibility(combined.get("visibility", combined.get("publish_status")))
    if visibility_value is not None:
        updates["visibility"] = visibility_value

    publish_source = combined.get("publish_at_iso", combined.get("publish_at"))
    if not is_blank(publish_source):
        updates["publishAt"] = parse_publish_at(publish_source)

    category_source = combined.get("category_id")
    if not is_blank(category_source):
        updates["categoryId"] = to_clean_string(category_source)

    thumbnail_source = combined.get("thumbnail_path")
    if not is_blank(thumbnail_source):
        thumbnail_path = Path(to_clean_string(thumbnail_source))
        if not thumbnail_path.is_absolute():
            thumbnail_path = (input_dir / thumbnail_path).resolve()
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

        video_id = parse_video_id(record.get("video_id", record.get("video_url", record.get("url"))))
        if not video_id:
            raise ValueError(
                f"Row {row_number} is missing a valid video identifier. Provide video_id, video_url, or url."
            )

        jobs.append(
            VideoJob(
                row_number=row_number,
                identifier=video_id,
                video_id=video_id,
                updates=build_updates(record, defaults=defaults, input_dir=input_dir),
            )
        )
    return jobs


def authenticate_service(credentials_path: Path, token_path: Path) -> Any:
    Request, Credentials, InstalledAppFlow, build, _ = load_google_clients()

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"credentials_json file does not exist: {credentials_path}. "
            "Create an OAuth Desktop App client in Google Cloud and download the JSON first."
        )

    credentials = None
    if token_path.exists():
        credentials = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    if not credentials or not credentials.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        credentials = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(credentials.to_json(), encoding="utf-8")
    return build("youtube", "v3", credentials=credentials, cache_discovery=False)


def fetch_video_state(service: Any, video_id: str) -> dict[str, Any]:
    response = service.videos().list(part="snippet,status", id=video_id, maxResults=1).execute()
    items = response.get("items", [])
    if not items:
        raise ValueError(f"Video not found or not accessible with current credentials: {video_id}")
    return items[0]


def extract_writable_part(source: dict[str, Any], allowed_keys: set[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in allowed_keys:
        if key in source and source[key] not in (None, ""):
            payload[key] = source[key]
    return payload


def merge_tags(existing_tags: list[str], new_tags: list[str]) -> list[str]:
    merged: list[str] = []
    for tag in existing_tags + new_tags:
        if tag and tag not in merged:
            merged.append(tag)
    return merged


def build_update_payload(existing_video: dict[str, Any], updates: dict[str, Any], tags_mode: str) -> tuple[list[str], dict[str, Any], list[str]]:
    snippet = extract_writable_part(existing_video.get("snippet", {}), {"title", "description", "tags", "categoryId"})
    status = extract_writable_part(existing_video.get("status", {}), {"privacyStatus", "publishAt", "selfDeclaredMadeForKids"})
    parts: list[str] = []
    applied: list[str] = []

    if "title" in updates:
        snippet["title"] = updates["title"]
        applied.append("title")
    if "description" in updates:
        snippet["description"] = updates["description"]
        applied.append("description")
    if "categoryId" in updates:
        snippet["categoryId"] = updates["categoryId"]
        applied.append("category_id")
    if "tags" in updates:
        if updates["tags"] == CLEAR_SENTINEL:
            snippet["tags"] = []
        elif tags_mode == "append":
            existing_tags = [to_clean_string(tag) for tag in snippet.get("tags", []) if to_clean_string(tag)]
            snippet["tags"] = merge_tags(existing_tags, updates["tags"])
        else:
            snippet["tags"] = updates["tags"]
        applied.append("tags")
    if applied and not to_clean_string(snippet.get("title")):
        raise ValueError("Snippet update requires a title.")
    if any(field in applied for field in {"title", "description", "category_id", "tags"}):
        if not to_clean_string(snippet.get("categoryId")):
            raise ValueError("Snippet update requires categoryId. Provide category_id or set it in Studio first.")
        parts.append("snippet")

    if "audience" in updates:
        status["selfDeclaredMadeForKids"] = updates["audience"] == "made_for_kids"
        applied.append("audience")
    if "visibility" in updates:
        status["privacyStatus"] = updates["visibility"]
        applied.append("visibility")
    if "publishAt" in updates:
        if updates["publishAt"] == CLEAR_SENTINEL:
            status.pop("publishAt", None)
        else:
            status["publishAt"] = updates["publishAt"]
        applied.append("publish_at")
    if any(field in applied for field in {"audience", "visibility", "publish_at"}):
        if "publishAt" in status and status.get("privacyStatus") != "private":
            raise ValueError("publish_at requires visibility/private on the target video.")
        if status.get("privacyStatus") != "private":
            status.pop("publishAt", None)
        parts.append("status")

    body: dict[str, Any] = {"id": existing_video["id"]}
    if "snippet" in parts:
        body["snippet"] = snippet
    if "status" in parts:
        body["status"] = status
    return parts, body, applied


def extract_playlist_id(value: str) -> str | None:
    text = to_clean_string(value)
    if not text:
        return None
    if re.match(r"^(PL|UU|LL|FL|OL|RD)[A-Za-z0-9_-]{8,}$", text):
        return text
    match = re.search(r"[?&]list=([A-Za-z0-9_-]{8,})", text)
    if match:
        return match.group(1)
    return None


def resolve_playlist_id(service: Any, raw_value: str, playlist_cache: dict[str, str]) -> str:
    direct_id = extract_playlist_id(raw_value)
    if direct_id:
        return direct_id

    cache_key = raw_value.strip().lower()
    if cache_key in playlist_cache:
        return playlist_cache[cache_key]

    page_token = ""
    while True:
        response = service.playlists().list(part="snippet", mine=True, maxResults=50, pageToken=page_token or None).execute()
        for item in response.get("items", []):
            title = to_clean_string(item.get("snippet", {}).get("title"))
            if title.lower() == cache_key:
                playlist_id = to_clean_string(item.get("id"))
                playlist_cache[cache_key] = playlist_id
                return playlist_id
        page_token = to_clean_string(response.get("nextPageToken"))
        if not page_token:
            break
    raise ValueError(f"Could not find a channel playlist named '{raw_value}'.")


def ensure_playlist_membership(service: Any, video_id: str, playlist_reference: str, playlist_cache: dict[str, str]) -> None:
    playlist_id = resolve_playlist_id(service, playlist_reference, playlist_cache)
    page_token = ""
    while True:
        response = service.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token or None,
        ).execute()
        for item in response.get("items", []):
            resource_id = item.get("snippet", {}).get("resourceId", {})
            if to_clean_string(resource_id.get("videoId")) == video_id:
                return
        page_token = to_clean_string(response.get("nextPageToken"))
        if not page_token:
            break
    service.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            }
        },
    ).execute()


def upload_thumbnail(service: Any, video_id: str, thumbnail_path: Path) -> None:
    _, _, _, _, MediaFileUpload = load_google_clients()
    if not thumbnail_path.exists():
        raise FileNotFoundError(f"thumbnail_path does not exist: {thumbnail_path}")
    media_body = MediaFileUpload(str(thumbnail_path), resumable=False)
    service.thumbnails().set(videoId=video_id, media_body=media_body).execute()


def process_job(service: Any, job: VideoJob, tags_mode: str, dry_run: bool, playlist_cache: dict[str, str]) -> dict[str, str]:
    if not job.updates:
        return {
            "row_number": str(job.row_number),
            "identifier": job.identifier,
            "status": "skipped",
            "attempts": "0",
            "video_id": job.video_id,
            "applied_fields": "",
            "save_result": "",
            "error": "No metadata fields were provided for update.",
        }

    existing_video = fetch_video_state(service, job.video_id)
    parts, body, applied = build_update_payload(existing_video, job.updates, tags_mode=tags_mode)

    if dry_run:
        preview_actions: list[str] = []
        if parts:
            preview_actions.append(f"would_update_parts={','.join(parts)}")
        if "playlist" in job.updates:
            preview_actions.append("would_ensure_playlist")
        if "thumbnail_path" in job.updates:
            preview_actions.append("would_set_thumbnail")
        final_fields = list(applied)
        if "playlist" in job.updates:
            final_fields.append("playlist")
        if "thumbnail_path" in job.updates:
            final_fields.append("thumbnail_path")
        return {
            "row_number": str(job.row_number),
            "identifier": job.identifier,
            "status": "ok",
            "attempts": "1",
            "video_id": job.video_id,
            "applied_fields": ",".join(final_fields),
            "save_result": "; ".join(preview_actions) or "dry_run_no_changes",
            "error": "",
        }

    save_actions: list[str] = []
    if parts:
        service.videos().update(part=",".join(parts), body=body).execute()
        save_actions.append(f"updated_parts={','.join(parts)}")
    if "playlist" in job.updates:
        ensure_playlist_membership(service, job.video_id, job.updates["playlist"], playlist_cache)
        save_actions.append("playlist_ensured")
    if "thumbnail_path" in job.updates:
        upload_thumbnail(service, job.video_id, job.updates["thumbnail_path"])
        save_actions.append("thumbnail_uploaded")

    final_fields = list(applied)
    if "playlist" in job.updates:
        final_fields.append("playlist")
    if "thumbnail_path" in job.updates:
        final_fields.append("thumbnail_path")
    return {
        "row_number": str(job.row_number),
        "identifier": job.identifier,
        "status": "ok",
        "attempts": "1",
        "video_id": job.video_id,
        "applied_fields": ",".join(final_fields),
        "save_result": "; ".join(save_actions),
        "error": "",
    }


def write_results(results: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        for row in results:
            writer.writerow({column: row.get(column, "") for column in RESULT_COLUMNS})
    LOGGER.info("Wrote %s result row(s) to %s", len(results), output_path)


def load_resume_row_numbers(path: Path | None) -> set[int]:
    if path is None:
        return set()
    if not path.exists():
        raise FileNotFoundError(f"resume_from_results file does not exist: {path}")

    completed: set[int] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            status = to_clean_string(row.get("status")).lower()
            if status not in {"ok", "skipped"}:
                continue
            row_number_raw = to_clean_string(row.get("row_number"))
            if not row_number_raw:
                continue
            try:
                completed.add(int(row_number_raw))
            except ValueError:
                LOGGER.warning("Ignoring non-integer row_number in resume file: %s", row_number_raw)
    LOGGER.info("Loaded %s completed row(s) from %s", len(completed), path)
    return completed


def main() -> int:
    args = parse_args()
    configure_logging(verbose=args.verbose)

    if not to_clean_string(args.input):
        raise ValueError("--input is required.")
    if args.checkpoint_every < 1:
        raise ValueError("checkpoint_every must be >= 1")

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    defaults_path = Path(args.defaults_json).resolve() if args.defaults_json else None
    credentials_path = Path(args.credentials_json).resolve()
    token_path = Path(args.token_json).resolve()
    results_path = Path(args.results_csv).resolve()
    resume_path = Path(args.resume_from_results).resolve() if args.resume_from_results else None

    jobs = build_jobs(
        load_input_records(input_path),
        defaults=load_defaults(defaults_path),
        input_dir=input_path.parent,
        start_row=args.start_row,
        limit=args.limit,
    )
    completed_rows = load_resume_row_numbers(resume_path)
    service = authenticate_service(credentials_path=credentials_path, token_path=token_path)

    results: list[dict[str, str]] = []
    playlist_cache: dict[str, str] = {}

    for job in jobs:
        if job.row_number in completed_rows:
            results.append(
                {
                    "row_number": str(job.row_number),
                    "identifier": job.identifier,
                    "status": "resumed_skip",
                    "attempts": "0",
                    "video_id": job.video_id,
                    "applied_fields": "",
                    "save_result": "",
                    "error": "Skipped because prior results marked row as ok/skipped.",
                }
            )
            continue

        LOGGER.info("Processing row %s (%s)", job.row_number, job.identifier)
        try:
            result = process_job(
                service,
                job,
                tags_mode=args.tags_mode,
                dry_run=args.dry_run,
                playlist_cache=playlist_cache,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Row %s failed", job.row_number)
            result = {
                "row_number": str(job.row_number),
                "identifier": job.identifier,
                "status": "failed",
                "attempts": "1",
                "video_id": job.video_id,
                "applied_fields": "",
                "save_result": "",
                "error": str(exc),
            }
        results.append(result)
        if len(results) % args.checkpoint_every == 0:
            write_results(results, results_path)

    write_results(results, results_path)
    failed_count = sum(1 for row in results if row["status"] == "failed")
    skipped_count = sum(1 for row in results if row["status"] in {"skipped", "resumed_skip"})
    ok_count = sum(1 for row in results if row["status"] == "ok")
    LOGGER.info("Completed YouTube metadata run. ok=%s skipped=%s failed=%s", ok_count, skipped_count, failed_count)
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
