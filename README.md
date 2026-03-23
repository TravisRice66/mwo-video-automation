# mwo-video-automation

Practical scripts for MWO video workflows, including a working MVP for bulk metadata updates on already-uploaded YouTube videos.

## Recommended approach for your exact problem

For **any number of existing uploads** (small or large batches), the most reliable path is:

1. Use YouTube Studio UI automation with [`Playwright`](src/update_youtube_metadata.py:1).
2. Prefer identifying each video by `video_id` (most stable).
3. Fall back to `current_title` / filename text search when ID is unavailable.
4. Apply only provided fields (partial update behavior) so blanks do not overwrite data.
5. Keep login persistent via a local browser profile (no re-login each run).

Why this approach:
- YouTube Studio native bulk editing is limited and inconsistent across fields (especially tags, audience, playlist combinations).
- A per-video loop via UI is slower than a true API bulk endpoint, but in practice it is robust and works **now** for already uploaded videos.

- [src/update_youtube_metadata_api.py](src/update_youtube_metadata_api.py:1)
  - Uses the YouTube Data API with OAuth instead of Studio UI automation.
  - Updates title, description, tags, playlist membership, audience, visibility, scheduled publish time, category, and optional thumbnail.
  - Uses the same CSV/JSON row format and results CSV pattern as the Studio updater for smaller, cleaner automation runs when the API supports the field.

- [`src/update_youtube_metadata.py`](src/update_youtube_metadata.py:1)
  - Reads CSV or JSON.
  - Supports per-row mapping by `video_id`, URL, current title, filename text.
  - Updates title, description, tags, playlist, audience, visibility, optional thumbnail.
  - Supports defaults file.
  - Retries failed rows.
  - Supports resumable large runs via prior results CSV.
  - Checkpoints results periodically during execution.
  - Periodic Studio refresh + optional early-stop on repeated failures.
  - Logs status and writes per-row results CSV.

- [`src/youtube_studio_selectors.py`](src/youtube_studio_selectors.py:1)
  - Selector constants separated for easier maintenance when YouTube UI changes.

- [`src/export_metadata_update_template.py`](src/export_metadata_update_template.py:1)
  - Creates a metadata-update template from [`output/youtube_upload_plan.csv`](output/youtube_upload_plan.csv).

## Setup (Windows)

From repository root (`mwo-video-automation`):

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

Dependencies are listed in [`requirements.txt`](requirements.txt).

## Input format (CSV/JSON)

The updater accepts either CSV or JSON with one row/object per video.

### Supported identifier fields

- `video_id` (preferred)
- `video_url` / `url`
- `current_title`
- `lookup_title`
- `current_filename` / `filename`

### Supported update fields

- `new_title` (or `title`)
- `description`
- `tags` (comma-separated in CSV, array or comma-separated in JSON)
- `playlist`
- `audience` (`not_made_for_kids` / `made_for_kids`, plus common synonyms)
- `visibility` or `publish_status` (`public`, `unlisted`, `private`)
- `thumbnail_path` (optional)

If a field is blank/missing, it is skipped (not overwritten).

See examples:
- [`data/youtube_metadata_updates.sample.csv`](data/youtube_metadata_updates.sample.csv)
- [`data/youtube_metadata_updates.sample.json`](data/youtube_metadata_updates.sample.json)
- [`config/youtube_metadata_defaults.sample.json`](config/youtube_metadata_defaults.sample.json)
- [`config/youtube_updater_run_config.sample.json`](config/youtube_updater_run_config.sample.json)
- [`config/youtube_selector_overrides.sample.json`](config/youtube_selector_overrides.sample.json)

## Generate starter CSV from existing plan

If you already have [`output/youtube_upload_plan.csv`](output/youtube_upload_plan.csv), generate an editable template:

```bat
python src/export_metadata_update_template.py --input_csv output/youtube_upload_plan.csv --output_csv output/youtube_metadata_updates.csv
```

Then fill `video_id` where possible for best reliability.

## Run metadata updater

### First run (recommended dry run)

```bat
python src/update_youtube_metadata.py --input output/youtube_metadata_updates.csv --defaults_json config/youtube_metadata_defaults.sample.json --dry_run
```

First launch may require manual login in the opened Chromium window. Session persists in `.playwright/youtube-studio-profile`.

Optional AI recovery guidance:

```bat
set OPENAI_API_KEY=your_key_here
python src/update_youtube_metadata.py --input output/youtube_metadata_updates.csv --defaults_json config/youtube_metadata_defaults.sample.json --dry_run --openai_recovery
```

When enabled, failed rows can ask the OpenAI Responses API for a bounded recovery recommendation (`retry_same_page`, `reopen_content_area`, `reload_editor`, or `abort_row`). The guidance is advisory and is also written to the results CSV in `ai_guidance`.

### Real run (saves changes)

```bat
python src/update_youtube_metadata.py --input output/youtube_metadata_updates.csv --defaults_json config/youtube_metadata_defaults.sample.json --results_csv output/youtube_metadata_update_results_{timestamp}.csv
```

Useful flags:

- `--max_retries 2`
- `--start_row 1 --limit 0` (0 = no limit, process all rows)
- `--tags_mode replace|append`
- `--playlist_mode replace|append`
- `--checkpoint_every 10`
- `--refresh_every 50`
- `--resume_from_results output/youtube_metadata_update_results_prev.csv`
- `--stop_after_consecutive_failures 5`
- `--retry_base_delay_seconds 1.2 --retry_max_delay_seconds 20 --retry_jitter_seconds 0.8`
- `--retry_transient_only`
- `--capture_failure_artifacts --save_failure_html --trace_on_failure`
- `--failure_artifacts_dir output/youtube_metadata_failure_artifacts/{timestamp}`
- `--selectors_json config/youtube_selector_overrides.sample.json`
- `--run_config_json config/youtube_updater_run_config.sample.json`
- `--effective_config_json output/youtube_effective_run_config_{timestamp}.json`
- `--openai_recovery --openai_model gpt-5-mini --openai_timeout_seconds 20`
- `--headless` (optional, usually better off while tuning)

### Reproducible config-driven run

Run with a committed config file and capture the exact effective config used:

```bat
python src/update_youtube_metadata.py --run_config_json config/youtube_updater_run_config.sample.json --effective_config_json output/youtube_effective_run_config_{timestamp}.json
```

CLI arguments override values in `--run_config_json`.

### Resume workflow after interruption

```bat
python src/update_youtube_metadata.py --input output/youtube_metadata_updates.csv --results_csv output/youtube_metadata_update_results_resume.csv --resume_from_results output/youtube_metadata_update_results_prev.csv
```

Rows previously marked `ok` or `skipped` are auto-skipped as `resumed_skip`.

### Selector drift workflow

If YouTube Studio UI changes and a selector breaks:

1. Copy [`config/youtube_selector_overrides.sample.json`](config/youtube_selector_overrides.sample.json) and adjust only failing selector arrays.
2. Run with `--selectors_json <your_overrides.json>`.
3. Keep overrides in version control for reproducibility.

## Output

Updater writes a result file (default):
- [`output/youtube_metadata_update_results.csv`](output/youtube_metadata_update_results.csv)

Columns include `status`, `attempts`, `identifier`, `applied_fields`, `save_result`, `error`, `transient_error`, and optional artifact paths (`screenshot_path`, `html_path`, `trace_path`).

If OpenAI recovery is enabled, results also include `ai_guidance`.

## Notes on reliability

- YouTube Studio UI changes occasionally; update selectors in [`src/youtube_studio_selectors.py`](src/youtube_studio_selectors.py:1) if needed.
- Prefer `video_id` to avoid title collision issues.
- Run a small smoke test first (`--limit 3`) before full-scale runs.
- For very large runs, use checkpoint + resume flow:
  1. run with `--checkpoint_every`
  2. if interrupted, restart with `--resume_from_results <previous_results_csv>`
- For unknown failures, enable `--capture_failure_artifacts` and optionally `--trace_on_failure` for deep diagnosis.

## API-based updater option

If you prefer the official YouTube Data API over Studio UI automation, use [`src/update_youtube_metadata_api.py`](src/update_youtube_metadata_api.py:1).

Important notes:
- This path uses OAuth client credentials, not an API key.
- It works best when each row includes `video_id`.
- Playlist behavior is additive: it ensures the video is in the requested playlist.
- Some fields only work if the current video already has the required metadata Google expects in the update payload; the script preserves existing `categoryId` when present.

### One-time setup for the API updater

1. In Google Cloud, enable `YouTube Data API v3`.
2. Create an OAuth Client ID of type `Desktop app`.
3. Save the downloaded JSON as `config/youtube_client_secret.json`.
4. Install dependencies from [`requirements.txt`](requirements.txt).

### First API dry run

```bat
python src/update_youtube_metadata_api.py --input output/youtube_metadata_updates.csv --defaults_json config/youtube_metadata_defaults.sample.json --dry_run
```

The first run opens a browser for Google sign-in and writes a reusable token to `config/youtube_oauth_token.json`.

### Real API run

```bat
python src/update_youtube_metadata_api.py --input output/youtube_metadata_updates.csv --defaults_json config/youtube_metadata_defaults.sample.json --results_csv output/youtube_metadata_update_results_api.csv
```
