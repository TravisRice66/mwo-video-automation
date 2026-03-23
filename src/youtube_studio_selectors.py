from __future__ import annotations

# Centralized selector candidates for YouTube Studio UI automation.
# Keep multiple fallbacks because Studio markup changes periodically.

CONTENT_NAV_SELECTORS = (
    "a[title='Content']",
    "a[aria-label='Content']",
    "tp-yt-paper-item:has-text('Content')",
)

SEARCH_INPUT_SELECTORS = (
    "input#search-input",
    "input[placeholder*='Search across your channel']",
    "input[aria-label*='Search across your channel']",
)

VIDEO_ROW_TITLE_LINK_SELECTORS = (
    "ytcp-video-row a[href*='/video/']",
    "a[href*='/video/'][id='video-title']",
)

SAVE_BUTTON_SELECTORS = (
    "button:has-text('Save')",
    "ytcp-button#save button",
)

TITLE_INPUT_SELECTORS = (
    "ytcp-social-suggestions-textbox#title-textarea #textbox",
    "ytcp-social-suggestion-input#title-textarea #textbox",
    "textarea[aria-label*='title']",
)

DESCRIPTION_INPUT_SELECTORS = (
    "ytcp-social-suggestions-textbox#description-textarea #textbox",
    "ytcp-social-suggestion-input#description-textarea #textbox",
    "textarea[aria-label*='description']",
)

SHOW_MORE_BUTTON_SELECTORS = (
    "button:has-text('Show more')",
    "ytcp-button#toggle-button button:has-text('Show more')",
)

TAGS_INPUT_SELECTORS = (
    "input[aria-label*='Tags']",
    "ytcp-freezable-editor#tags-container input",
)

PLAYLIST_DROPDOWN_SELECTORS = (
    "ytcp-video-metadata-playlists ytcp-dropdown-trigger",
    "ytcp-video-metadata-playlists button",
    "#basics ytcp-video-metadata-playlists",
)

PLAYLIST_DIALOG_SELECTORS = (
    "ytcp-playlist-dialog",
    "ytcp-dialog tp-yt-paper-dialog",
    "tp-yt-paper-dialog:has(input[placeholder*='Search for a playlist'])",
)

PLAYLIST_SEARCH_INPUT_SELECTORS = (
    "ytcp-playlist-dialog input",
    "input[placeholder*='Search for a playlist']",
    "input[placeholder*='Search']",
)

PLAYLIST_DONE_BUTTON_SELECTORS = (
    "ytcp-playlist-dialog button:has-text('Done')",
    "ytcp-playlist-dialog button:has-text('Save')",
    "tp-yt-paper-dialog button:has-text('Done')",
    "button:has-text('Done')",
)

THUMBNAIL_FILE_INPUT_SELECTORS = (
    "input[type='file'][accept*='image']",
    "input[type='file']",
)

VISIBILITY_DROPDOWN_SELECTORS = (
    "ytcp-video-visibility-select ytcp-dropdown-trigger",
    "ytcp-video-visibility-select button",
)

