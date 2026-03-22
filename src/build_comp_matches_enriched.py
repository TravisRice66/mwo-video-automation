from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)

DEFAULT_PLAYER_NAME = "NubyaTheRealtor"
OUTPUT_COLUMNS = [
    "match_id",
    "game_time",
    "player_name",
    "team_tag",
    "mech",
    "round",
    "game",
    "map",
    "validation_status",
    "notes",
]
MATCHES_COLUMN_MAP = {
    "match_id": "match_id",
    "match_time": "game_time",
    "name": "player_name",
    "unit": "team_tag",
    "mechname": "mech",
    "team": "team_number",
}
REQUIRED_MATCHES_COLUMNS = set(MATCHES_COLUMN_MAP)
REQUIRED_MATCHES2_COLUMNS = {"map", "team_a", "team_b"}
REQUIRED_SCHEDULE_COLUMNS = {"round", "game", "map"}
REQUIRED_ROSTER_COLUMNS = {"Division", "Team", "Pilot"}

MAP_ALIASES = {
    "bearclaw": "bearclaw",
    "bearclawii": "bearclaw",
    "causticvalley": "causticvalley",
    "emeraldtaigaqp": "emeraldvale",
    "emeraldvale": "emeraldvale",
    "frozencitynight": "frozencitynight",
    "frozencitynightclassic": "frozencitynight",
    "hibernalrift": "hibernalrift",
    "polarhighlands": "polarhighlands",
    "rivercity": "rivercity",
    "terrathermaqp": "terrathermacrucible",
    "terrathermacrucible": "terrathermacrucible",
    "vitricforgeqp": "vitricstation",
    "vitricstation": "vitricstation",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build one-row-per-match competitive table for a target player."
    )
    parser.add_argument(
        "--data_dir",
        default="data",
        help="Directory containing the local CSV inputs.",
    )
    parser.add_argument(
        "--player_name",
        default=DEFAULT_PLAYER_NAME,
        help="Target player name to extract from matches.csv.",
    )
    parser.add_argument(
        "--output",
        default="data/comp_matches_enriched.csv",
        help="Output CSV path.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str)


def ensure_columns(dataframe: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        LOGGER.error("Missing required columns in %s: %s", source_name, ", ".join(missing_columns))
        raise ValueError(f"Missing required columns in {source_name}: {missing_columns}")


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def normalize_map_name(value: object) -> str:
    normalized = normalize_text(value)
    return MAP_ALIASES.get(normalized, normalized)


def split_names(value: object) -> list[str]:
    if pd.isna(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def load_matches(matches_path: Path) -> pd.DataFrame:
    dataframe = read_csv(matches_path)
    ensure_columns(dataframe, REQUIRED_MATCHES_COLUMNS, matches_path.name)

    normalized = dataframe.rename(columns=MATCHES_COLUMN_MAP).loc[:, MATCHES_COLUMN_MAP.values()].copy()
    normalized["match_id"] = normalized["match_id"].astype(str).str.strip()
    normalized["player_name"] = normalized["player_name"].astype(str).str.strip()
    normalized["team_tag"] = normalized["team_tag"].astype(str).str.strip()
    normalized["mech"] = normalized["mech"].astype(str).str.strip()
    normalized["team_number"] = normalized["team_number"].astype(str).str.strip()
    normalized["game_time"] = pd.to_datetime(normalized["game_time"], errors="coerce")

    invalid_times = normalized["game_time"].isna().sum()
    if invalid_times:
        LOGGER.warning("Found %s rows in matches.csv with invalid game_time values.", invalid_times)

    LOGGER.info("Loaded %s rows from %s", len(normalized), matches_path)
    return normalized


def load_lobby_ids(path: Path) -> set[str]:
    dataframe = read_csv(path)
    first_column = dataframe.columns[0]
    lobby_ids = dataframe[first_column].astype(str).str.strip()

    duplicate_count = int(lobby_ids.duplicated().sum())
    if duplicate_count:
        LOGGER.warning("Found %s duplicate lobby IDs in %s", duplicate_count, path.name)

    unique_ids = set(lobby_ids)
    LOGGER.info("Loaded %s unique lobby IDs from %s", len(unique_ids), path)
    return unique_ids


def filter_comp_matches(matches_df: pd.DataFrame, lobby_ids: set[str]) -> pd.DataFrame:
    filtered = matches_df[matches_df["match_id"].isin(lobby_ids)].copy()
    LOGGER.info("Filtered matches.csv down to %s competitive rows", len(filtered))
    return filtered


def select_player_rows(matches_df: pd.DataFrame, lobby_ids: set[str], player_name: str) -> pd.DataFrame:
    player_rows = matches_df[matches_df["player_name"].str.lower() == player_name.lower()].copy()
    LOGGER.info("Filtered competitive rows down to %s rows for %s", len(player_rows), player_name)

    duplicate_ids = sorted(player_rows.loc[player_rows["match_id"].duplicated(), "match_id"].unique())
    if duplicate_ids:
        LOGGER.warning("Duplicate player match IDs found: %s", ", ".join(duplicate_ids))

    missing_ids = sorted(lobby_ids - set(player_rows["match_id"]))
    if missing_ids:
        LOGGER.warning("Missing player rows for lobby IDs: %s", ", ".join(missing_ids))

    extra_ids = sorted(set(player_rows["match_id"]) - lobby_ids)
    if extra_ids:
        LOGGER.warning("Unexpected extra player match IDs: %s", ", ".join(extra_ids))

    player_rows = player_rows.drop_duplicates(subset=["match_id"]).copy()
    player_rows = player_rows.sort_values("game_time").reset_index(drop=True)
    return player_rows


def assign_round_game(player_rows: pd.DataFrame) -> pd.DataFrame:
    assigned = player_rows.copy()
    assigned["round"] = (assigned.index // 5) + 1
    assigned["game"] = (assigned.index % 5) + 1

    if len(assigned) != 25:
        LOGGER.warning("Expected 25 player rows but found %s; round/game assignment still applied.", len(assigned))

    return assigned


def load_round_schedule(path: Path) -> pd.DataFrame:
    dataframe = read_csv(path)
    ensure_columns(dataframe, REQUIRED_SCHEDULE_COLUMNS, path.name)

    dataframe["round"] = pd.to_numeric(dataframe["round"], errors="raise").astype(int)
    dataframe["game"] = pd.to_numeric(dataframe["game"], errors="raise").astype(int)
    dataframe["map"] = dataframe["map"].astype(str).str.strip()

    LOGGER.info("Loaded %s schedule rows from %s", len(dataframe), path)
    return dataframe.loc[:, ["round", "game", "map"]]


def join_schedule(player_rows: pd.DataFrame, schedule_df: pd.DataFrame) -> pd.DataFrame:
    enriched = player_rows.merge(schedule_df, on=["round", "game"], how="left", validate="many_to_one")
    missing_map_rows = int(enriched["map"].isna().sum())
    if missing_map_rows:
        LOGGER.warning("Missing schedule map assignments for %s rows", missing_map_rows)
    return enriched


def load_roster_info(path: Path, player_name: str) -> str:
    dataframe = read_csv(path)
    ensure_columns(dataframe, REQUIRED_ROSTER_COLUMNS, path.name)

    dataframe["pilot_normalized"] = dataframe["Pilot"].map(normalize_text)
    matches = dataframe[dataframe["pilot_normalized"] == normalize_text(player_name)].copy()

    if matches.empty:
        LOGGER.warning("Could not find %s in %s", player_name, path.name)
        return ""

    unique_rows = matches.loc[:, ["Division", "Team"]].drop_duplicates()
    if len(unique_rows) > 1:
        LOGGER.warning("Multiple roster matches found for %s in %s", player_name, path.name)

    roster_row = unique_rows.iloc[0]
    roster_note = f"roster={roster_row['Division']} / {roster_row['Team']}"
    LOGGER.info("Roster match for %s: %s", player_name, roster_note)
    return roster_note


def load_stats_note(path: Path, player_name: str) -> str:
    if not path.exists():
        return ""

    dataframe = read_csv(path)
    if "Pilot Name" not in dataframe.columns:
        return ""

    matches = dataframe[dataframe["Pilot Name"].map(normalize_text) == normalize_text(player_name)]
    if matches.empty:
        return ""

    return "stats_match_found"


def build_team_lookup(matches_df: pd.DataFrame) -> dict[str, dict[str, set[str]]]:
    lookup: dict[str, dict[str, set[str]]] = {}
    for match_id, match_rows in matches_df.groupby("match_id"):
        team_lookup: dict[str, set[str]] = {}
        for team_number, team_rows in match_rows.groupby("team_number"):
            team_lookup[str(team_number)] = {normalize_text(name) for name in team_rows["player_name"]}
        lookup[str(match_id)] = team_lookup
    return lookup


def prepare_matches2(path: Path) -> pd.DataFrame:
    dataframe = read_csv(path)
    ensure_columns(dataframe, REQUIRED_MATCHES2_COLUMNS, path.name)

    prepared = dataframe.loc[:, ["map", "team_a", "team_b"]].copy()
    prepared["validation_row"] = prepared.index + 1
    prepared["map_normalized"] = prepared["map"].map(normalize_map_name)
    prepared["team_a_names"] = prepared["team_a"].map(split_names)
    prepared["team_b_names"] = prepared["team_b"].map(split_names)
    prepared["team_a_normalized"] = prepared["team_a_names"].apply(
        lambda values: {normalize_text(value) for value in values}
    )
    prepared["team_b_normalized"] = prepared["team_b_names"].apply(
        lambda values: {normalize_text(value) for value in values}
    )

    LOGGER.info("Loaded %s validation rows from %s", len(prepared), path)
    return prepared


def build_validation_details(
    row: pd.Series,
    team_lookup: dict[str, dict[str, set[str]]],
    matches2_df: pd.DataFrame,
    player_name: str,
) -> tuple[str, str]:
    player_normalized = normalize_text(player_name)
    match_id = str(row["match_id"])
    assigned_map_normalized = normalize_map_name(row["map"])
    actual_team = team_lookup.get(match_id, {}).get(str(row["team_number"]), set())

    if not actual_team:
        note = "validation=no teammate context found in matches.csv"
        LOGGER.warning("Missing teammate context for match_id=%s", match_id)
        return "ambiguous", note

    candidates: list[dict[str, object]] = []
    for validation_row in matches2_df.itertuples(index=False):
        for side_name, side_column in (("team_a", "team_a_normalized"), ("team_b", "team_b_normalized")):
            validation_team = getattr(validation_row, side_column)
            if player_normalized not in validation_team:
                continue

            overlap = len(actual_team & validation_team)
            if overlap == 0:
                continue

            candidates.append(
                {
                    "validation_row": int(validation_row.validation_row),
                    "side": side_name,
                    "overlap": overlap,
                    "map": validation_row.map,
                    "map_normalized": validation_row.map_normalized,
                }
            )

    if not candidates:
        LOGGER.warning("No matches2 validation candidate found for match_id=%s", match_id)
        return "ambiguous", "validation=no matches2 candidate found"

    max_overlap = max(candidate["overlap"] for candidate in candidates)
    best_candidates = [candidate for candidate in candidates if candidate["overlap"] == max_overlap]
    map_matching_candidates = [
        candidate
        for candidate in best_candidates
        if candidate["map_normalized"] == assigned_map_normalized
    ]

    selected_candidate: dict[str, object] | None = None
    if len(best_candidates) == 1:
        selected_candidate = best_candidates[0]
    elif len(map_matching_candidates) == 1:
        selected_candidate = map_matching_candidates[0]

    if selected_candidate is None:
        candidate_summary = "; ".join(
            f"row {candidate['validation_row']} {candidate['side']} overlap={candidate['overlap']} map={candidate['map']}"
            for candidate in best_candidates[:4]
        )
        LOGGER.warning(
            "Ambiguous validation for match_id=%s with overlap=%s candidates: %s",
            match_id,
            max_overlap,
            candidate_summary,
        )
        return "ambiguous", f"validation=ambiguous; {candidate_summary}"

    notes = (
        f"validation_row={selected_candidate['validation_row']}; "
        f"matches2_side={selected_candidate['side']}; "
        f"teammate_overlap={selected_candidate['overlap']}/6; "
        f"matches2_map={selected_candidate['map']}"
    )

    if selected_candidate["map_normalized"] == assigned_map_normalized:
        return "validated", notes

    LOGGER.warning(
        "Map mismatch for match_id=%s: schedule=%s matches2=%s",
        match_id,
        row["map"],
        selected_candidate["map"],
    )
    return "map_mismatch", notes


def enrich_notes(base_note: str, roster_note: str, stats_note: str) -> str:
    parts = [part for part in [base_note, roster_note, stats_note] if part]
    return " | ".join(parts)


def build_enriched_matches(data_dir: Path, player_name: str) -> pd.DataFrame:
    matches_df = load_matches(data_dir / "matches.csv")
    lobby_ids = load_lobby_ids(data_dir / "comp_lobby_ids.csv")
    comp_matches_df = filter_comp_matches(matches_df, lobby_ids)
    player_rows = select_player_rows(comp_matches_df, lobby_ids, player_name)
    player_rows = assign_round_game(player_rows)

    schedule_df = load_round_schedule(data_dir / "round_schedule.csv")
    player_rows = join_schedule(player_rows, schedule_df)

    matches2_df = prepare_matches2(data_dir / "matches2.csv")
    roster_note = load_roster_info(data_dir / "tournament_rosters - Roster.csv", player_name)
    stats_note = load_stats_note(data_dir / "tournament_rosters - Stats.csv", player_name)
    team_lookup = build_team_lookup(comp_matches_df)

    validation_statuses: list[str] = []
    notes: list[str] = []
    for row in player_rows.itertuples(index=False):
        status, validation_note = build_validation_details(
            pd.Series(row._asdict()),
            team_lookup,
            matches2_df,
            player_name,
        )
        validation_statuses.append(status)
        notes.append(enrich_notes(validation_note, roster_note, stats_note))

    player_rows["validation_status"] = validation_statuses
    player_rows["notes"] = notes
    player_rows["game_time"] = player_rows["game_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return player_rows.loc[:, OUTPUT_COLUMNS]


def write_output(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s enriched rows to %s", len(dataframe), output_path)


def main() -> int:
    configure_logging()
    args = parse_args()

    data_dir = Path(args.data_dir).expanduser()
    output_path = Path(args.output).expanduser()

    enriched_df = build_enriched_matches(data_dir, args.player_name)
    write_output(enriched_df, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
