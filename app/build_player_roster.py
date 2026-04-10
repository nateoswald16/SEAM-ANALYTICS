#!/usr/bin/env python3
"""
build_player_roster.py

Fetches all 30 MLB team rosters (40-man) from the MLB Stats API,
writes a players.csv index, and pre-caches headshot images.

Usage:
    python build_player_roster.py          # full refresh
    python build_player_roster.py --quick  # CSV only, skip headshots
"""

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import _app_paths

# ── Paths ────────────────────────────────────────────────────────────
PLAYERS_CSV = os.path.join(_app_paths.ASSETS_DIR, "players.csv")
HEADSHOT_DIR = _app_paths.HEADSHOT_CACHE_DIR
os.makedirs(HEADSHOT_DIR, exist_ok=True)

# ── HTTP session ─────────────────────────────────────────────────────
_session = requests.Session()
_retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
_session.mount("https://", HTTPAdapter(max_retries=_retry))

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1&season={season}"
ROSTER_URL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=40Man&season={season}"
PERSON_URL = "https://statsapi.mlb.com/api/v1/people/{pid}?hydrate=currentTeam"
HEADSHOT_URL = (
    "https://img.mlbstatic.com/mlb-photos/image/upload/"
    "d_people:generic:headshot:67:current.png/"
    "w_213,q_auto:best/v1/people/{pid}/headshot/67/current"
)

CSV_FIELDS = [
    "player_id", "name_full", "name_last", "name_first",
    "team", "team_id", "jersey_number",
    "position", "position_type",
    "bats", "throws",
    "age", "height", "weight",
    "mlb_debut", "birth_country",
    "headshot_url",
]


def fetch_teams(season: int) -> list[dict]:
    """Return list of active MLB teams for the given season."""
    resp = _session.get(TEAMS_URL.format(season=season), timeout=15)
    resp.raise_for_status()
    teams = resp.json().get("teams", [])
    # Filter to active MLB teams only
    return [t for t in teams if t.get("sport", {}).get("id") == 1 and t.get("active")]


def fetch_roster(team_id: int, season: int) -> list[dict]:
    """Return raw roster entries for a team."""
    resp = _session.get(ROSTER_URL.format(team_id=team_id, season=season), timeout=15)
    resp.raise_for_status()
    return resp.json().get("roster", [])


def fetch_person(pid: int) -> dict:
    """Fetch full person details from MLB API."""
    resp = _session.get(PERSON_URL.format(pid=pid), timeout=15)
    resp.raise_for_status()
    people = resp.json().get("people", [])
    return people[0] if people else {}


def build_roster_csv(season: int) -> list[dict]:
    """Fetch all rosters and return a list of player dicts."""
    teams = fetch_teams(season)
    print(f"Found {len(teams)} active MLB teams for {season}")

    players = {}  # keyed by player_id to dedupe
    for t in teams:
        tid = t["id"]
        abbr = t.get("abbreviation", "???")
        try:
            roster = fetch_roster(tid, season)
        except Exception as e:
            print(f"  Warning: failed to fetch roster for {abbr}: {e}")
            continue
        for entry in roster:
            person = entry.get("person", {})
            pid = person.get("id")
            if not pid or pid in players:
                continue
            pos = entry.get("position", {})
            players[pid] = {
                "player_id": pid,
                "name_full": person.get("fullName", ""),
                "name_last": person.get("lastName", ""),
                "name_first": person.get("firstName", ""),
                "team": abbr,
                "team_id": tid,
                "jersey_number": entry.get("jerseyNumber", ""),
                "position": pos.get("abbreviation", ""),
                "position_type": pos.get("type", ""),
                "bats": "",
                "throws": "",
                "age": "",
                "height": "",
                "weight": "",
                "mlb_debut": "",
                "birth_country": "",
                "headshot_url": HEADSHOT_URL.format(pid=pid),
            }
        print(f"  {abbr}: {len(roster)} players")

    # Enrich with person details (bats/throws/age/etc) in parallel
    print(f"\nEnriching {len(players)} players with bio details...")
    pids = list(players.keys())

    def _enrich(pid):
        try:
            p = fetch_person(pid)
            return pid, p
        except Exception:
            return pid, {}

    done = 0
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_enrich, pid): pid for pid in pids}
        for fut in as_completed(futures):
            pid, data = fut.result()
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(pids)} enriched...")
            if not data:
                continue
            row = players[pid]
            row["name_first"] = data.get("firstName", "")
            row["name_last"] = data.get("lastName", "")
            row["bats"] = data.get("batSide", {}).get("code", "")
            row["throws"] = data.get("pitchHand", {}).get("code", "")
            row["age"] = data.get("currentAge", "")
            row["height"] = data.get("height", "")
            row["weight"] = data.get("weight", "")
            row["mlb_debut"] = data.get("mlbDebutDate", "")
            row["birth_country"] = data.get("birthCountry", "")
            # Update team from currentTeam if available
            ct = data.get("currentTeam", {})
            if ct.get("abbreviation"):
                row["team"] = ct["abbreviation"]
                row["team_id"] = ct.get("id", row["team_id"])

    result = sorted(players.values(), key=lambda r: (r["team"], r["name_last"]))

    # Write CSV
    with open(PLAYERS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(result)
    print(f"\nWrote {len(result)} players to {PLAYERS_CSV}")
    return result


def download_headshots(players: list[dict], force: bool = False):
    """Download headshot PNGs for all players in parallel."""
    to_download = []
    for p in players:
        pid = p["player_id"]
        path = os.path.join(HEADSHOT_DIR, f"{pid}.png")
        if not force and os.path.exists(path):
            continue
        to_download.append((pid, p["headshot_url"], path))

    if not to_download:
        print("All headshots already cached.")
        return

    print(f"Downloading {len(to_download)} headshots...")

    def _fetch(args):
        pid, url, path = args
        try:
            resp = _session.get(url, timeout=10)
            if resp.status_code == 200 and len(resp.content) > 500:
                with open(path, "wb") as f:
                    f.write(resp.content)
                return pid, True
        except Exception:
            pass
        return pid, False

    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_fetch, args): args[0] for args in to_download}
        for fut in as_completed(futures):
            pid, success = fut.result()
            if success:
                ok += 1
            else:
                fail += 1
            total = ok + fail
            if total % 100 == 0:
                print(f"  {total}/{len(to_download)} downloaded ({ok} ok, {fail} failed)")

    print(f"Headshots done: {ok} downloaded, {fail} failed, "
          f"{len(players) - len(to_download)} already cached")


def main():
    parser = argparse.ArgumentParser(description="Build MLB player roster CSV and cache headshots")
    parser.add_argument("--season", type=int, default=2026, help="MLB season year")
    parser.add_argument("--quick", action="store_true", help="CSV only, skip headshot downloads")
    parser.add_argument("--force-headshots", action="store_true", help="Re-download all headshots")
    args = parser.parse_args()

    t0 = time.time()
    players = build_roster_csv(args.season)
    if not args.quick:
        download_headshots(players, force=args.force_headshots)
    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
