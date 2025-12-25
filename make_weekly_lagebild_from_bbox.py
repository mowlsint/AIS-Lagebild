#!/usr/bin/env python3
"""
make_weekly_lagebild_from_bbox.py (FIXED)

Fix:
- week_bounds() was broken in the previous draft (TypeError with NoneType).
- Now uses clean ISO-week boundaries:
    start = Monday 00:00 UTC
    end   = next Monday 00:00 UTC

Build weekly GeoJSON from bbox sampler logs.

Inputs:
- logs/bbox_YYYY-MM-DD.jsonl created by aisstream_bbox_sampler.py
- optional: watchlist_shadowfleet.csv (GUR) to tag shadow-fleet vessels by MMSI/IMO

Outputs:
- GeoJSON FeatureCollection with:
  - LineString tracks
  - Point last_position
for vessels that are either:
  - shadow_fleet hit (MMSI or IMO match) OR
  - ru_likely_mid273 (MMSI starts with 273)

Usage (Windows cmd):
  python make_weekly_lagebild_from_bbox.py ^
    --in "logs\\bbox_*.jsonl" ^
    --week 2025-W51 ^
    --shadowfleet watchlist_shadowfleet.csv ^
    --out "exports\\lagebild_2025-W51.geojson"
"""

from __future__ import annotations

import argparse
import glob
import json
import csv
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Set

def parse_iso_z(ts: str) -> datetime:
    # expects e.g. 2025-12-20T12:49:32Z
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def week_bounds(week_str: str) -> Tuple[datetime, datetime]:
    # ISO week: YYYY-Www
    m = re.fullmatch(r"(\d{4})-W(\d{2})", week_str)
    if not m:
        raise ValueError("week must be like 2025-W51")
    year = int(m.group(1))
    week = int(m.group(2))
    start = datetime.fromisocalendar(year, week, 1).replace(tzinfo=timezone.utc)  # Monday 00:00
    end = start + timedelta(days=7)  # next Monday 00:00
    return start, end

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def load_shadowfleet(path: str) -> Tuple[Set[str], Set[str]]:
    """
    Returns (mmsi_set, imo_set) from CSV with columns mmsi/imo.
    """
    mmsi_set: Set[str] = set()
    imo_set: Set[str] = set()
    if not path:
        return mmsi_set, imo_set
    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())
            if mmsi.isdigit() and len(mmsi) == 9:
                mmsi_set.add(mmsi)
            if imo.isdigit() and len(imo) == 7:
                imo_set.add(imo)
    return mmsi_set, imo_set

def is_ru_likely(mmsi: str) -> bool:
    # AIS does not broadcast "flag". MID=273 is a pragmatic proxy indicator for Russia.
    return mmsi.startswith("273")

def build_geojson(tracks: Dict[str, List[Dict]], shadow_mmsi: Set[str], shadow_imo: Set[str]) -> Dict:
    features: List[Dict] = []

    for mmsi, pts in tracks.items():
        if len(pts) < 2:
            continue

        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        coords = [[p["lon"], p["lat"]] for p in pts_sorted]
        first = pts_sorted[0]
        last = pts_sorted[-1]

        imo = first.get("imo") or last.get("imo") or ""
        name = first.get("name") or last.get("name") or ""
        shiptype = first.get("shiptype") or last.get("shiptype") or ""

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if imo else False)
        ru_hit = is_ru_likely(mmsi)

        if not (shadow_hit or ru_hit):
            continue

        props = {
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"],
            "last_seen_utc": last["ts_utc"],
            "shadow_fleet": bool(shadow_hit),
            "ru_likely_mid273": bool(ru_hit),
        }

        features.append({
            "type": "Feature",
            "properties": {**props, "feature": "track"},
            "geometry": {"type": "LineString", "coordinates": coords},
        })

        features.append({
            "type": "Feature",
            "properties": {**props, "feature": "last_position"},
            "geometry": {"type": "Point", "coordinates": [last["lon"], last["lat"]]},
        })

    return {"type": "FeatureCollection", "features": features}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_glob", required=True, help='e.g. "logs\\\\bbox_*.jsonl"')
    ap.add_argument("--week", required=True, help="e.g. 2025-W51")
    ap.add_argument("--shadowfleet", default="", help="CSV from GUR export to tag shadow fleet")
    ap.add_argument("--out", required=True, help='e.g. "exports\\\\lagebild_2025-W51.geojson"')
    args = ap.parse_args()

    start, end = week_bounds(args.week)
    shadow_mmsi, shadow_imo = load_shadowfleet(args.shadowfleet)

    tracks: Dict[str, List[Dict]] = defaultdict(list)

    files = sorted(glob.glob(args.in_glob))
    if not files:
        raise SystemExit(f"No input files match: {args.in_glob}")

    kept = 0
    for fp in files:
        with open(fp, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue

                ts = ev.get("ts_utc")
                if not ts:
                    continue
                try:
                    dt = parse_iso_z(ts)
                except Exception:
                    continue
                if not (start <= dt < end):
                    continue

                mmsi = digits_only(str(ev.get("mmsi", "")))
                if not (mmsi.isdigit() and len(mmsi) == 9):
                    continue

                lat = ev.get("lat"); lon = ev.get("lon")
                if lat is None or lon is None:
                    continue
                try:
                    lat = float(lat); lon = float(lon)
                except Exception:
                    continue

                tracks[mmsi].append({
                    "ts_utc": ts,
                    "lat": lat,
                    "lon": lon,
                    "imo": digits_only(str(ev.get("imo", ""))),
                    "name": str(ev.get("name", "")).strip(),
                    "shiptype": str(ev.get("shiptype", "")).strip(),
                })
                kept += 1

    gj = build_geojson(tracks, shadow_mmsi, shadow_imo)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(gj, ensure_ascii=False), encoding="utf-8")

    # Summary
    print(f"[weekly] week={args.week} range_utc={start.isoformat()}..{end.isoformat()}")
    print(f"[weekly] input_files={len(files)} lines_kept={kept} unique_mmsi={len(tracks)}")
    print(f"[weekly] shadowfleet_mmsi={len(shadow_mmsi)} shadowfleet_imo={len(shadow_imo)}")
    print(f"[weekly] wrote GeoJSON features={len(gj['features'])} -> {out_path}")

if __name__ == "__main__":
    main()
