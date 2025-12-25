#!/usr/bin/env python3
"""
make_weekly_lagebild_from_bbox_v2.py

Adds uMap-friendly labeling:
- Always sets properties.label (fallback to "MMSI <mmsi>")
- Enriches name from:
  1) bbox logs (name/callsign if present)
  2) GUR shadowfleet list (name by MMSI/IMO) if matched

Outputs:
- Track LineString + last_position Point per vessel (shadow_fleet or ru_likely_mid273)

Usage:
  python make_weekly_lagebild_from_bbox_v2.py --in "logs\\bbox_*.jsonl" --week 2025-W51 --shadowfleet watchlist_shadowfleet.csv --out "exports\\lagebild_2025-W51.geojson"
"""

from __future__ import annotations

import argparse, glob, json, csv, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Set

def parse_iso_z(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def week_bounds(week_str: str) -> Tuple[datetime, datetime]:
    m = re.fullmatch(r"(\d{4})-W(\d{2})", week_str)
    if not m:
        raise ValueError("week must be like 2025-W51")
    year = int(m.group(1)); week = int(m.group(2))
    start = datetime.fromisocalendar(year, week, 1).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    return start, end

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def load_shadowfleet_maps(path: str) -> Tuple[Set[str], Set[str], Dict[str, str], Dict[str, str]]:
    """
    Returns:
      shadow_mmsi_set, shadow_imo_set, name_by_mmsi, name_by_imo
    from a CSV with columns: name, mmsi, imo (like watchlist_shadowfleet.csv).
    """
    shadow_mmsi: Set[str] = set()
    shadow_imo: Set[str] = set()
    name_by_mmsi: Dict[str, str] = {}
    name_by_imo: Dict[str, str] = {}
    if not path:
        return shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo

    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())
            name = (row.get("name") or "").strip().strip('"')
            if mmsi.isdigit() and len(mmsi) == 9:
                shadow_mmsi.add(mmsi)
                if name and mmsi not in name_by_mmsi:
                    name_by_mmsi[mmsi] = name
            if imo.isdigit() and len(imo) == 7:
                shadow_imo.add(imo)
                if name and imo not in name_by_imo:
                    name_by_imo[imo] = name
    return shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo

def is_ru_likely(mmsi: str) -> bool:
    return mmsi.startswith("273")  # Russia MID (proxy)

def best_name(mmsi: str, imo: str, pts_sorted: List[Dict], name_by_mmsi: Dict[str,str], name_by_imo: Dict[str,str]) -> str:
    # 1) bbox log name
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    # 2) bbox callsign (as last resort)
    for p in (pts_sorted[0], pts_sorted[-1]):
        cs = (p.get("callsign") or "").strip()
        if cs:
            return cs
    # 3) shadowfleet list name if matched
    if mmsi in name_by_mmsi and name_by_mmsi[mmsi]:
        return name_by_mmsi[mmsi]
    if imo and imo in name_by_imo and name_by_imo[imo]:
        return name_by_imo[imo]
    return ""  # let label fallback handle

def build_geojson(tracks: Dict[str, List[Dict]], shadow_mmsi: Set[str], shadow_imo: Set[str],
                  name_by_mmsi: Dict[str,str], name_by_imo: Dict[str,str]) -> Dict:
    features: List[Dict] = []

    for mmsi, pts in tracks.items():
        if len(pts) < 2:
            continue

        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        coords = [[p["lon"], p["lat"]] for p in pts_sorted]
        first = pts_sorted[0]
        last = pts_sorted[-1]

        imo = (first.get("imo") or last.get("imo") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if imo else False)
        ru_hit = is_ru_likely(mmsi)

        if not (shadow_hit or ru_hit):
            continue

        name = best_name(mmsi, imo, pts_sorted, name_by_mmsi, name_by_imo)
        label = name if name else f"MMSI {mmsi}"

        props = {
            "label": label,          # <-- uMap-friendly
            "mmsi": mmsi,
            "imo": imo,
            "name": name,            # may be empty, label never is
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
    shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo = load_shadowfleet_maps(args.shadowfleet)

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

                mmsi = digits_only(str(ev.get("mmsi","")))
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
                    "imo": digits_only(str(ev.get("imo",""))),
                    "name": str(ev.get("name","")).strip(),
                    "callsign": str(ev.get("callsign","")).strip(),
                    "shiptype": str(ev.get("shiptype","")).strip(),
                })
                kept += 1

    gj = build_geojson(tracks, shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(gj, ensure_ascii=False), encoding="utf-8")

    print(f"[weekly] week={args.week} range_utc={start.isoformat()}..{end.isoformat()}")
    print(f"[weekly] input_files={len(files)} lines_kept={kept} unique_mmsi={len(tracks)}")
    print(f"[weekly] wrote GeoJSON features={len(gj['features'])} -> {out_path}")

if __name__ == "__main__":
    main()
