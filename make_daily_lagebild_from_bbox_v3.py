#!/usr/bin/env python3
"""
make_daily_lagebild_from_bbox_v3.py

Daily GeoJSON exporter for uMap with TWO layers:
- Shadow Fleet layer (from GUR list, MMSI/IMO match)
- "Russian flag" layer as a pragmatic AIS proxy: MMSI MID == 273
  (AIS does not broadcast flag; MID=273 is an indicator, not proof.)

Precedence rule:
- If a vessel matches BOTH (shadow_fleet AND MID273), it goes to Shadow Fleet layer only.

Outputs:
- exports/lagebild_<DATE>_shadowfleet.geojson
- exports/lagebild_<DATE>_ru_mid273.geojson

Each vessel produces:
- a LineString track
- a Point last_position
Properties include:
- label (always non-empty; fallback "MMSI <mmsi>")
- shadow_fleet (bool)
- ru_likely_mid273 (bool)
- sanctions_regimes (from GUR page if matched)
- sanctions_source (always "GUR war-sanctions site" when matched)
- gur_url (detail page when matched)

Usage (Windows cmd):
  python make_daily_lagebild_from_bbox_v3.py ^
    --in "logs\\bbox_*.jsonl" ^
    --date 2025-12-20 ^
    --shadowfleet watchlist_shadowfleet.csv ^
    --outdir exports

Timezone:
- Daily bounds use Europe/Berlin by default (midnight-to-midnight local), then converted to UTC.
- Change with --tz "Europe/Berlin" (or "UTC").
"""

from __future__ import annotations

import argparse, glob, json, csv, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

def parse_iso_z(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def load_shadowfleet_maps(path: str) -> Tuple[Set[str], Set[str], Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str]]:
    """
    Returns:
      shadow_mmsi_set, shadow_imo_set,
      name_by_mmsi, name_by_imo,
      sanctions_by_mmsi, sanctions_by_imo,
      url_by_mmsi, url_by_imo
    from a CSV with columns: name, mmsi, imo, sanctions, note (like watchlist_shadowfleet.csv).
    """
    shadow_mmsi: Set[str] = set()
    shadow_imo: Set[str] = set()
    name_by_mmsi: Dict[str, str] = {}
    name_by_imo: Dict[str, str] = {}
    sanctions_by_mmsi: Dict[str, str] = {}
    sanctions_by_imo: Dict[str, str] = {}
    url_by_mmsi: Dict[str, str] = {}
    url_by_imo: Dict[str, str] = {}

    if not path:
        return shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo, sanctions_by_mmsi, sanctions_by_imo, url_by_mmsi, url_by_imo

    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())
            name = (row.get("name") or "").strip().strip('"')
            sanctions = (row.get("sanctions") or "").strip()
            url = (row.get("note") or "").strip()

            if mmsi.isdigit() and len(mmsi) == 9:
                shadow_mmsi.add(mmsi)
                if name and mmsi not in name_by_mmsi:
                    name_by_mmsi[mmsi] = name
                if sanctions and mmsi not in sanctions_by_mmsi:
                    sanctions_by_mmsi[mmsi] = sanctions
                if url and mmsi not in url_by_mmsi:
                    url_by_mmsi[mmsi] = url

            if imo.isdigit() and len(imo) == 7:
                shadow_imo.add(imo)
                if name and imo not in name_by_imo:
                    name_by_imo[imo] = name
                if sanctions and imo not in sanctions_by_imo:
                    sanctions_by_imo[imo] = sanctions
                if url and imo not in url_by_imo:
                    url_by_imo[imo] = url

    return shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo, sanctions_by_mmsi, sanctions_by_imo, url_by_mmsi, url_by_imo

def is_ru_mid273(mmsi: str) -> bool:
    return mmsi.startswith("273")

def best_name(mmsi: str, imo: str, pts_sorted: List[Dict], name_by_mmsi: Dict[str,str], name_by_imo: Dict[str,str]) -> str:
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    for p in (pts_sorted[0], pts_sorted[-1]):
        cs = (p.get("callsign") or "").strip()
        if cs:
            return cs
    if mmsi in name_by_mmsi and name_by_mmsi[mmsi]:
        return name_by_mmsi[mmsi]
    if imo and imo in name_by_imo and name_by_imo[imo]:
        return name_by_imo[imo]
    return ""

def local_day_bounds_to_utc(d: date, tz_name: str) -> Tuple[datetime, datetime]:
    if tz_name.upper() == "UTC" or ZoneInfo is None:
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return start, end
    tz = ZoneInfo(tz_name)
    start_local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def build_features_for_tracks(
    tracks: Dict[str, List[Dict]],
    shadow_mmsi: Set[str],
    shadow_imo: Set[str],
    name_by_mmsi: Dict[str,str],
    name_by_imo: Dict[str,str],
    sanctions_by_mmsi: Dict[str,str],
    sanctions_by_imo: Dict[str,str],
    url_by_mmsi: Dict[str,str],
    url_by_imo: Dict[str,str],
    mode: str,  # "shadowfleet" or "ru"
) -> List[Dict]:
    features: List[Dict] = []
    for mmsi, pts in tracks.items():
        if len(pts) < 2:
            continue

        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        first = pts_sorted[0]
        last = pts_sorted[-1]
        imo = (first.get("imo") or last.get("imo") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if imo else False)
        ru_hit = is_ru_mid273(mmsi)

        # Precedence: shadow wins
        if mode == "shadowfleet" and not shadow_hit:
            continue
        if mode == "ru" and not (ru_hit and not shadow_hit):
            continue

        name = best_name(mmsi, imo, pts_sorted, name_by_mmsi, name_by_imo)
        label = name if name else f"MMSI {mmsi}"

        sanctions = ""
        gur_url = ""
        if shadow_hit:
            sanctions = sanctions_by_mmsi.get(mmsi) or (sanctions_by_imo.get(imo) if imo else "") or ""
            gur_url = url_by_mmsi.get(mmsi) or (url_by_imo.get(imo) if imo else "") or ""

        props_common = {
            "label": label,
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"],
            "last_seen_utc": last["ts_utc"],
            "shadow_fleet": bool(shadow_hit),
            "ru_likely_mid273": bool(ru_hit),
            "sanctions_regimes": sanctions,
            "sanctions_source": "GUR war-sanctions site" if shadow_hit else "",
            "gur_url": gur_url,
        }

        coords = [[p["lon"], p["lat"]] for p in pts_sorted]

        features.append({
            "type": "Feature",
            "properties": {**props_common, "feature": "track", "layer": mode},
            "geometry": {"type": "LineString", "coordinates": coords},
        })

        features.append({
            "type": "Feature",
            "properties": {**props_common, "feature": "last_position", "layer": mode},
            "geometry": {"type": "Point", "coordinates": [last["lon"], last["lat"]]},
        })

    return features

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_glob", required=True, help='e.g. "logs\\\\bbox_*.jsonl"')
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (local day, default tz Europe/Berlin)")
    ap.add_argument("--tz", default="Europe/Berlin", help='IANA tz name, e.g. "Europe/Berlin" or "UTC"')
    ap.add_argument("--shadowfleet", default="", help="CSV from GUR export (watchlist_shadowfleet.csv)")
    ap.add_argument("--outdir", default="exports")
    args = ap.parse_args()

    d = datetime.strptime(args.date, "%Y-%m-%d").date()
    start_utc, end_utc = local_day_bounds_to_utc(d, args.tz)

    (shadow_mmsi, shadow_imo,
     name_by_mmsi, name_by_imo,
     sanctions_by_mmsi, sanctions_by_imo,
     url_by_mmsi, url_by_imo) = load_shadowfleet_maps(args.shadowfleet)

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
                if not (start_utc <= dt < end_utc):
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

    # Build two layer outputs
    shadow_features = build_features_for_tracks(
        tracks, shadow_mmsi, shadow_imo,
        name_by_mmsi, name_by_imo,
        sanctions_by_mmsi, sanctions_by_imo,
        url_by_mmsi, url_by_imo,
        mode="shadowfleet",
    )
    ru_features = build_features_for_tracks(
        tracks, shadow_mmsi, shadow_imo,
        name_by_mmsi, name_by_imo,
        sanctions_by_mmsi, sanctions_by_imo,
        url_by_mmsi, url_by_imo,
        mode="ru",
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    out_shadow = outdir / f"lagebild_{args.date}_shadowfleet.geojson"
    out_ru = outdir / f"lagebild_{args.date}_ru_mid273.geojson"

    out_shadow.write_text(json.dumps({"type":"FeatureCollection","features": shadow_features}, ensure_ascii=False), encoding="utf-8")
    out_ru.write_text(json.dumps({"type":"FeatureCollection","features": ru_features}, ensure_ascii=False), encoding="utf-8")

    # Summary
    def count_tracks(features: List[Dict]) -> int:
        return sum(1 for f in features if f.get("properties",{}).get("feature") == "track")
    print(f"[daily] date={args.date} tz={args.tz} range_utc={start_utc.isoformat()}..{end_utc.isoformat()}")
    print(f"[daily] input_files={len(files)} lines_kept={kept} unique_mmsi={len(tracks)}")
    print(f"[daily] shadowfleet: tracks={count_tracks(shadow_features)} features={len(shadow_features)} -> {out_shadow}")
    print(f"[daily] ru_mid273 (excluding shadow): tracks={count_tracks(ru_features)} features={len(ru_features)} -> {out_ru}")
    print("[daily] sanctions list used for shadow_fleet tagging: GUR war-sanctions shadow-fleet site (sanctions_regimes field).")

if __name__ == "__main__":
    main()
