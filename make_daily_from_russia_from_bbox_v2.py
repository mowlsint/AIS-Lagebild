#!/usr/bin/env python3
r"""
make_daily_from_russia_from_bbox_v2.py

FROM_RUSSIA only exporter.
Update: RU port boxes now include Arctic/North gateways (Murmansk, Arkhangelsk, Varandey, Sabetta, Dudinka)
in addition to Baltic.

Requires sampler_v3 preset northsea_southbaltic_russia_ports to have those boxes in your logs.

Usage:
  python make_daily_from_russia_from_bbox_v2.py --in "logs\\bbox_*.jsonl" --date 2025-12-20 --tz UTC --out exports --lookback-days 14
"""

from __future__ import annotations
import argparse, glob, json, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional

try:
    from zoneinfo import ZoneInfo
    from zoneinfo import ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None  # type: ignore
    ZoneInfoNotFoundError = Exception  # type: ignore

NORTHSEA = (-6.0, 50.0, 10.5, 62.0)
SOUTHBALTIC = (8.5, 53.3, 20.5, 56.2)

RU_PORT_BOXES = {
    "Baltiysk (KO)": (19.70, 54.58, 20.05, 54.75),
    "Kaliningrad (lagoon)": (20.35, 54.62, 20.75, 54.78),
    "St Petersburg": (29.7, 59.7, 30.9, 60.1),
    "Ust-Luga": (28.0, 59.5, 28.8, 59.9),
    "Primorsk": (28.2, 60.2, 28.9, 60.5),
    "Murmansk": (32.6, 68.9, 33.4, 69.2),
    "Arkhangelsk": (40.3, 64.4, 40.9, 64.7),
    "Varandey": (57.6, 68.7, 58.2, 68.9),
    "Sabetta (Yamal LNG)": (71.0, 71.1, 71.7, 71.3),
    "Dudinka": (86.0, 69.3, 86.5, 69.5),
}

def parse_iso_z(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def in_box(lat: float, lon: float, box: Tuple[float,float,float,float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = box
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def in_main_area(lat: float, lon: float) -> bool:
    return in_box(lat, lon, NORTHSEA) or in_box(lat, lon, SOUTHBALTIC)

def ru_port_hit(lat: float, lon: float) -> Optional[str]:
    for name, b in RU_PORT_BOXES.items():
        if in_box(lat, lon, b):
            return name
    return None

def local_day_bounds_to_utc(d: date, tz_name: str):
    if tz_name.upper() == "UTC" or ZoneInfo is None:
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return start, start + timedelta(days=1), "UTC"
    try:
        tz = ZoneInfo(tz_name)
        start_local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), tz_name
    except ZoneInfoNotFoundError:
        local_tz = datetime.now().astimezone().tzinfo
        start_local = datetime(d.year, d.month, d.day, 0, 0, 0).replace(tzinfo=local_tz)
        end_local = start_local + timedelta(days=1)
        print(f"[fromRU] WARNING: ZoneInfo('{tz_name}') not found. Using LOCAL {local_tz}.")
        print("[fromRU] Fix: python -m pip install tzdata")
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), f"LOCAL({local_tz})"

def best_label(mmsi: str, pts_sorted: List[Dict]) -> str:
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    return f"MMSI {mmsi}"

def build_features(pts_sorted: List[Dict], props: Dict) -> List[Dict]:
    coords = [[p["lon"], p["lat"]] for p in pts_sorted]
    last = pts_sorted[-1]
    return [
        {"type":"Feature","properties":{**props,"feature":"track"},"geometry":{"type":"LineString","coordinates":coords}},
        {"type":"Feature","properties":{**props,"feature":"last_position"},"geometry":{"type":"Point","coordinates":[last["lon"], last["lat"]]}},
    ]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_glob", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--tz", default="UTC")
    ap.add_argument("--out", default="exports")
    ap.add_argument("--lookback-days", type=int, default=14)
    args = ap.parse_args()

    d = datetime.strptime(args.date, "%Y-%m-%d").date()
    day_start_utc, day_end_utc, tz_used = local_day_bounds_to_utc(d, args.tz)
    lookback_start_utc = day_start_utc - timedelta(days=max(0, args.lookback_days))

    files = sorted(glob.glob(args.in_glob))
    if not files:
        raise SystemExit(f"No input files match: {args.in_glob}")

    last_ru_seen: Dict[str, str] = {}
    last_ru_port: Dict[str, str] = {}
    day_tracks: Dict[str, List[Dict]] = defaultdict(list)

    scanned = 0
    kept_main_day = 0

    for fp in files:
        with open(fp, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                scanned += 1
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                ts = ev.get("ts_utc")
                if not ts: continue
                try:
                    dt = parse_iso_z(ts)
                except Exception:
                    continue

                mmsi = digits_only(str(ev.get("mmsi","")))
                if not (mmsi.isdigit() and len(mmsi)==9): continue
                lat = ev.get("lat"); lon = ev.get("lon")
                if lat is None or lon is None: continue
                try:
                    lat = float(lat); lon = float(lon)
                except Exception:
                    continue

                if lookback_start_utc <= dt < day_end_utc:
                    port = ru_port_hit(lat, lon)
                    if port:
                        prev = last_ru_seen.get(mmsi)
                        if (prev is None) or (parse_iso_z(prev) < dt):
                            last_ru_seen[mmsi] = ts
                            last_ru_port[mmsi] = port

                if day_start_utc <= dt < day_end_utc and in_main_area(lat, lon):
                    day_tracks[mmsi].append({
                        "ts_utc": ts, "lat": lat, "lon": lon,
                        "imo": digits_only(str(ev.get("imo",""))),
                        "name": str(ev.get("name","")).strip(),
                        "shiptype": str(ev.get("shiptype","")).strip(),
                        "destination": str(ev.get("destination","")).strip(),
                        "eta": str(ev.get("eta","")).strip(),
                    })
                    kept_main_day += 1

    features: List[Dict] = []
    ships_out = 0

    for mmsi, pts in day_tracks.items():
        if mmsi not in last_ru_seen: 
            continue
        if len(pts) < 2:
            continue
        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        first, last = pts_sorted[0], pts_sorted[-1]
        imo = (first.get("imo") or last.get("imo") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()
        label = best_label(mmsi, pts_sorted)
        name = label if not label.startswith("MMSI ") else ""

        props = {
            "label": label,
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"],
            "last_seen_utc": last["ts_utc"],
            "destination": (last.get("destination") or first.get("destination") or "").strip(),
            "eta": (last.get("eta") or first.get("eta") or "").strip(),
            "from_russia": True,
            "last_ru_seen_utc": last_ru_seen.get(mmsi,""),
            "last_ru_port_box": last_ru_port.get(mmsi,""),
            "method": "AIS seen in RU port box (lookback) + track in main area (day)",
        }
        features.extend(build_features(pts_sorted, props))
        ships_out += 1

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    out_fp = outdir / f"lagebild_{args.date}_from_russia.geojson"
    out_fp.write_text(json.dumps({"type":"FeatureCollection","features":features}, ensure_ascii=False), encoding="utf-8")

    tracks = sum(1 for f in features if f.get("properties",{}).get("feature")=="track")
    print(f"[fromRU] date={args.date} tz_used={tz_used} range_utc={day_start_utc.isoformat()}..{day_end_utc.isoformat()}")
    print(f"[fromRU] scanned_lines={scanned} kept_main_day_lines={kept_main_day} unique_ru_mmsi={len(last_ru_seen)} output_ships={ships_out} tracks={tracks}")
    print(f"[fromRU] -> {out_fp}")

if __name__ == "__main__":
    main()
