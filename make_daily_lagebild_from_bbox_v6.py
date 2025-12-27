#!/usr/bin/env python3
r"""
make_daily_lagebild_from_bbox_v6.py (UPDATED)

Daily exporter with 3 layers:
1) shadowfleet (watchlist match: MMSI/IMO + NAME fallback)
2) russia_routes (to/from Russia, heuristic)
3) ru_mid273 (MMSI MID=273 excluding 1+2)

Run:
  python make_daily_lagebild_from_bbox_v6.py --in "logs_v4\bbox_*.jsonl" --date 2025-12-20 --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 10
"""

from __future__ import annotations

import argparse, glob, json, csv, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple, Set

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

    "St Petersburg": (29.70, 59.70, 30.90, 60.10),
    "Ust-Luga": (28.00, 59.50, 28.80, 59.90),
    "Primorsk": (28.20, 60.20, 28.90, 60.50),

    "Murmansk": (32.60, 68.90, 33.40, 69.20),
    "Arkhangelsk": (40.30, 64.40, 40.90, 64.70),
    "Varandey": (57.60, 68.70, 58.20, 68.90),
    "Sabetta (Yamal LNG)": (71.00, 71.10, 71.70, 71.30),
    "Dudinka": (86.00, 69.30, 86.50, 69.50),
}

RU_DEST_RE = re.compile(
    r"\b(RU|RUSSIA|RUS|PRIMORSK|UST[- ]?LUGA|ST\s*PETERSBURG|PETERSBURG|KALININGRAD|BALTIYSK|MURMANSK|ARKHANGELSK|SABETTA|DUDINKA)\b",
    re.I
)

NON_ALNUM = re.compile(r"[^A-Z0-9]+", re.I)
WS_RE = re.compile(r"\s+")

def parse_iso_z(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def is_mmsi(s: str) -> bool:
    return s.isdigit() and len(s) == 9

def is_imo(s: str) -> bool:
    return s.isdigit() and len(s) == 7

def is_mid273(mmsi: str) -> bool:
    return mmsi.startswith("273")

def in_box(lat: float, lon: float, box: Tuple[float,float,float,float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = box
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)

def in_main_area(lat: float, lon: float) -> bool:
    return in_box(lat, lon, NORTHSEA) or in_box(lat, lon, SOUTHBALTIC)

def ru_port_hit(lat: float, lon: float) -> bool:
    for _, b in RU_PORT_BOXES.items():
        if in_box(lat, lon, b):
            return True
    return False

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
        print(f"[daily] WARNING: ZoneInfo('{tz_name}') not found. Using LOCAL {local_tz}.")
        print("[daily] Fix: python -m pip install tzdata")
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), f"LOCAL({local_tz})"

def norm_name(s: str) -> str:
    """
    Normalize vessel names for matching:
    - uppercase
    - remove punctuation
    - collapse whitespace
    """
    s = (s or "").strip().upper()
    if not s:
        return ""
    s = NON_ALNUM.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s

def load_shadowfleet_ids(path: str) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Reads a CSV and extracts:
      - MMSI (9 digits) from 'mmsi'
      - IMO (7 digits) from 'imo'
      - NAME fallback from 'name' (and a few common variants)
    Returns sets (shadow_mmsi, shadow_imo, shadow_names_norm)
    """
    shadow_mmsi: Set[str] = set()
    shadow_imo: Set[str] = set()
    shadow_names: Set[str] = set()

    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())

            # Try common name columns
            name = (
                (row.get("name") or "")
                or (row.get("vessel_name") or "")
                or (row.get("ship_name") or "")
                or (row.get("Vessel name") or "")
            ).strip()

            if is_mmsi(mmsi):
                shadow_mmsi.add(mmsi)
            if is_imo(imo):
                shadow_imo.add(imo)
            nn = norm_name(name)
            if nn:
                shadow_names.add(nn)

    return shadow_mmsi, shadow_imo, shadow_names

def best_label(mmsi: str, pts_sorted: List[Dict]) -> str:
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    return f"MMSI {mmsi}"

def build_track_and_last(pts_sorted: List[Dict], props: Dict) -> List[Dict]:
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
    ap.add_argument("--shadowfleet", default="watchlist_shadowfleet.csv")
    ap.add_argument("--outdir", default="exports")
    ap.add_argument("--lookback-days", type=int, default=14)
    args = ap.parse_args()

    d = datetime.strptime(args.date, "%Y-%m-%d").date()
    day_start_utc, day_end_utc, tz_used = local_day_bounds_to_utc(d, args.tz)
    lookback_start_utc = day_start_utc - timedelta(days=max(0, args.lookback_days))

    shadow_mmsi, shadow_imo, shadow_names = load_shadowfleet_ids(args.shadowfleet)

    files = sorted(glob.glob(args.in_glob))
    if not files:
        raise SystemExit(f"No input files match: {args.in_glob}")

    day_tracks: Dict[str, List[Dict]] = defaultdict(list)
    from_russia_seen: Set[str] = set()

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

                mmsi = digits_only(str(ev.get("mmsi","")))
                if not is_mmsi(mmsi):
                    continue

                lat = ev.get("lat"); lon = ev.get("lon")
                if lat is None or lon is None:
                    continue
                try:
                    lat = float(lat); lon = float(lon)
                except Exception:
                    continue

                # Lookback for "from Russia" based on RU port boxes
                if lookback_start_utc <= dt < day_end_utc and ru_port_hit(lat, lon):
                    from_russia_seen.add(mmsi)

                # Track points for the selected day within main area
                if day_start_utc <= dt < day_end_utc and in_main_area(lat, lon):
                    day_tracks[mmsi].append({
                        "ts_utc": ts, "lat": lat, "lon": lon,
                        "imo": digits_only(str(ev.get("imo",""))),
                        "name": str(ev.get("name","")).strip(),
                        "shiptype": str(ev.get("shiptype","")).strip(),
                        "destination": str(ev.get("destination","")).strip(),
                        "eta": str(ev.get("eta") or "").strip(),
                    })

    features: List[Dict] = []
    counts = {"shadowfleet":0, "russia_routes":0, "ru_mid273":0}

    for mmsi, pts in day_tracks.items():
        if len(pts) < 2:
            continue

        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        first, last = pts_sorted[0], pts_sorted[-1]
        imo = (first.get("imo") or last.get("imo") or "").strip()
        dest = (last.get("destination") or first.get("destination") or "").strip()

        vessel_name = (first.get("name") or last.get("name") or "").strip()
        vessel_name_norm = norm_name(vessel_name)

        # Shadowfleet match: MMSI/IMO primary + NAME fallback
        is_shadow = (
            (mmsi in shadow_mmsi)
            or (imo in shadow_imo if is_imo(imo) else False)
            or (vessel_name_norm in shadow_names if vessel_name_norm else False)
        )

        # Decide layer with strict priority: shadowfleet > russia_routes > ru_mid273
        layer = None
        if is_shadow:
            layer = "shadowfleet"
        else:
            to_ru = bool(dest and RU_DEST_RE.search(dest))
            from_ru = (mmsi in from_russia_seen)
            if to_ru or from_ru:
                layer = "russia_routes"
            elif is_mid273(mmsi):
                layer = "ru_mid273"

        if layer is None:
            continue

        counts[layer] += 1

        base_label = best_label(mmsi, pts_sorted)

        # Label prefix priority: shadowfleet > russia_routes
        if layer == "shadowfleet":
            label = f"🕶️ Schattenflotte – {base_label}"
        elif layer == "russia_routes":
            label = f"🇷🇺 Aus Russland – {base_label}"
        else:
            label = base_label

        # Keep "name" as pure vessel name (no prefix)
        name = base_label if not base_label.startswith("MMSI ") else ""
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()

        props = {
            "layer": layer,
            "label": label,
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"],
            "last_seen_utc": last["ts_utc"],
            "destination": dest,
            "eta": (last.get("eta") or first.get("eta") or "").strip(),
            "from_russia_lookback": (mmsi in from_russia_seen),
            "to_russia_destination_match": bool(dest and RU_DEST_RE.search(dest)),
            "shadowfleet_match": bool(is_shadow),
            "shadowfleet_match_via": (
                "mmsi" if (mmsi in shadow_mmsi)
                else "imo" if (is_imo(imo) and imo in shadow_imo)
                else "name" if (vessel_name_norm and vessel_name_norm in shadow_names)
                else ""
            ),
            "vessel_name_norm": vessel_name_norm,
        }

        features.extend(build_track_and_last(pts_sorted, props))

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    out_fp = outdir / f"lagebild_{args.date}_3layers.geojson"
    out_fp.write_text(json.dumps({"type":"FeatureCollection","features":features}, ensure_ascii=False), encoding="utf-8")

    tracks = sum(1 for f in features if f.get("properties",{}).get("feature")=="track")
    print(f"[daily] wrote tracks={tracks} counts={counts} tz={tz_used} lookback_days={args.lookback_days} -> {out_fp}")

if __name__ == "__main__":
    main()
