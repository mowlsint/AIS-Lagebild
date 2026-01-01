#!/usr/bin/env python3
r"""
make_daily_3_geojson_layers_v2.py (REWRITE)

Wie v1, aber:
- schreibt zusätzlich properties.display = "NAME (MMSI)"
  - Wenn name leer ist, nimmt es label.
  - Wenn beides fehlt, "SHIP".
  - Für track-Features wird display leer gelassen (damit uMap-Labels nur bei Punkten stören).
    last_position bekommt display.
- NEU: schreibt properties.links (HTML) + optional vf_url/mst_url für uMap Popups.

Outputs:
- lagebild_YYYY-MM-DD_shadowfleet.geojson
- lagebild_YYYY-MM-DD_ru_mid273.geojson
- lagebild_YYYY-MM-DD_from_russia_ports_excluding_shadow_mid273.geojson
"""

from __future__ import annotations

import argparse
import glob
import json
import csv
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import quote_plus

try:
    from zoneinfo import ZoneInfo
    from zoneinfo import ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None  # type: ignore
    ZoneInfoNotFoundError = Exception  # type: ignore


# -----------------------------
# Areas / Boxes
# -----------------------------
# bbox format: (min_lon, min_lat, max_lon, max_lat)
NORTHSEA = (-6.0, 50.0, 10.5, 62.0)
SOUTHBALTIC = (8.5, 53.3, 20.5, 56.2)

RU_PORT_BOXES: Dict[str, Tuple[float, float, float, float]] = {
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


# -----------------------------
# Parsing / Normalization
# -----------------------------
def parse_iso_z(ts: str) -> datetime:
    """Parse ISO string; allow trailing Z; return UTC aware datetime."""
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


# -----------------------------
# Geometry
# -----------------------------
def in_box(lat: float, lon: float, box: Tuple[float, float, float, float]) -> bool:
    """box=(min_lon,min_lat,max_lon,max_lat)"""
    min_lon, min_lat, max_lon, max_lat = box
    return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)


def in_main_area(lat: float, lon: float) -> bool:
    return in_box(lat, lon, NORTHSEA) or in_box(lat, lon, SOUTHBALTIC)


def ru_port_hit(lat: float, lon: float) -> Optional[str]:
    for name, b in RU_PORT_BOXES.items():
        if in_box(lat, lon, b):
            return name
    return None


# -----------------------------
# Time bounds (local day -> UTC window)
# -----------------------------
def local_day_bounds_to_utc(d: date, tz_name: str):
    """
    Returns (start_utc, end_utc, used_tz_label)
    Where start/end represent the local day's's bounds mapped to UTC.
    """
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
        print(f"[layers] WARNING: ZoneInfo('{tz_name}') not found. Using LOCAL {local_tz}.")
        print("[layers] Fix: python -m pip install tzdata  (then use Europe/Berlin)")
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), f"LOCAL({local_tz})"


# -----------------------------
# Watchlist loader
# -----------------------------
def load_shadowfleet_ids(path: str) -> Tuple[Set[str], Set[str]]:
    shadow_mmsi: Set[str] = set()
    shadow_imo: Set[str] = set()
    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())
            if is_mmsi(mmsi):
                shadow_mmsi.add(mmsi)
            if is_imo(imo):
                shadow_imo.add(imo)
    return shadow_mmsi, shadow_imo


# -----------------------------
# Labels / Display
# -----------------------------
def best_label(mmsi: str, pts_sorted: List[Dict]) -> str:
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    return f"MMSI {mmsi}"


def make_display(name: str, label: str, mmsi: str) -> str:
    base = name.strip() if name else ""
    if not base:
        base = label.strip() if label else ""
    if not base:
        base = "SHIP"
    m = (mmsi or "").strip()
    return f"{base} ({m})" if m else base


# -----------------------------
# Links for uMap Popup
# -----------------------------
def mk_links(name: str, mmsi: str, imo: str) -> Tuple[str, str, str]:
    """
    Returns (links_html, vf_url, mst_url)
    links_html contains clickable anchors; vf_url/mst_url are plain URLs as fallback.
    """
    name_s = (name or "").strip()
    mmsi_s = digits_only(mmsi or "")
    imo_s = digits_only(imo or "")

    vf_url = ""
    mst_url = ""

    # VesselFinder: prefer IMO, else name
    if is_imo(imo_s):
        vf_url = f"https://www.vesselfinder.com/vessels?imo={quote_plus(imo_s)}"
    elif name_s:
        vf_url = f"https://www.vesselfinder.com/vessels?name={quote_plus(name_s)}"

    # MyShipTracking: name + mmsi works often, fallback none
    if name_s and is_mmsi(mmsi_s):
        mst_url = f"https://www.myshiptracking.com/vessels/{quote_plus(name_s)}-mmsi-{quote_plus(mmsi_s)}"

    parts: List[str] = []
    if vf_url:
        parts.append(f"<a href='{vf_url}' target='_blank' rel='noopener'>VesselFinder</a>")
    if mst_url:
        parts.append(f"<a href='{mst_url}' target='_blank' rel='noopener'>MyShipTracking</a>")

    return " | ".join(parts), vf_url, mst_url


# -----------------------------
# Feature builder
# -----------------------------
def build_track_and_last(pts_sorted: List[Dict], props: Dict, display: str) -> List[Dict]:
    coords = [[p["lon"], p["lat"]] for p in pts_sorted]
    last = pts_sorted[-1]

    # For tracks: keep display empty so labels don't clutter.
    p_track = {**props, "feature": "track", "display": ""}

    # For last point: show display
    p_point = {**props, "feature": "last_position", "display": display}

    return [
        {"type": "Feature", "properties": p_track, "geometry": {"type": "LineString", "coordinates": coords}},
        {"type": "Feature", "properties": p_point, "geometry": {"type": "Point", "coordinates": [last["lon"], last["lat"]]}},
    ]


def write_fc(out_fp: Path, features: List[Dict]):
    out_fp.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8"
    )


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_glob", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (lokaler Tag je --tz)")
    ap.add_argument("--tz", default="UTC")
    ap.add_argument("--shadowfleet", default="watchlist_shadowfleet.csv")
    ap.add_argument("--outdir", default="exports")
    ap.add_argument("--lookback-days", type=int, default=14)
    ap.add_argument("--min-track-points", type=int, default=2)
    args = ap.parse_args()

    d = datetime.strptime(args.date, "%Y-%m-%d").date()
    day_start_utc, day_end_utc, used_tz = local_day_bounds_to_utc(d, args.tz)
    lookback_start_utc = day_start_utc - timedelta(days=max(0, args.lookback_days))

    shadow_mmsi, shadow_imo = load_shadowfleet_ids(args.shadowfleet)

    files = sorted(glob.glob(args.in_glob))
    if not files:
        raise SystemExit(f"No input files match: {args.in_glob}")

    # last RU port sightings (lookback window)
    last_ru_seen: Dict[str, str] = {}
    last_ru_port: Dict[str, str] = {}

    # day tracks in main area
    day_tracks: Dict[str, List[Dict]] = defaultdict(list)

    # Parse input jsonl events
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

                mmsi = digits_only(str(ev.get("mmsi", "")))
                if not is_mmsi(mmsi):
                    continue

                lat = ev.get("lat")
                lon = ev.get("lon")
                if lat is None or lon is None:
                    continue
                try:
                    lat = float(lat)
                    lon = float(lon)
                except Exception:
                    continue

                # Lookback: remember last seen in RU port boxes (up to end of day)
                if lookback_start_utc <= dt < day_end_utc:
                    port = ru_port_hit(lat, lon)
                    if port:
                        prev = last_ru_seen.get(mmsi)
                        if (prev is None) or (parse_iso_z(prev) < dt):
                            last_ru_seen[mmsi] = ts
                            last_ru_port[mmsi] = port

                # Day window: build tracks only for main area
                if day_start_utc <= dt < day_end_utc and in_main_area(lat, lon):
                    day_tracks[mmsi].append({
                        "ts_utc": ts,
                        "lat": lat,
                        "lon": lon,
                        "imo": digits_only(str(ev.get("imo", ""))),
                        "name": str(ev.get("name", "")).strip(),
                        "shiptype": str(ev.get("shiptype", "")).strip(),
                        "destination": str(ev.get("destination", "")).strip(),
                        "eta": str(ev.get("eta", "")).strip(),
                    })

    feats_shadow: List[Dict] = []
    feats_mid273: List[Dict] = []
    feats_fromru: List[Dict] = []

    for mmsi, pts in day_tracks.items():
        if len(pts) < args.min_track_points:
            continue

        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        first, last = pts_sorted[0], pts_sorted[-1]

        imo = (first.get("imo") or last.get("imo") or "").strip()
        if not is_imo(imo):
            imo = digits_only(imo)

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if is_imo(imo) else False)

        label = best_label(mmsi, pts_sorted)
        name = (first.get("name") or last.get("name") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()
        dest = (last.get("destination") or first.get("destination") or "").strip()
        display = make_display(name, label, mmsi)

        links_html, vf_url, mst_url = mk_links(name=name, mmsi=mmsi, imo=imo)

        base_props = {
            "label": label,
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"],
            "last_seen_utc": last["ts_utc"],
            "destination": dest,
            "eta": (last.get("eta") or first.get("eta") or "").strip(),

            # NEW: Links for popup
            "links": links_html,
            "vf_url": vf_url,
            "mst_url": mst_url,
        }

        if shadow_hit:
            props = {
                **base_props,
                "layer": "shadowfleet",
                "shadowfleet": True,
                "method": "GUR watchlist match (MMSI/IMO)"
            }
            feats_shadow.extend(build_track_and_last(pts_sorted, props, display))
            continue

        if is_mid273(mmsi):
            props = {
                **base_props,
                "layer": "ru_mid273",
                "ru_mid273": True,
                "method": "MMSI MID=273 excluding shadowfleet"
            }
            feats_mid273.extend(build_track_and_last(pts_sorted, props, display))
            continue

        if mmsi in last_ru_seen:
            props = {
                **base_props,
                "layer": "from_russia_ports",
                "from_russia_ports": True,
                "last_ru_seen_utc": last_ru_seen.get(mmsi, ""),
                "last_ru_port_box": last_ru_port.get(mmsi, ""),
                "method": "Seen in RU port bbox (lookback) + track in main area (day), excluding shadowfleet & MID273",
            }
            feats_fromru.extend(build_track_and_last(pts_sorted, props, display))

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    fp_shadow = outdir / f"lagebild_{args.date}_shadowfleet.geojson"
    fp_mid = outdir / f"lagebild_{args.date}_ru_mid273.geojson"
    fp_fromru = outdir / f"lagebild_{args.date}_from_russia_ports_excluding_shadow_mid273.geojson"

    write_fc(fp_shadow, feats_shadow)
    write_fc(fp_mid, feats_mid273)
    write_fc(fp_fromru, feats_fromru)

    print(f"[layers] tz={used_tz} day_start_utc={day_start_utc.isoformat()} day_end_utc={day_end_utc.isoformat()}")
    print(f"[layers] wrote:\n  {fp_shadow}\n  {fp_mid}\n  {fp_fromru}")


if __name__ == "__main__":
    main()
