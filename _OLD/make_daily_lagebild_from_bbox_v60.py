#!/usr/bin/env python3
r"""
make_daily_lagebild_from_bbox_v6.py

Daily exporter with 3 layers:
1) shadowfleet (GUR list match)
2) russia_routes (to/from Russia; includes Baltic + Arctic/North gateways)
3) ru_mid273 (excluding 1+2)

Precedence: shadowfleet > russia_routes > ru_mid273

Notes:
- "to Russia" uses Destination free-text regex (unreliable).
- "from Russia" uses recent presence in RU port boxes (lookback-days).
  Requires sampler_v3 preset northsea_southbaltic_russia_ports.

Usage:
  python make_daily_lagebild_from_bbox_v6.py --in "logs\\bbox_*.jsonl" --date 2025-12-20 --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 14
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
    "Kaliningrad/Baltiysk": (19.1, 54.4, 20.6, 54.9),
    "St Petersburg": (29.7, 59.7, 30.9, 60.1),
    "Ust-Luga": (28.0, 59.5, 28.8, 59.9),
    "Primorsk": (28.2, 60.2, 28.9, 60.5),
    "Murmansk": (32.6, 68.9, 33.4, 69.2),
    "Arkhangelsk": (40.3, 64.4, 40.9, 64.7),
    "Varandey": (57.6, 68.7, 58.2, 68.9),
    "Sabetta (Yamal LNG)": (71.0, 71.1, 71.7, 71.3),
    "Dudinka": (86.0, 69.3, 86.5, 69.5),
}

RU_DEST_PAT = re.compile(
    r"(?:\bRU\b|RUSSIA|RUS\b|KALININGRAD|BALTIYSK|UST[\s\-]?LUGA|PRIMORSK|ST[\s\-]?PETERSBURG|PETERSBURG|VYSOTSK|VYBORG|"
    r"S(?:A|O)?BETTA|SABETTA|YAMAL|MURMANSK|ARKHANGELSK|DUDINKA|VARANDEY|NOVAYA\s+ZEMLYA|KARA\s+SEA|BARENTS)",
    re.IGNORECASE
)

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

def in_any_ru_port(lat: float, lon: float) -> bool:
    return any(in_box(lat, lon, b) for b in RU_PORT_BOXES.values())

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
        print("[daily] Fix: python -m pip install tzdata  (then you can use Europe/Berlin)")
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), f"LOCAL({local_tz})"

def load_shadowfleet_maps(path: str):
    shadow_mmsi, shadow_imo = set(), set()
    name_by_mmsi, name_by_imo = {}, {}
    sanctions_by_mmsi, sanctions_by_imo = {}, {}
    url_by_mmsi, url_by_imo = {}, {}
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
                if name and mmsi not in name_by_mmsi: name_by_mmsi[mmsi] = name
                if sanctions and mmsi not in sanctions_by_mmsi: sanctions_by_mmsi[mmsi] = sanctions
                if url and mmsi not in url_by_mmsi: url_by_mmsi[mmsi] = url
            if imo.isdigit() and len(imo) == 7:
                shadow_imo.add(imo)
                if name and imo not in name_by_imo: name_by_imo[imo] = name
                if sanctions and imo not in sanctions_by_imo: sanctions_by_imo[imo] = sanctions
                if url and imo not in url_by_imo: url_by_imo[imo] = url
    return shadow_mmsi, shadow_imo, name_by_mmsi, name_by_imo, sanctions_by_mmsi, sanctions_by_imo, url_by_mmsi, url_by_imo

def is_ru_mid273(mmsi: str) -> bool:
    return mmsi.startswith("273")

def best_label(mmsi: str, pts_sorted: List[Dict], name_by_mmsi: Dict[str,str], name_by_imo: Dict[str,str]) -> str:
    for p in (pts_sorted[0], pts_sorted[-1]):
        n = (p.get("name") or "").strip()
        if n:
            return n
    imo = (pts_sorted[0].get("imo") or "").strip()
    if mmsi in name_by_mmsi: return name_by_mmsi[mmsi]
    if imo and imo in name_by_imo: return name_by_imo[imo]
    return f"MMSI {mmsi}"

def build_track_features(pts_sorted: List[Dict], props: Dict, layer: str) -> List[Dict]:
    coords = [[p["lon"], p["lat"]] for p in pts_sorted]
    last = pts_sorted[-1]
    return [
        {"type":"Feature","properties":{**props,"feature":"track","layer":layer},"geometry":{"type":"LineString","coordinates":coords}},
        {"type":"Feature","properties":{**props,"feature":"last_position","layer":layer},"geometry":{"type":"Point","coordinates":[last["lon"], last["lat"]]}},
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

    (shadow_mmsi, shadow_imo,
     name_by_mmsi, name_by_imo,
     sanctions_by_mmsi, sanctions_by_imo,
     url_by_mmsi, url_by_imo) = load_shadowfleet_maps(args.shadowfleet)

    files = sorted(glob.glob(args.in_glob))
    if not files:
        raise SystemExit(f"No input files match: {args.in_glob}")

    day_tracks: Dict[str, List[Dict]] = defaultdict(list)
    seen_in_ru_ports: Set[str] = set()
    dest_today: Dict[str, Set[str]] = defaultdict(set)

    scanned = 0
    kept_day = 0

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

                if lookback_start_utc <= dt < day_end_utc and in_any_ru_port(lat, lon):
                    seen_in_ru_ports.add(mmsi)

                if day_start_utc <= dt < day_end_utc and in_main_area(lat, lon):
                    day_tracks[mmsi].append({
                        "ts_utc": ts, "lat": lat, "lon": lon,
                        "imo": digits_only(str(ev.get("imo",""))),
                        "name": str(ev.get("name","")).strip(),
                        "shiptype": str(ev.get("shiptype","")).strip(),
                        "destination": str(ev.get("destination","")).strip(),
                        "eta": str(ev.get("eta","")).strip(),
                    })
                    kept_day += 1
                    dest = str(ev.get("destination","")).strip()
                    if dest: dest_today[mmsi].add(dest)

    shadow_features, russia_features, ru_features = [], [], []

    for mmsi, pts in day_tracks.items():
        if len(pts) < 2: continue
        pts_sorted = sorted(pts, key=lambda p: p["ts_utc"])
        first, last = pts_sorted[0], pts_sorted[-1]
        imo = (first.get("imo") or last.get("imo") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()

        label = best_label(mmsi, pts_sorted, name_by_mmsi, name_by_imo)
        name = label if not label.startswith("MMSI ") else ""

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if imo else False)
        ru_mid = is_ru_mid273(mmsi)
        to_russia = any(RU_DEST_PAT.search(d) for d in dest_today.get(mmsi, set()))
        from_russia = (mmsi in seen_in_ru_ports)
        russia_related = bool(to_russia or from_russia)

        sanctions = ""
        gur_url = ""
        if shadow_hit:
            sanctions = sanctions_by_mmsi.get(mmsi) or (sanctions_by_imo.get(imo) if imo else "") or ""
            gur_url = url_by_mmsi.get(mmsi) or (url_by_imo.get(imo) if imo else "") or ""

        props = {
            "label": label, "mmsi": mmsi, "imo": imo, "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first["ts_utc"], "last_seen_utc": last["ts_utc"],
            "destination_samples": "; ".join(sorted(dest_today.get(mmsi,set())))[0:500],
            "eta": (last.get("eta") or first.get("eta") or "").strip(),
            "shadow_fleet": bool(shadow_hit),
            "russia_to": bool(to_russia),
            "russia_from": bool(from_russia),
            "russia_related": bool(russia_related),
            "ru_likely_mid273": bool(ru_mid),
            "sanctions_regimes": sanctions,
            "sanctions_source": "GUR war-sanctions site" if shadow_hit else "",
            "gur_url": gur_url,
        }

        if shadow_hit:
            shadow_features.extend(build_track_features(pts_sorted, props, "shadowfleet"))
        elif russia_related:
            russia_features.extend(build_track_features(pts_sorted, props, "russia_routes"))
        elif ru_mid:
            ru_features.extend(build_track_features(pts_sorted, props, "ru_mid273"))

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    out_shadow = outdir / f"lagebild_{args.date}_shadowfleet.geojson"
    out_russia = outdir / f"lagebild_{args.date}_russia_routes.geojson"
    out_ru = outdir / f"lagebild_{args.date}_ru_mid273.geojson"

    out_shadow.write_text(json.dumps({"type":"FeatureCollection","features":shadow_features}, ensure_ascii=False), encoding="utf-8")
    out_russia.write_text(json.dumps({"type":"FeatureCollection","features":russia_features}, ensure_ascii=False), encoding="utf-8")
    out_ru.write_text(json.dumps({"type":"FeatureCollection","features":ru_features}, ensure_ascii=False), encoding="utf-8")

    def ct(feats): return sum(1 for f in feats if f.get("properties",{}).get("feature")=="track")
    print(f"[daily] date={args.date} tz_used={tz_used} range_utc={day_start_utc.isoformat()}..{day_end_utc.isoformat()}")
    print(f"[daily] scanned_lines={scanned} kept_day_lines={kept_day} unique_mmsi_day={len(day_tracks)}")
    print(f"[daily] shadowfleet tracks={ct(shadow_features)} -> {out_shadow}")
    print(f"[daily] russia_routes tracks={ct(russia_features)} -> {out_russia}")
    print(f"[daily] ru_mid273 tracks={ct(ru_features)} -> {out_ru}")

if __name__ == "__main__":
    main()
