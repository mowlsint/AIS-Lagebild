#!/usr/bin/env python3
r"""
make_daily_4_geojson_layers_v3.py (FULL REPLACEMENT - for logs_v4 bbox_*.jsonl)

Input format (one JSON object per line), like your sampler writes:
{"ts_utc":"2025-12-30T00:00:02Z","mmsi":"232026149","imo":"9695523","name":"...","lat":50.89,"lon":-1.39,...}

Outputs (dated + live) into --outdir:
Dated:
- lagebild_YYYY-MM-DD_shadowfleet.geojson
- lagebild_YYYY-MM-DD_ru_mid273.geojson
- lagebild_YYYY-MM-DD_from_russia_ports_excluding_shadow_mid273.geojson
- lagebild_YYYY-MM-DD_pre_sanctioned.geojson

Live aliases (always overwritten):
- live_shadowfleet.geojson
- live_ru_mid273.geojson
- live_from_russia_ports_excl_shadow.geojson
- live_pre_sanctioned.geojson

Classification logic:
- shadowfleet: MMSI/IMO match via --shadowfleet CSV (loose digit parser)
- ru_mid273: MMSI starts with "273"
- from_russia_ports_excluding_shadow_mid273:
    Heuristic: ship seen in a RU-port bbox during lookback window
    AND ship is seen in the monitoring area on the target --date (UTC day)
    AND not shadowfleet
    AND not ru_mid273
- pre_sanctioned:
    IMO/MMSI match via --presanction CSV
    (default behavior: EXCLUDES shadowfleet hits so you don't double-tag)
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# -----------------------------
# Helpers
# -----------------------------
def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def is_imo(s: str) -> bool:
    s = digits_only(s)
    return len(s) == 7


def parse_iso_z(s: str) -> Optional[datetime]:
    try:
        s = (s or "").strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def safe_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def read_shadowfleet_csv(fp: Path) -> Tuple[set, set]:
    """
    Loose parser: collects any 9-digit sequences as MMSI and any 7-digit as IMO.
    """
    shadow_mmsi, shadow_imo = set(), set()
    if not fp.exists():
        return shadow_mmsi, shadow_imo
    for line in fp.read_text(encoding="utf-8", errors="ignore").splitlines():
        for n in re.findall(r"\d+", line):
            if len(n) == 9:
                shadow_mmsi.add(n)
            elif len(n) == 7:
                shadow_imo.add(n)
    return shadow_mmsi, shadow_imo


def read_presanction_csv(fp: Path) -> Tuple[Dict[str, Dict], set, set]:
    """
    Prefer: a proper CSV with headers (imo, vessel_name, kse_category, ...).
    Fallback: loose digit parser (like shadowfleet) if not parseable.

    Returns:
      meta_by_imo: dict[imo] -> dict of extra metadata
      pres_mmsi_set, pres_imo_set
    """
    meta_by_imo: Dict[str, Dict] = {}
    pres_mmsi, pres_imo = set(), set()
    if not fp.exists():
        return meta_by_imo, pres_mmsi, pres_imo

    txt = fp.read_text(encoding="utf-8", errors="ignore")
    # Try CSV header parse first
    try:
        reader = csv.DictReader(txt.splitlines())
        if reader.fieldnames:
            for row in reader:
                imo = digits_only(row.get("imo", "") or row.get("IMO", "") or "")
                mmsi = digits_only(row.get("mmsi", "") or row.get("MMSI", "") or "")
                if is_imo(imo):
                    pres_imo.add(imo)
                    # keep a small, stable subset of fields for popups
                    meta = {}
                    for k in [
                        "vessel_name","name","tanker_size","build_year","flag",
                        "kse_category","kse_source","kse_as_of_date","pre_sanction_reason","pre_sanction_source"
                    ]:
                        if k in row and (row.get(k) not in (None, "")):
                            meta[k] = row.get(k)
                    # normalize a couple of common alternates
                    if "name" in row and "vessel_name" not in meta and row.get("name"):
                        meta["vessel_name"] = row.get("name")
                    if "Vessel" in row and "vessel_name" not in meta and row.get("Vessel"):
                        meta["vessel_name"] = row.get("Vessel")
                    meta_by_imo[imo] = meta
                if len(mmsi) == 9:
                    pres_mmsi.add(mmsi)
        # If we got any IMO/MMSI, we're good
        if pres_imo or pres_mmsi:
            return meta_by_imo, pres_mmsi, pres_imo
    except Exception:
        pass

    # Fallback: loose digit scrape
    for line in txt.splitlines():
        for n in re.findall(r"\d+", line):
            if len(n) == 9:
                pres_mmsi.add(n)
            elif len(n) == 7:
                pres_imo.add(n)
    return meta_by_imo, pres_mmsi, pres_imo


def mk_links(name: str, mmsi: str, imo: str) -> Tuple[str, str, str]:
    name_s = (name or "").strip()
    mmsi_s = digits_only(mmsi or "")
    imo_s = digits_only(imo or "")
    q = imo_s if is_imo(imo_s) else (mmsi_s or name_s)
    q_enc = re.sub(r"\s+", "+", q.strip())

    vf_url = f"https://www.vesselfinder.com/vessels?name={q_enc}" if q_enc else ""
    mt_url = f"https://www.marinetraffic.com/en/ais/search/all?keyword={q_enc}" if q_enc else ""

    links = []
    if vf_url:
        links.append(f'<a href="{vf_url}" target="_blank" rel="noopener">VesselFinder</a>')
    if mt_url:
        links.append(f'<a href="{mt_url}" target="_blank" rel="noopener">MarineTraffic</a>')

    return " | ".join(links), vf_url, mt_url


def write_fc(out_fp: Path, features: List[Dict]) -> None:
    out_fp.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )


def in_any_bbox(lon: float, lat: float, bboxes: List[Tuple[float, float, float, float]]) -> bool:
    for min_lon, min_lat, max_lon, max_lat in bboxes:
        if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat:
            return True
    return False


# -----------------------------
# Presets (heuristics)
# -----------------------------
# Monitoring space (rough): North Sea + South Baltic
MONITOR_BBOXES = [
    (-6.0, 50.0, 10.5, 62.0),   # North Sea
    (8.5, 53.3, 20.5, 56.2),    # South Baltic
]

# RU port/gateway bboxes (coarse, but good enough as "seen near RU ports" signal)
RU_PORT_BBOXES = [
    # Baltic: Ust-Luga / Primorsk / St. Petersburg region
    (27.0, 59.0, 31.0, 60.7),
    # Kaliningrad / Baltiysk
    (19.0, 54.2, 21.8, 55.4),
    # Murmansk / Kola
    (32.0, 68.5, 34.8, 69.4),
    # Arkhangelsk / White Sea approach
    (38.0, 64.0, 41.0, 65.5),
    # Novorossiysk (optional)
    (36.0, 44.5, 38.5, 45.5),
]


# -----------------------------
# GeoJSON feature builders
# -----------------------------
def mk_point_feature(ev: Dict, props: Dict) -> Optional[Dict]:
    lon = safe_float(ev.get("lon"))
    lat = safe_float(ev.get("lat"))
    if lon is None or lat is None:
        return None
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props}


def mk_line_feature(pts_sorted: List[Dict], props: Dict, min_pts: int) -> Optional[Dict]:
    coords = []
    for ev in pts_sorted:
        lon = safe_float(ev.get("lon"))
        lat = safe_float(ev.get("lat"))
        if lon is None or lat is None:
            continue
        coords.append([lon, lat])
    if len(coords) < min_pts:
        return None
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": coords}, "properties": props}


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_glob", required=True, help="Input glob, e.g. logs_v4/bbox_*.jsonl")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day window, because ts_utc is Z)")
    ap.add_argument("--shadowfleet", required=True, help="CSV containing shadowfleet MMSI/IMO")
    ap.add_argument("--presanction", required=True, help="CSV containing pre-sanctioned vessels (e.g. KSE list)")
    ap.add_argument("--outdir", required=True, help="Output directory (repo/public)")
    ap.add_argument("--lookback-days", type=int, default=21, help="Lookback for RU-port heuristic")
    ap.add_argument("--min-track-points", type=int, default=2, help="Min points for LineString")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        day = date.fromisoformat(args.date)
    except Exception as e:
        raise SystemExit(f"Invalid --date {args.date!r}. Expected YYYY-MM-DD.") from e

    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    cutoff = day_start - timedelta(days=int(args.lookback_days))

    shadow_mmsi, shadow_imo = read_shadowfleet_csv(Path(args.shadowfleet))
    pres_meta_by_imo, pres_mmsi, pres_imo = read_presanction_csv(Path(args.presanction))

    per_mmsi: Dict[str, List[Dict]] = {}
    ru_seen_lookback: Dict[str, bool] = {}
    seen_in_monitor_on_day: Dict[str, bool] = {}

    files = sorted(glob.glob(args.in_glob))
    if not files:
        print(f"[warn] No input files matched: {args.in_glob}")

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue

                    dt = parse_iso_z(ev.get("ts_utc"))
                    if not dt or dt < cutoff:
                        continue

                    mmsi = digits_only(str(ev.get("mmsi") or ""))
                    if not mmsi:
                        continue

                    per_mmsi.setdefault(mmsi, []).append(ev)

                    lon = safe_float(ev.get("lon"))
                    lat = safe_float(ev.get("lat"))
                    if lon is None or lat is None:
                        continue

                    if in_any_bbox(lon, lat, RU_PORT_BBOXES):
                        ru_seen_lookback[mmsi] = True

                    if day_start <= dt < day_end and in_any_bbox(lon, lat, MONITOR_BBOXES):
                        seen_in_monitor_on_day[mmsi] = True

        except Exception as e:
            print(f"[warn] Failed reading {fp}: {e}")

    feats_shadow: List[Dict] = []
    feats_mid273: List[Dict] = []
    feats_fromru: List[Dict] = []
    feats_presanction: List[Dict] = []

    for mmsi, pts in per_mmsi.items():
        pts_sorted = sorted(pts, key=lambda x: x.get("ts_utc") or "")
        if not pts_sorted:
            continue

        first = pts_sorted[0]
        last = pts_sorted[-1]

        imo = digits_only(str(first.get("imo") or last.get("imo") or ""))
        name = (first.get("name") or last.get("name") or "").strip()
        shiptype = (first.get("shiptype") or last.get("shiptype") or "").strip()
        dest = (last.get("destination") or first.get("destination") or "").strip()
        eta = (last.get("eta") or first.get("eta") or "").strip()

        shadow_hit = (mmsi in shadow_mmsi) or (imo in shadow_imo if is_imo(imo) else False)
        mid273 = mmsi.startswith("273")

        ru_seen = bool(ru_seen_lookback.get(mmsi, False))
        in_day_area = bool(seen_in_monitor_on_day.get(mmsi, False))
        from_ru_ports = ru_seen and in_day_area

        pres_hit = ((mmsi in pres_mmsi) if mmsi else False) or ((imo in pres_imo) if is_imo(imo) else False)
        # default: do not double-tag shadowfleet ships as pre-sanction
        if shadow_hit:
            pres_hit = False

        links, vf_url, mt_url = mk_links(name=name, mmsi=mmsi, imo=imo)
        label = name or (pres_meta_by_imo.get(imo, {}).get("vessel_name") if is_imo(imo) else "") or mmsi
        display = f"{label} ({mmsi})" if label else mmsi

        base_props = {
            "label": label,
            "mmsi": mmsi,
            "imo": imo,
            "name": name,
            "shiptype": shiptype,
            "first_seen_utc": first.get("ts_utc", ""),
            "last_seen_utc": last.get("ts_utc", ""),
            "destination": dest,
            "eta": eta,
            "display": display,
            "links": links,
            "vf_url": vf_url,
            "mst_url": mt_url,
            "method": "v3_plus_presanction",
            "ru_seen_in_lookback": ru_seen,
            "in_monitor_on_day": in_day_area,
        }

        # merge in pre-sanction metadata (KSE fields etc.) if present
        if pres_hit and is_imo(imo) and imo in pres_meta_by_imo:
            for k, v in pres_meta_by_imo[imo].items():
                if v not in (None, ""):
                    base_props[k] = v

        track_props = dict(base_props)
        track_props["display"] = ""

        p = mk_point_feature(last, base_props)
        ls = mk_line_feature(pts_sorted, track_props, min_pts=int(args.min_track_points))

        if shadow_hit:
            if ls:
                feats_shadow.append(ls)
            if p:
                feats_shadow.append(p)

        if mid273:
            if ls:
                feats_mid273.append(ls)
            if p:
                feats_mid273.append(p)

        if from_ru_ports and (not shadow_hit) and (not mid273):
            if ls:
                feats_fromru.append(ls)
            if p:
                feats_fromru.append(p)

        if pres_hit:
            # mark it explicitly for styling/filtering in uMap
            if p:
                p["properties"]["pre_sanctioned"] = True
                p["properties"].setdefault("pre_sanction_source", p["properties"].get("kse_source", "KSE"))
                feats_presanction.append(p)
            if ls:
                ls["properties"]["pre_sanctioned"] = True
                ls["properties"].setdefault("pre_sanction_source", ls["properties"].get("kse_source", "KSE"))
                feats_presanction.append(ls)

    # Write dated + live
    fp_shadow = outdir / f"lagebild_{args.date}_shadowfleet.geojson"
    fp_mid = outdir / f"lagebild_{args.date}_ru_mid273.geojson"
    fp_fromru = outdir / f"lagebild_{args.date}_from_russia_ports_excluding_shadow_mid273.geojson"
    fp_pres = outdir / f"lagebild_{args.date}_pre_sanctioned.geojson"

    write_fc(fp_shadow, feats_shadow)
    write_fc(fp_mid, feats_mid273)
    write_fc(fp_fromru, feats_fromru)
    write_fc(fp_pres, feats_presanction)

    write_fc(outdir / "live_shadowfleet.geojson", feats_shadow)
    write_fc(outdir / "live_ru_mid273.geojson", feats_mid273)
    write_fc(outdir / "live_from_russia_ports_excl_shadow.geojson", feats_fromru)
    write_fc(outdir / "live_pre_sanctioned.geojson", feats_presanction)

    print(f"[ok] shadowfleet   features: {len(feats_shadow)}")
    print(f"[ok] ru_mid273     features: {len(feats_mid273)}")
    print(f"[ok] from_ru       features: {len(feats_fromru)}")
    print(f"[ok] pre_sanction  features: {len(feats_presanction)}")
    print(f"[ok] wrote dated + live geojson to: {outdir}")


if __name__ == "__main__":
    main()
