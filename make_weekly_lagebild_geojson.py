#!/usr/bin/env python3
"""
make_weekly_lagebild_geojson.py

Build a weekly Lagebild GeoJSON (uMap-ready) from:
1) a WATCHLIST (CSV)
2) AIS event logs (JSONL)

Usage:
  python make_weekly_lagebild_geojson.py --watchlist watchlist.csv --in logs/*.jsonl --bbox -6 50 20.5 62 --week 2025-W51 --out lagebild_2025-W51.geojson

If --week is omitted, it uses the current ISO week (UTC).
"""

from __future__ import annotations
import argparse, csv, glob, json
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

def parse_iso_week(s: str) -> Tuple[int, int]:
    year, w = s.split("-W"); return int(year), int(w)

def week_start_end_utc(iso_year: int, iso_week: int) -> Tuple[datetime, datetime]:
    start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
    return start, start + timedelta(days=7)

def iso_to_dt(ts: str) -> Optional[datetime]:
    if not ts: return None
    ts = ts.strip()
    if ts.endswith("Z"): ts = ts[:-1] + "+00:00"
    try: return datetime.fromisoformat(ts)
    except Exception: return None

def in_bbox(lon: float, lat: float, bbox: Tuple[float, float, float, float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (min_lon <= lon <= max_lon) and (min_lat <= lat <= max_lat)

def dt_to_isoz(dt: Optional[datetime]) -> Optional[str]:
    if not dt: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

@dataclass
class WatchItem:
    category: str; name: str; imo: str; mmsi: str; sanctioned: bool; sanctions: str; note: str

@dataclass
class Seen:
    key: str; category: str; name: str; imo: str; mmsi: str; sanctioned: bool; sanctions: str
    first_seen: Optional[datetime]=None; last_seen: Optional[datetime]=None
    last_lat: Optional[float]=None; last_lon: Optional[float]=None; last_src: Optional[str]=None
    sightings: int=0; track: Optional[List[Tuple[float,float]]]=None

def load_watchlist(path: str) -> Dict[str, WatchItem]:
    out: Dict[str, WatchItem] = {}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            category = (row.get("category") or "").strip() or "unknown"
            name = (row.get("name") or "").strip()
            imo = (row.get("imo") or "").strip()
            mmsi = (row.get("mmsi") or "").strip()
            sanctioned = (row.get("sanctioned") or "").strip().lower() in ("1","true","yes","y")
            sanctions = (row.get("sanctions") or "").strip()
            note = (row.get("note") or "").strip()
            key = imo if (imo.isdigit() and len(imo)==7) else mmsi
            if key: out[key] = WatchItem(category,name,imo,mmsi,sanctioned,sanctions,note)
    return out

def iter_events(patterns: List[str]):
    files: List[str] = []
    for p in patterns: files.extend(glob.glob(p))
    for fp in sorted(set(files)):
        with open(fp, encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try: yield json.loads(line)
                except Exception: continue

def to_features(seen: Seen, week: str, trackline: bool) -> List[Dict]:
    props = {
        "name": seen.name, "category": seen.category, "imo": seen.imo, "mmsi": seen.mmsi,
        "sanctioned": bool(seen.sanctioned), "sanctions": seen.sanctions, "week": week,
        "first_seen_utc": dt_to_isoz(seen.first_seen), "last_seen_utc": dt_to_isoz(seen.last_seen),
        "sightings_count": seen.sightings, "last_position_source": seen.last_src,
    }
    geom = None if (seen.last_lon is None or seen.last_lat is None) else {"type":"Point","coordinates":[seen.last_lon, seen.last_lat]}
    out = [{"type":"Feature","geometry": geom, "properties": props}]
    if trackline and seen.track and len(seen.track)>=2:
        out.append({"type":"Feature","geometry":{"type":"LineString","coordinates":[[lon,lat] for lon,lat in seen.track]},
                    "properties":{**props,"name":f"{seen.name} (Trackline)","geometry_role":"trackline"}})
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--in", dest="inputs", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--week", default=None)
    ap.add_argument("--bbox", nargs=4, type=float, required=True, metavar=("MIN_LON","MIN_LAT","MAX_LON","MAX_LAT"))
    ap.add_argument("--trackline", action="store_true")
    ap.add_argument("--sample-minutes", type=int, default=30)
    args=ap.parse_args()

    bbox=tuple(args.bbox)
    if args.week: y,w=parse_iso_week(args.week)
    else:
        iso=date.today().isocalendar(); y,w=iso.year, iso.week
    week=f"{y}-W{w:02d}"
    start,end=week_start_end_utc(y,w)
    watch=load_watchlist(args.watchlist)

    seen_map: Dict[str, Seen] = {}
    last_sample_ts: Dict[str, datetime] = {}

    for ev in iter_events(args.inputs):
        ts=iso_to_dt(ev.get("ts_utc",""))
        if not ts: continue
        if ts.tzinfo is None: ts=ts.replace(tzinfo=timezone.utc)
        ts=ts.astimezone(timezone.utc)
        if not (start<=ts<end): continue

        try: lat=float(ev["lat"]); lon=float(ev["lon"])
        except Exception: continue
        if not in_bbox(lon,lat,bbox): continue

        imo=str(ev.get("imo") or "").strip()
        mmsi=str(ev.get("mmsi") or "").strip()
        key=imo if (imo.isdigit() and len(imo)==7) else mmsi
        if not key: continue
        wi=watch.get(key)
        if not wi: continue

        s=seen_map.get(key)
        if not s:
            s=Seen(key=key, category=wi.category, name=wi.name or (ev.get("name") or key),
                   imo=wi.imo or imo, mmsi=wi.mmsi or mmsi, sanctioned=wi.sanctioned, sanctions=wi.sanctions,
                   track=[] if args.trackline else None)
            seen_map[key]=s

        s.sightings += 1
        if s.first_seen is None or ts < s.first_seen: s.first_seen=ts
        if s.last_seen is None or ts > s.last_seen:
            s.last_seen=ts; s.last_lat, s.last_lon=lat, lon; s.last_src=ev.get("src")

        if args.trackline and s.track is not None:
            prev=last_sample_ts.get(key)
            if (prev is None) or ((ts-prev).total_seconds()>=args.sample_minutes*60):
                s.track.append((lon,lat)); last_sample_ts[key]=ts

    features: List[Dict] = []
    for s in seen_map.values(): features.extend(to_features(s, week, args.trackline))

    cat_order={"shadow_fleet":0,"russian_flagged":1}
    features.sort(key=lambda f:(cat_order.get(f["properties"].get("category",""),9), f["properties"].get("name","")))

    with open(args.out,"w",encoding="utf-8") as f:
        json.dump({"type":"FeatureCollection","features":features}, f, ensure_ascii=False)

    print(f"[+] Week {week}: {len(seen_map)} watchlist vessels seen -> {args.out}")

if __name__=="__main__": main()
