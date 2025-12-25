#!/usr/bin/env python3
"""
aisstream_collector.py (patched)

24/7 AIS collector for aisstream.io (WebSocket). Logs ONLY vessels from your watchlist into JSONL.

This patched version fixes two common real-world issues:
1) CSV delimiter problems (German Excel often saves CSV with ';' instead of ',')
2) Watchlists that have IMO but no MMSI (common for some OSINT lists):
   - AIS PositionReports have MMSI, not IMO.
   - AISStream also sends ShipStaticData sometimes (contains IMO). We use that to map MMSI↔IMO
     and then log PositionReports for watchlist IMOs even if MMSI is unknown.

Install:
  python -m pip install websockets

Run:
  python aisstream_collector.py --watchlist watchlist.csv --outdir logs --preset northsea_southbaltic

Env var:
  AISSTREAM_API_KEY (set as Windows environment variable)

Output:
  logs/aisstream_YYYY-MM-DD.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

import websockets

WS_URL = "wss://stream.aisstream.io/v0/stream"

# Presets as (min_lon, min_lat, max_lon, max_lat)
PRESETS = {
    "northsea_southbaltic": [
        (-6.0, 50.0, 10.5, 62.0),   # Nordsee (grob)
        (8.5, 53.3, 20.5, 56.2),    # südliche Ostsee (grob)
    ],
    "northsea": [(-6.0, 50.0, 10.5, 62.0)],
    "southbaltic": [(8.5, 53.3, 20.5, 56.2)],
}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def is_imo(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 7

def is_mmsi(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 9

@dataclass
class WatchMeta:
    category: str
    name: str
    imo: str
    mmsi: str
    sanctioned: bool
    sanctions: str
    note: str

def sniff_dialect(path: str) -> csv.Dialect:
    sample = Path(path).read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
    except Exception:
        # fallback: comma
        class D(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return D()

def read_watchlist(path: str) -> Tuple[Dict[str, WatchMeta], Set[str]]:
    """
    Returns:
      - dict keyed by MMSI (only entries with MMSI)
      - set of target IMOs (entries with IMO)
    CSV expected columns: category,name,imo,mmsi,sanctioned,sanctions,note
    Delimiter is auto-detected between comma and semicolon.
    """
    dialect = sniff_dialect(path)
    by_mmsi: Dict[str, WatchMeta] = {}
    target_imos: Set[str] = set()

    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, dialect=dialect)
        for row in r:
            mmsi_raw = (row.get("mmsi") or "").strip()
            imo_raw = (row.get("imo") or "").strip()

            mmsi = digits_only(mmsi_raw)
            imo = digits_only(imo_raw)

            if is_imo(imo):
                target_imos.add(imo)

            if not is_mmsi(mmsi):
                # no valid MMSI -> can't key it here, but IMO might still be useful
                continue

            by_mmsi[mmsi] = WatchMeta(
                category=(row.get("category") or "").strip(),
                name=(row.get("name") or "").strip(),
                imo=imo if is_imo(imo) else "",
                mmsi=mmsi,
                sanctioned=(row.get("sanctioned") or "").strip().lower() in ("1","true","yes","y"),
                sanctions=(row.get("sanctions") or "").strip(),
                note=(row.get("note") or "").strip(),
            )

    return by_mmsi, target_imos

def ensure_outdir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def bbox_to_aisstream(b: Tuple[float, float, float, float]) -> List[List[float]]:
    min_lon, min_lat, max_lon, max_lat = b
    return [[min_lat, min_lon], [max_lat, max_lon]]

class RotatingJsonlWriter:
    def __init__(self, outdir: Path, prefix: str = "aisstream"):
        self.outdir = outdir
        self.prefix = prefix
        self.cur_date: Optional[str] = None
        self.f = None

    def _open_for_date(self, date_str: str):
        if self.f:
            self.f.flush()
            self.f.close()
        self.cur_date = date_str
        fp = self.outdir / f"{self.prefix}_{date_str}.jsonl"
        self.f = open(fp, "a", encoding="utf-8")

    def write_event(self, ev: Dict):
        ts = ev.get("ts_utc") or utc_now_iso()
        date_str = str(ts)[:10]
        if self.cur_date != date_str or self.f is None:
            self._open_for_date(date_str)
        self.f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    def flush(self):
        if self.f:
            self.f.flush()

    def close(self):
        if self.f:
            self.f.flush()
            self.f.close()
            self.f = None

async def run_collector(
    api_key: str,
    bounding_boxes: List[Tuple[float, float, float, float]],
    watch_by_mmsi: Dict[str, WatchMeta],
    watch_imos: Set[str],
    outdir: Path,
    min_seconds_per_ship: int,
    flush_every: int,
    force_no_server_side_filter: bool,
):
    writer = RotatingJsonlWriter(outdir)
    last_written: Dict[str, datetime] = {}
    n_written = 0
    stop_event = asyncio.Event()

    def _stop(*_):
        stop_event.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass
    except Exception:
        pass

    bbox_payload = [bbox_to_aisstream(b) for b in bounding_boxes]

    # If we have IMO targets without MMSI, we MUST NOT use server-side MMSI filtering,
    # otherwise we would never receive those ships at all.
    server_side_possible = (not force_no_server_side_filter) and (len(watch_imos) == 0) and (len(watch_by_mmsi) <= 50) and (len(watch_by_mmsi) > 0)

    sub = {
        "APIKey": api_key,
        "BoundingBoxes": bbox_payload,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    if server_side_possible:
        sub["FiltersShipMMSI"] = sorted(watch_by_mmsi.keys())

    # We'll build MMSI→IMO map from ShipStaticData
    mmsi_to_imo: Dict[str, str] = {}

    backoff = 1.0
    backoff_max = 60.0

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=4000,
            ) as ws:
                await ws.send(json.dumps(sub))
                backoff = 1.0

                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if isinstance(msg, dict) and "error" in msg:
                        print(f"[aisstream] server error: {msg.get('error')}")
                        break

                    mtype = msg.get("MessageType")

                    if mtype == "ShipStaticData":
                        sd = (msg.get("Message") or {}).get("ShipStaticData") or {}
                        mmsi_val = sd.get("UserID")
                        if mmsi_val is None:
                            continue
                        mmsi = digits_only(str(mmsi_val))
                        if not is_mmsi(mmsi):
                            continue

                        imo_val = sd.get("ImoNumber") or sd.get("IMO") or sd.get("Imo")
                        imo = digits_only(str(imo_val or ""))
                        if is_imo(imo):
                            mmsi_to_imo[mmsi] = imo
                        # We don't write ShipStaticData events; we only use them for mapping.
                        continue

                    if mtype != "PositionReport":
                        continue

                    pr = (msg.get("Message") or {}).get("PositionReport") or {}
                    mmsi_val = pr.get("UserID")
                    if mmsi_val is None:
                        continue
                    mmsi = digits_only(str(mmsi_val))
                    if not is_mmsi(mmsi):
                        continue

                    lat = pr.get("Latitude")
                    lon = pr.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    try:
                        lat = float(lat); lon = float(lon)
                    except Exception:
                        continue
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        continue

                    # Decide whether this PositionReport belongs to our watchlist:
                    meta = watch_by_mmsi.get(mmsi)

                    # If not in MMSI watchlist, try IMO matching via ShipStaticData mapping:
                    mapped_imo = mmsi_to_imo.get(mmsi, "")
                    if (meta is None) and mapped_imo and (mapped_imo in watch_imos):
                        # Build lightweight meta from IMO-only watchlist (we keep minimal info)
                        meta = WatchMeta(
                            category="shadow_fleet",   # default; you can refine in weekly export
                            name="",
                            imo=mapped_imo,
                            mmsi=mmsi,
                            sanctioned=False,
                            sanctions="",
                            note="Matched via IMO from ShipStaticData (watchlist lacked MMSI).",
                        )

                    if meta is None:
                        continue

                    # Throttle per vessel to keep disk sane
                    now = datetime.now(timezone.utc)
                    prev = last_written.get(mmsi)
                    if prev and (now - prev).total_seconds() < min_seconds_per_ship:
                        continue
                    last_written[mmsi] = now

                    ev = {
                        "ts_utc": utc_now_iso(),
                        "mmsi": mmsi,
                        "imo": meta.imo or mapped_imo or "",
                        "name": meta.name or "",
                        "lat": lat,
                        "lon": lon,
                        "src": "aisstream",
                        "msg_type": "PositionReport",
                    }
                    if "Sog" in pr:
                        ev["sog"] = pr.get("Sog")
                    if "Cog" in pr:
                        ev["cog"] = pr.get("Cog")

                    writer.write_event(ev)
                    n_written += 1
                    if n_written % flush_every == 0:
                        writer.flush()
                        print(f"[aisstream] written: {n_written}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[aisstream] connection error: {e}")

        if stop_event.is_set():
            break

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, backoff_max)

    writer.close()
    print(f"[aisstream] stopped. total events written: {n_written}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="logs")
    ap.add_argument("--apikey", default=None)
    ap.add_argument("--preset", choices=list(PRESETS.keys()), default="northsea_southbaltic")
    ap.add_argument("--bbox", nargs=4, type=float, action="append", metavar=("MIN_LON","MIN_LAT","MAX_LON","MAX_LAT"))
    ap.add_argument("--min-seconds-per-ship", type=int, default=300, help="Default: 300s (5 min).")
    ap.add_argument("--flush-every", type=int, default=50)
    ap.add_argument("--no-server-side-filter", action="store_true", help="Force disable server-side MMSI filter.")
    args = ap.parse_args()

    api_key = (args.apikey or os.environ.get("AISSTREAM_API_KEY", "")).strip()
    if not api_key:
        raise SystemExit("Missing API key. Provide --apikey or set env AISSTREAM_API_KEY.")

    watch_by_mmsi, watch_imos = read_watchlist(args.watchlist)

    print(f"[aisstream] watchlist: {len(watch_by_mmsi)} MMSI entries, {len(watch_imos)} IMO entries")

    if not watch_by_mmsi and not watch_imos:
        raise SystemExit("Watchlist empty (no valid MMSI and no valid IMO). Check delimiter and columns.")

    outdir = ensure_outdir(args.outdir)

    if args.bbox:
        boxes = [tuple(b) for b in args.bbox]  # type: ignore
    else:
        boxes = [tuple(b) for b in PRESETS[args.preset]]  # type: ignore

    # Inform about filtering mode
    if (len(watch_imos) > 0) or args.no_server_side_filter:
        print("[aisstream] NOTE: server-side MMSI filter disabled (needed for IMO-matching or forced off).")
        print("[aisstream] This will receive all ships in the BBox and filter locally (CPU/network higher, disk stays low).")
    else:
        if len(watch_by_mmsi) <= 50:
            print("[aisstream] server-side FiltersShipMMSI enabled (<= 50 MMSI).")
        else:
            print("[aisstream] local filtering (watchlist MMSI > 50).")

    asyncio.run(run_collector(
        api_key=api_key,
        bounding_boxes=boxes,
        watch_by_mmsi=watch_by_mmsi,
        watch_imos=watch_imos,
        outdir=outdir,
        min_seconds_per_ship=args.min_seconds_per_ship,
        flush_every=max(1, args.flush_every),
        force_no_server_side_filter=args.no_server_side_filter,
    ))

if __name__ == "__main__":
    main()
