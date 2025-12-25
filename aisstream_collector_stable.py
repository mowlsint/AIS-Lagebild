#!/usr/bin/env python3
"""
aisstream_collector_stable.py

Drop-in replacement for aisstream_collector.py with more robust reconnect handling.
Focus: reduce noisy reconnect loops and be nicer when aisstream throttles.

Key improvements
- Higher open_timeout (configurable) to reduce "timed out during opening handshake"
- Special handling for server error "concurrent connections per user exceeded"
  -> waits longer before retrying (default 120s)
- Periodic "alive" heartbeat prints while connected (default every 5 minutes)

Usage (Windows cmd):
  cd "C:\\Users\\User\\Documents\\WSP\\GIS-Analyse\\AIS-Lagebild"
  python aisstream_collector_stable.py --watchlist watchlist.csv --outdir logs --preset northsea_southbaltic

Install:
  python -m pip install websockets

API key:
  set env var AISSTREAM_API_KEY
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

PRESETS = {
    "northsea_southbaltic": [
        (-6.0, 50.0, 10.5, 62.0),
        (8.5, 53.3, 20.5, 56.2),
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
        class D(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return D()

def read_watchlist(path: str) -> Tuple[Dict[str, WatchMeta], Set[str]]:
    dialect = sniff_dialect(path)
    by_mmsi: Dict[str, WatchMeta] = {}
    target_imos: Set[str] = set()

    with open(path, newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, dialect=dialect)
        for row in r:
            mmsi = digits_only((row.get("mmsi") or "").strip())
            imo = digits_only((row.get("imo") or "").strip())

            if is_imo(imo):
                target_imos.add(imo)

            if not is_mmsi(mmsi):
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

async def heartbeat_printer(stop: asyncio.Event, minutes: int):
    if minutes <= 0:
        return
    while not stop.is_set():
        await asyncio.sleep(minutes * 60)
        if not stop.is_set():
            print(f"[aisstream] alive {utc_now_iso()} (waiting for watchlist hits)")

async def run_collector(
    api_key: str,
    bounding_boxes: List[Tuple[float, float, float, float]],
    watch_by_mmsi: Dict[str, WatchMeta],
    outdir: Path,
    min_seconds_per_ship: int,
    flush_every: int,
    open_timeout: int,
    throttle_wait_seconds: int,
    alive_minutes: int,
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

    sub = {
        "APIKey": api_key,
        "BoundingBoxes": bbox_payload,
        "FilterMessageTypes": ["PositionReport"],
        "FiltersShipMMSI": sorted(watch_by_mmsi.keys()),
    }

    backoff = 2.0
    backoff_max = 60.0

    hb_task = asyncio.create_task(heartbeat_printer(stop_event, alive_minutes))

    try:
        while not stop_event.is_set():
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=2000,
                    open_timeout=open_timeout,
                ) as ws:
                    await ws.send(json.dumps(sub))
                    backoff = 2.0

                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if isinstance(msg, dict) and "error" in msg:
                            err = str(msg.get("error") or "")
                            print(f"[aisstream] server error: {err}")
                            if "concurrent connections" in err.lower():
                                print(f"[aisstream] throttled: waiting {throttle_wait_seconds}s before retry...")
                                await asyncio.sleep(throttle_wait_seconds)
                            break

                        if msg.get("MessageType") != "PositionReport":
                            continue

                        pr = (msg.get("Message") or {}).get("PositionReport") or {}
                        mmsi_val = pr.get("UserID")
                        if mmsi_val is None:
                            continue
                        mmsi = digits_only(str(mmsi_val))
                        if mmsi not in watch_by_mmsi:
                            continue

                        lat = pr.get("Latitude"); lon = pr.get("Longitude")
                        if lat is None or lon is None:
                            continue
                        try:
                            lat = float(lat); lon = float(lon)
                        except Exception:
                            continue

                        now = datetime.now(timezone.utc)
                        prev = last_written.get(mmsi)
                        if prev and (now - prev).total_seconds() < min_seconds_per_ship:
                            continue
                        last_written[mmsi] = now

                        meta = watch_by_mmsi[mmsi]
                        ev = {
                            "ts_utc": utc_now_iso(),
                            "mmsi": mmsi,
                            "imo": meta.imo or "",
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

    finally:
        stop_event.set()
        hb_task.cancel()
        writer.close()
        print(f"[aisstream] stopped. total events written: {n_written}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="logs")
    ap.add_argument("--apikey", default=None)
    ap.add_argument("--preset", choices=list(PRESETS.keys()), default="northsea_southbaltic")
    ap.add_argument("--bbox", nargs=4, type=float, action="append", metavar=("MIN_LON","MIN_LAT","MAX_LON","MAX_LAT"))
    ap.add_argument("--min-seconds-per-ship", type=int, default=300)
    ap.add_argument("--flush-every", type=int, default=50)
    ap.add_argument("--open-timeout", type=int, default=45)
    ap.add_argument("--throttle-wait", type=int, default=120)
    ap.add_argument("--alive-minutes", type=int, default=5)
    args = ap.parse_args()

    api_key = (args.apikey or os.environ.get("AISSTREAM_API_KEY", "")).strip()
    if not api_key:
        raise SystemExit("Missing API key. Provide --apikey or set env AISSTREAM_API_KEY.")

    watch_by_mmsi, watch_imos = read_watchlist(args.watchlist)
    print(f"[aisstream] watchlist: {len(watch_by_mmsi)} MMSI entries, {len(watch_imos)} IMO entries")

    if not watch_by_mmsi:
        raise SystemExit("Watchlist has no valid 9-digit MMSI. (This stable version is MMSI-only.)")

    outdir = ensure_outdir(args.outdir)

    if args.bbox:
        boxes = [tuple(b) for b in args.bbox]
    else:
        boxes = [tuple(b) for b in PRESETS[args.preset]]

    if len(watch_by_mmsi) <= 50:
        print("[aisstream] server-side FiltersShipMMSI enabled (<= 50 MMSI).")
    else:
        print("[aisstream] NOTE: MMSI count > 50. This stable version is intended for <= 50 MMSI.")

    asyncio.run(run_collector(
        api_key=api_key,
        bounding_boxes=boxes,
        watch_by_mmsi=watch_by_mmsi,
        outdir=outdir,
        min_seconds_per_ship=args.min_seconds_per_ship,
        flush_every=max(1, args.flush_every),
        open_timeout=max(5, args.open_timeout),
        throttle_wait_seconds=max(10, args.throttle_wait),
        alive_minutes=max(0, args.alive_minutes),
    ))

if __name__ == "__main__":
    main()
