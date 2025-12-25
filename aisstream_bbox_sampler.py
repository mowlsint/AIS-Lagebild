#!/usr/bin/env python3
"""
aisstream_bbox_sampler.py

Purpose
- Collect a *throttled* AIS feed for a bounding box (North Sea / South Baltic preset)
- Store downsampled PositionReports to JSONL so you can build a weekly "Lagebild"
- Optionally enrich PositionReports with ShipStaticData (name/IMO), when available

Why you need this
Your GUR shadow-fleet MMSI list is global. During your 20s probe, *zero* of those MMSI were present
in the North Sea/South Baltic BBox — so a watchlist-only collector can legitimately stay 0 bytes.
The solution: collect a thin "background sample" of all vessels in the BBox, then classify weekly:
- Shadow-fleet hit: MMSI (or IMO) appears in GUR list
- RU-likely: MMSI MID == 273 (Indiz, not perfect)

Install once:
  python -m pip install websockets

Run (Windows cmd, from your project folder):
  python aisstream_bbox_sampler.py --outdir logs --preset northsea_southbaltic --min-seconds-per-ship 1800

Output:
  logs/bbox_YYYY-MM-DD.jsonl

API Key:
  environment variable AISSTREAM_API_KEY must be set
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import websockets

WS_URL = "wss://stream.aisstream.io/v0/stream"

# Presets as (min_lon, min_lat, max_lon, max_lat)
PRESETS = {
    "northsea_southbaltic": [
        (-6.0, 50.0, 10.5, 62.0),   # North Sea (coarse)
        (8.5, 53.3, 20.5, 56.2),    # South Baltic (coarse)
    ],
    "northsea": [(-6.0, 50.0, 10.5, 62.0)],
    "southbaltic": [(8.5, 53.3, 20.5, 56.2)],
}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def is_mmsi(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 9

def bbox_to_aisstream(b: Tuple[float, float, float, float]) -> List[List[float]]:
    min_lon, min_lat, max_lon, max_lat = b
    return [[min_lat, min_lon], [max_lat, max_lon]]

class RotatingJsonlWriter:
    def __init__(self, outdir: Path, prefix: str = "bbox"):
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

async def heartbeat(stop: asyncio.Event, minutes: int):
    if minutes <= 0:
        return
    while not stop.is_set():
        await asyncio.sleep(minutes * 60)
        if not stop.is_set():
            print(f"[bbox] alive {utc_now_iso()}")

async def run_sampler(
    api_key: str,
    boxes: List[Tuple[float, float, float, float]],
    outdir: Path,
    min_seconds_per_ship: int,
    flush_every: int,
    open_timeout: int,
    alive_minutes: int,
):
    outdir.mkdir(parents=True, exist_ok=True)
    writer = RotatingJsonlWriter(outdir)
    stop_event = asyncio.Event()

    # last write per MMSI (throttle)
    last_written: Dict[str, datetime] = {}

    # in-memory enrichment map from ShipStaticData
    meta: Dict[str, Dict[str, str]] = {}  # mmsi -> {name, imo, callsign, shiptype}

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

    bbox_payload = [bbox_to_aisstream(b) for b in boxes]
    sub = {
        "APIKey": api_key,
        "BoundingBoxes": bbox_payload,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    hb_task = asyncio.create_task(heartbeat(stop_event, alive_minutes))

    n_written = 0
    backoff = 2.0
    backoff_max = 60.0

    try:
        while not stop_event.is_set():
            try:
                print(f"[bbox] connecting… {utc_now_iso()}")
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=4000,
                    open_timeout=open_timeout,
                ) as ws:
                    await ws.send(json.dumps(sub))
                    print(f"[bbox] subscribed. throttle={min_seconds_per_ship}s per MMSI.")
                    backoff = 2.0

                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if isinstance(msg, dict) and "error" in msg:
                            print(f"[bbox] server error: {msg.get('error')}")
                            break

                        mtype = msg.get("MessageType")

                        if mtype == "ShipStaticData":
                            sd = (msg.get("Message") or {}).get("ShipStaticData") or {}
                            mmsi = digits_only(str(sd.get("UserID","")))
                            if not is_mmsi(mmsi):
                                continue
                            meta[mmsi] = {
                                "name": str(sd.get("Name") or sd.get("ShipName") or "").strip(),
                                "imo": digits_only(str(sd.get("ImoNumber") or sd.get("IMO") or "")),
                                "callsign": str(sd.get("CallSign") or "").strip(),
                                "shiptype": str(sd.get("ShipType") or "").strip(),
                            }
                            continue

                        if mtype != "PositionReport":
                            continue

                        pr = (msg.get("Message") or {}).get("PositionReport") or {}
                        mmsi = digits_only(str(pr.get("UserID","")))
                        if not is_mmsi(mmsi):
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

                        m = meta.get(mmsi, {})
                        ev = {
                            "ts_utc": utc_now_iso(),
                            "mmsi": mmsi,
                            "imo": m.get("imo",""),
                            "name": m.get("name",""),
                            "callsign": m.get("callsign",""),
                            "shiptype": m.get("shiptype",""),
                            "lat": lat,
                            "lon": lon,
                            "sog": pr.get("Sog"),
                            "cog": pr.get("Cog"),
                            "src": "aisstream_bbox_sampler",
                        }
                        writer.write_event(ev)
                        n_written += 1
                        if n_written % flush_every == 0:
                            writer.flush()
                            print(f"[bbox] written: {n_written}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[bbox] connection error: {e}")

            if stop_event.is_set():
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)

    finally:
        stop_event.set()
        hb_task.cancel()
        writer.close()
        print(f"[bbox] stopped. total events written: {n_written}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="logs")
    ap.add_argument("--apikey", default=None)
    ap.add_argument("--preset", choices=list(PRESETS.keys()), default="northsea_southbaltic")
    ap.add_argument("--bbox", nargs=4, type=float, action="append", metavar=("MIN_LON","MIN_LAT","MAX_LON","MAX_LAT"))
    ap.add_argument("--min-seconds-per-ship", type=int, default=1800, help="Default 1800s (30 min).")
    ap.add_argument("--flush-every", type=int, default=200)
    ap.add_argument("--open-timeout", type=int, default=60)
    ap.add_argument("--alive-minutes", type=int, default=5)
    args = ap.parse_args()

    api_key = (args.apikey or os.environ.get("AISSTREAM_API_KEY", "")).strip()
    if not api_key:
        raise SystemExit("Missing API key. Provide --apikey or set env AISSTREAM_API_KEY.")

    outdir = Path(args.outdir)

    if args.bbox:
        boxes = [tuple(b) for b in args.bbox]  # type: ignore
    else:
        boxes = [tuple(b) for b in PRESETS[args.preset]]  # type: ignore

    print(f"[bbox] output dir: {outdir.resolve()}")
    asyncio.run(run_sampler(
        api_key=api_key,
        boxes=boxes,
        outdir=outdir,
        min_seconds_per_ship=max(10, args.min_seconds_per_ship),
        flush_every=max(1, args.flush_every),
        open_timeout=max(5, args.open_timeout),
        alive_minutes=max(0, args.alive_minutes),
    ))

if __name__ == "__main__":
    main()
