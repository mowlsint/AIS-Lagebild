#!/usr/bin/env python3
"""
aisstream_probe_bbox.py

Kurztest: Kommen überhaupt AIS-Meldungen aus der BBox bei dir an?

- Verbindet sich zu aisstream.io
- Abonniert eine BoundingBox (Preset wie im Collector)
- Schreibt die ersten N PositionReports in eine JSONL-Datei
- Danach beendet sich das Script automatisch

Installation:
  python -m pip install websockets

Start (im Projektordner):
  python aisstream_probe_bbox.py --preset northsea_southbaltic --seconds 20 --max 200 --out logs\probe.jsonl

API Key:
  env var AISSTREAM_API_KEY
"""

from __future__ import annotations
import argparse, asyncio, json, os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple
import websockets

WS_URL = "wss://stream.aisstream.io/v0/stream"

PRESETS = {
    "northsea_southbaltic": [
        (-6.0, 50.0, 10.5, 62.0),   # Nordsee (grob)
        (8.5, 53.3, 20.5, 56.2),    # südliche Ostsee (grob)
    ],
    "northsea": [(-6.0, 50.0, 10.5, 62.0)],
    "southbaltic": [(8.5, 53.3, 20.5, 56.2)],
}

def bbox_to_aisstream(b: Tuple[float,float,float,float]) -> List[List[float]]:
    min_lon, min_lat, max_lon, max_lat = b
    return [[min_lat, min_lon], [max_lat, max_lon]]

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

async def main_async(api_key: str, boxes: List[Tuple[float,float,float,float]], seconds: int, max_n: int, out: Path, open_timeout: int):
    out.parent.mkdir(parents=True, exist_ok=True)
    bbox_payload = [bbox_to_aisstream(b) for b in boxes]
    sub = {
        "APIKey": api_key,
        "BoundingBoxes": bbox_payload,
        "FilterMessageTypes": ["PositionReport"],
    }

    deadline = asyncio.get_running_loop().time() + seconds
    n = 0

    print(f"[probe] connecting… (timeout={open_timeout}s) {utc_now_iso()}")
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_queue=4000,
        open_timeout=open_timeout,
    ) as ws:
        await ws.send(json.dumps(sub))
        print(f"[probe] subscribed. capturing up to {max_n} PositionReports for {seconds}s …")

        with open(out, "w", encoding="utf-8") as f:
            while n < max_n and asyncio.get_running_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                if isinstance(msg, dict) and "error" in msg:
                    print(f"[probe] server error: {msg.get('error')}")
                    break

                if msg.get("MessageType") != "PositionReport":
                    continue

                pr = (msg.get("Message") or {}).get("PositionReport") or {}
                ev = {
                    "ts_utc": utc_now_iso(),
                    "mmsi": str(pr.get("UserID","")),
                    "lat": pr.get("Latitude"),
                    "lon": pr.get("Longitude"),
                    "sog": pr.get("Sog"),
                    "cog": pr.get("Cog"),
                    "src": "aisstream_probe",
                }
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                n += 1

    print(f"[probe] wrote {n} lines -> {out}")
    if n == 0:
        print("[probe] Ergebnis: 0 Meldungen. Dann kommt in deiner Umgebung/Verbindung gerade nichts an (Proxy/Handshake/Throttling).")
    else:
        print("[probe] Ergebnis: Du bekommst Daten aus der BBox. Dann ist 0 Bytes im Watchlist-Log sehr wahrscheinlich: keine Watchlist-Treffer in der Zeit.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preset", choices=list(PRESETS.keys()), default="northsea_southbaltic")
    ap.add_argument("--bbox", nargs=4, type=float, action="append", metavar=("MIN_LON","MIN_LAT","MAX_LON","MAX_LAT"))
    ap.add_argument("--seconds", type=int, default=20)
    ap.add_argument("--max", dest="max_n", type=int, default=200)
    ap.add_argument("--out", default=r"logs\probe.jsonl")
    ap.add_argument("--open-timeout", type=int, default=60)
    args = ap.parse_args()

    api_key = (os.environ.get("AISSTREAM_API_KEY","") or "").strip()
    if not api_key:
        raise SystemExit("Missing AISSTREAM_API_KEY env var.")

    if args.bbox:
        boxes = [tuple(b) for b in args.bbox]  # type: ignore
    else:
        boxes = [tuple(b) for b in PRESETS[args.preset]]  # type: ignore

    asyncio.run(main_async(api_key, boxes, max(5,args.seconds), max(1,args.max_n), Path(args.out), max(5,args.open_timeout)))

if __name__ == "__main__":
    main()
