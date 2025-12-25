#!/usr/bin/env python3
"""
fix_watchlist_csv.py

Normalizes a watchlist CSV so that aisstream_collector.py can read it reliably.

Fixes:
- Encoding issues (UTF-16 / UTF-8 BOM)
- Delimiter issues (; vs ,)
- Header case issues (MMSI vs mmsi, IMO vs imo, etc.)
- Strips non-digits from IMO/MMSI fields and validates lengths

Usage (Windows cmd, inside your project folder):
  python fix_watchlist_csv.py watchlist.csv watchlist_fixed.csv

Afterwards:
  copy /Y watchlist_fixed.csv watchlist.csv
"""

from __future__ import annotations
import csv
import io
import re
import sys
from pathlib import Path
from typing import Dict, List

FIELDS = ["category","name","imo","mmsi","sanctioned","sanctions","note"]

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def is_imo(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 7

def is_mmsi(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 9

def detect_encoding(data: bytes) -> str:
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        return "utf-16"
    if data.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"

def detect_delimiter(text: str) -> str:
    first_line = text.splitlines()[0] if text.splitlines() else ""
    # crude but effective: choose the one that appears more in header line
    return ";" if first_line.count(";") > first_line.count(",") else ","

def norm_header(h: str) -> str:
    h = (h or "").strip().lower()
    h = h.replace(" ", "").replace("-", "_")
    # common variants
    if h in ("mmsi_number","mmsino","mmsiid","userid"):
        return "mmsi"
    if h in ("imo_number","imonumber","imo_no","imoid"):
        return "imo"
    if h in ("vesselname","shipname"):
        return "name"
    return h

def main():
    if len(sys.argv) < 3:
        print("Usage: python fix_watchlist_csv.py INPUT.csv OUTPUT.csv")
        raise SystemExit(2)

    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])

    data = inp.read_bytes()
    enc = detect_encoding(data)
    text = data.decode(enc, errors="ignore")
    delim = detect_delimiter(text)

    f = io.StringIO(text)
    reader = csv.DictReader(f, delimiter=delim)

    # normalize headers: DictReader uses fieldnames list
    if reader.fieldnames is None:
        print("[!] No header row found.")
        raise SystemExit(1)

    header_map = {orig: norm_header(orig) for orig in reader.fieldnames}

    rows: List[Dict[str,str]] = []
    n_imo = 0
    n_mmsi = 0

    for row in reader:
        norm_row = {FIELDS[i]: "" for i in range(len(FIELDS))}
        for k, v in row.items():
            nk = header_map.get(k, norm_header(k))
            if nk in norm_row:
                norm_row[nk] = (v or "").strip()

        # clean IMO/MMSI
        norm_row["imo"] = digits_only(norm_row.get("imo",""))
        norm_row["mmsi"] = digits_only(norm_row.get("mmsi",""))

        if is_imo(norm_row["imo"]):
            n_imo += 1
        else:
            norm_row["imo"] = ""

        if is_mmsi(norm_row["mmsi"]):
            n_mmsi += 1
        else:
            norm_row["mmsi"] = ""

        rows.append(norm_row)

    # Write clean CSV (UTF-8, comma)
    with open(out, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[+] Read {len(rows)} rows from {inp.name} (encoding={enc}, delimiter='{delim}')")
    print(f"[+] Valid IMO rows:  {n_imo}")
    print(f"[+] Valid MMSI rows: {n_mmsi}")
    print(f"[+] Wrote normalized CSV: {out.name}")

if __name__ == "__main__":
    main()
