#!/usr/bin/env python3
"""
merge_watchlists.py

Merge multiple watchlist CSV files into one, de-duplicating by IMO (preferred) or MMSI.

Usage:
  python merge_watchlists.py --out watchlist.csv watchlist_shadowfleet.csv watchlist_russian.csv

Input columns expected (minimum): category,name,imo,mmsi,sanctioned,sanctions,note
"""

from __future__ import annotations
import argparse, csv
from typing import Dict, List, Tuple

FIELDS = ["category","name","imo","mmsi","sanctioned","sanctions","note"]

def key_of(row: Dict[str,str]) -> str:
    imo = (row.get("imo") or "").strip()
    mmsi = (row.get("mmsi") or "").strip()
    if imo.isdigit() and len(imo)==7:
        return f"IMO:{imo}"
    if mmsi.isdigit() and len(mmsi)==9:
        return f"MMSI:{mmsi}"
    return ""

def merge_rows(existing: Dict[str,str], new: Dict[str,str]) -> Dict[str,str]:
    # Prefer non-empty values; keep category if existing empty
    out = dict(existing)
    for k in FIELDS:
        if not out.get(k) and new.get(k):
            out[k] = new[k]
    # Merge sanctions/note if both present
    if existing.get("sanctions") and new.get("sanctions") and existing["sanctions"] != new["sanctions"]:
        out["sanctions"] = "; ".join(sorted(set([s.strip() for s in (existing["sanctions"]+";"+new["sanctions"]).split(";") if s.strip()])))
    if existing.get("note") and new.get("note") and new["note"] not in existing["note"]:
        out["note"] = existing["note"] + " | " + new["note"]
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("inputs", nargs="+")
    args = ap.parse_args()

    merged: Dict[str, Dict[str,str]] = {}

    for fp in args.inputs:
        with open(fp, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                row = {k: (row.get(k) or "").strip() for k in FIELDS}
                k = key_of(row)
                if not k:
                    continue
                if k in merged:
                    merged[k] = merge_rows(merged[k], row)
                else:
                    merged[k] = row

    # Sort: shadow_fleet first, then russian_flagged, then name
    cat_order = {"shadow_fleet": 0, "russian_flagged": 1}
    rows = list(merged.values())
    rows.sort(key=lambda r: (cat_order.get(r.get("category",""), 9), r.get("name","")))

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print(f"[+] Wrote {args.out} with {len(rows)} unique vessels.")

if __name__ == "__main__":
    main()
