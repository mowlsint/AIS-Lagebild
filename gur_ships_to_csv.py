#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download the GUR War & Sanctions ship list (marine vessels) and export to CSV.

Outputs columns:
- name
- imo
- detail_url
- source

This scraper uses the paginated list pages and extracts "Vessel name ... IMO ...." + detail page link.

Run:
  python gur_ships_to_csv.py --out public/watchlist_gur_ships.csv
"""

import argparse
import csv
import re
import sys
from urllib.parse import urljoin
import requests

BASE = "https://war-sanctions.gur.gov.ua"
START = "https://war-sanctions.gur.gov.ua/en/transport/ships"

RE_CARD = re.compile(r"Vessel name\s+(.+?)\s+IMO\s+(\d{7})", re.IGNORECASE | re.DOTALL)

def fetch(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def extract_from_list_page(html: str):
    """
    Returns list of dicts {name, imo, detail_url}
    """
    out = []
    # Cards contain links like /en/transport/ships/903 etc
    # We find all such links, then around them we look for "Vessel name ... IMO ...."
    # Simple but robust approach: search globally for the card text pattern and then backtrack link.
    # We'll do: find all matches of the pattern, then find nearest preceding href to a /transport/ships/<id>.
    matches = list(RE_CARD.finditer(html))
    if not matches:
        return out

    # Find all detail links with positions
    link_re = re.compile(r'href="(/en/transport/ships/\d+)"')
    links = [(m.start(), m.group(1)) for m in link_re.finditer(html)]

    for m in matches:
        name = " ".join(m.group(1).split())
        imo = m.group(2)

        # nearest link before this match
        detail = None
        for pos, href in reversed(links):
            if pos < m.start():
                detail = urljoin(BASE, href)
                break

        out.append({"name": name, "imo": imo, "detail_url": detail or ""})
    return out

def find_last_page(html: str) -> int:
    # Pagination shows links like ?page=8&per-page=12 (ids 27..35 in your view)
    pages = re.findall(r'page=(\d+)', html)
    nums = [int(p) for p in pages] if pages else [1]
    return max(nums) if nums else 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="public/watchlist_gur_ships.csv")
    ap.add_argument("--per-page", type=int, default=12)
    args = ap.parse_args()

    first_html = fetch(f"{START}?page=1&per-page={args.per_page}")
    last_page = find_last_page(first_html)

    seen = set()
    rows = []

    for page in range(1, last_page + 1):
        url = f"{START}?page={page}&per-page={args.per_page}"
        html = first_html if page == 1 else fetch(url)
        items = extract_from_list_page(html)
        for it in items:
            key = it["imo"]
            if key in seen:
                continue
            seen.add(key)
            it["source"] = "GUR War & Sanctions – Marine vessels"
            rows.append(it)
        print(f"[gur] page {page}/{last_page}: +{len(items)} items, total unique={len(rows)}", file=sys.stderr)

    # write CSV
    out_path = args.out
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "imo", "detail_url", "source"])
        w.writeheader()
        w.writerows(rows)

    print(f"[gur] wrote {len(rows)} rows -> {out_path}")

if __name__ == "__main__":
    main()
