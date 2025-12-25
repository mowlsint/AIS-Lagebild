#!/usr/bin/env python3
"""
gur_shadowfleet_to_watchlist_v2.py

Builds a watchlist CSV from Ukraine GUR "Shadow Fleet" list.
Robust against:
- Pagination params
- Link formats (absolute/relative, with query strings)
- IMO/MMSI formatting (colon, newline, etc.)

Output columns (collector-compatible):
category,name,imo,mmsi,sanctioned,sanctions,note

Install once:
  python -m pip install requests beautifulsoup4

Run (default: first 3 pages = ~36 ships, good for a quick test):
  python gur_shadowfleet_to_watchlist_v2.py --out watchlist_shadowfleet.csv

Run full list (can be long, 756 ships as of Nov 2025):
  python gur_shadowfleet_to_watchlist_v2.py --out watchlist_shadowfleet.csv --max-pages 1000

Tip:
- If you only need a test set, use --max-pages 1 or --max-ships 50
"""

from __future__ import annotations
import argparse
import csv
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://war-sanctions.gur.gov.ua"
LIST_URL = BASE + "/en/transport/shadow-fleet"  # pagination uses ?page=N&per-page=12

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

SANCTION_TAGS = {
    "EU","USA","UK","Canada","Switzerland","Australia","New Zealand","Ukraine",
    "United States", "United Kingdom"
}

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def is_imo(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 7

def is_mmsi(s: str) -> bool:
    s = digits_only(s)
    return s.isdigit() and len(s) == 9

@dataclass
class Vessel:
    name: str = ""
    imo: str = ""
    mmsi: str = ""
    sanctions: str = ""
    sanctioned: bool = False
    url: str = ""

def fetch(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, headers=UA_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_shadowfleet_links(html: str) -> List[str]:
    # Match both relative and absolute URLs, optionally with querystrings
    # e.g. /en/transport/shadow-fleet/1010
    links = set(re.findall(r'https?://war-sanctions\.gur\.gov\.ua/en/transport/shadow-fleet/\d+|/en/transport/shadow-fleet/\d+', html))
    # Normalize to absolute
    out = []
    for href in links:
        out.append(urljoin(BASE, href))
    return sorted(set(out))

def parse_detail(html: str, url: str) -> Vessel:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Name: first H1 after # title
    name = ""
    h1 = soup.find(["h1"])
    if h1:
        name = (h1.get_text(" ", strip=True) or "").strip()

    # Robust patterns allow ":" or whitespace between label and digits
    imo = ""
    mmsi = ""
    m_imo = re.search(r"\bIMO\b\D{0,15}(\d{7})\b", text)
    if m_imo:
        imo = m_imo.group(1)
    m_mmsi = re.search(r"\bMMSI\b\D{0,15}(\d{9})\b", text)
    if m_mmsi:
        mmsi = m_mmsi.group(1)

    # Sanctions: look for known regime labels in anchor texts
    sanctions_found: Set[str] = set()
    for a in soup.find_all("a"):
        t = (a.get_text(" ", strip=True) or "").strip()
        if not t:
            continue
        if t in SANCTION_TAGS:
            sanctions_found.add(t)

    sanctions = ", ".join(sorted(sanctions_found))
    sanctioned = len(sanctions_found) > 0

    return Vessel(
        name=name,
        imo=imo if is_imo(imo) else "",
        mmsi=mmsi if is_mmsi(mmsi) else "",
        sanctions=sanctions,
        sanctioned=sanctioned,
        url=url,
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="watchlist_shadowfleet.csv")
    ap.add_argument("--per-page", type=int, default=12)
    ap.add_argument("--max-pages", type=int, default=3, help="Default 3 pages for quick test. Use 1000 for full.")
    ap.add_argument("--max-ships", type=int, default=0, help="0 = no limit")
    ap.add_argument("--sleep", type=float, default=0.25, help="Polite delay between detail page requests")
    args = ap.parse_args()

    session = requests.Session()

    all_links: List[str] = []
    for page in range(1, args.max_pages + 1):
        url = f"{LIST_URL}?page={page}&per-page={args.per_page}"
        html = fetch(session, url)
        links = extract_shadowfleet_links(html)
        # Remove self links (list page itself)
        links = [u for u in links if re.search(r"/shadow-fleet/\d+$", urlparse(u).path)]
        if not links:
            print(f"[gur] page {page}: no ship links found -> stopping")
            break
        new = [u for u in links if u not in all_links]
        all_links.extend(new)
        print(f"[gur] page {page}: +{len(new)} links (total {len(all_links)})")
        if args.max_ships and len(all_links) >= args.max_ships:
            all_links = all_links[:args.max_ships]
            break

    vessels: List[Vessel] = []
    for i, url in enumerate(all_links, 1):
        try:
            html = fetch(session, url)
            v = parse_detail(html, url)
            if not v.imo and not v.mmsi:
                # still keep it, but mark note
                v.url = url
            vessels.append(v)
        except Exception as e:
            print(f"[gur] detail error {url}: {e}")
        if args.sleep:
            time.sleep(args.sleep)
        if i % 25 == 0:
            print(f"[gur] parsed {i}/{len(all_links)}")

    # Write CSV
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["category","name","imo","mmsi","sanctioned","sanctions","note"])
        w.writeheader()
        for v in vessels:
            w.writerow({
                "category": "shadow_fleet",
                "name": v.name,
                "imo": v.imo,
                "mmsi": v.mmsi,
                "sanctioned": "true" if v.sanctioned else "false",
                "sanctions": v.sanctions,
                "note": v.url,
            })

    val_imo = sum(1 for v in vessels if v.imo)
    val_mmsi = sum(1 for v in vessels if v.mmsi)
    print(f"[gur] wrote {len(vessels)} rows to {args.out} (IMO: {val_imo}, MMSI: {val_mmsi})")

if __name__ == "__main__":
    main()
