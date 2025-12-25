#!/usr/bin/env python3
"""
gur_shadowfleet_to_watchlist.py

Export the Ukrainian GUR "shadow fleet" list to a watchlist.csv
that works with aisstream_collector.py / make_weekly_lagebild_geojson.py.

What it does
- Crawls the GUR shadow-fleet list pages
- Opens each ship detail page
- Extracts: name, IMO, MMSI, sanctions (as text list)
- Writes CSV with columns:
  category,name,imo,mmsi,sanctioned,sanctions,note

Run (Windows cmd, from your project folder):
  python -m pip install requests beautifulsoup4
  python gur_shadowfleet_to_watchlist.py --out watchlist_shadowfleet.csv

Then merge into your main watchlist:
  python merge_watchlists.py --out watchlist.csv watchlist_shadowfleet.csv watchlist_russian.csv

Notes
- Be polite: this script has a small delay between requests.
- This is best-effort parsing; if a page layout changes, extraction may need minor tweaks.
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

GUR_BASE = "https://war-sanctions.gur.gov.ua"
LIST_URL = GUR_BASE + "/en/transport/shadow-fleet?page={page}&per-page={per_page}"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AIS-Lagebild/1.0; +https://example.invalid)",
    "Accept-Language": "en,en-US;q=0.8,de;q=0.6",
}

@dataclass
class Vessel:
    name: str
    imo: str
    mmsi: str
    sanctions: List[str]
    url: str

def http_get(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, headers=UA_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_ship_links_from_list(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"^/en/transport/shadow-fleet/\d+$", href):
            links.add(urljoin(GUR_BASE, href))
    return sorted(links)

def parse_ship_page(html: str, url: str) -> Vessel:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    name = (h1.get_text(strip=True) if h1 else "").strip() or "UNKNOWN"

    page_text = soup.get_text("\n", strip=True)

    imo = ""
    m = re.search(r"\bIMO\s+(\d{7})\b", page_text)
    if m:
        imo = m.group(1)

    mmsi = ""
    m = re.search(r"\bMMSI\s+(\d{9})\b", page_text)
    if m:
        mmsi = m.group(1)

    sanctions: List[str] = []
    # Try to find a "Sanctions" block and collect link texts inside it
    label_node = soup.find(string=re.compile(r"^Sanctions\s*$"))
    if label_node:
        block = label_node.parent
        stop_labels = {
            "Sanctions lifted", "Cases of AIS shutdown", "Visited ports",
            "Calling at russian ports", "Available additional information",
            "Build year", "Web Resources", "Justification"
        }
        cur = block
        for _ in range(120):
            cur = cur.find_next()
            if not cur:
                break
            t = cur.get_text(" ", strip=True)
            if t in stop_labels:
                break
            for a in cur.find_all("a", href=True):
                txt = a.get_text(" ", strip=True)
                if txt and txt not in sanctions:
                    sanctions.append(txt)

    return Vessel(name=name, imo=imo, mmsi=mmsi, sanctions=sanctions, url=url)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="watchlist_shadowfleet.csv")
    ap.add_argument("--pages", type=int, default=50, help="How many list pages to crawl (safe upper bound).")
    ap.add_argument("--per-page", type=int, default=12)
    ap.add_argument("--sleep", type=float, default=0.6, help="Delay between HTTP requests (seconds).")
    args = ap.parse_args()

    session = requests.Session()

    ship_links: List[str] = []
    for page in range(1, args.pages + 1):
        url = LIST_URL.format(page=page, per_page=args.per_page)
        html = http_get(session, url)
        links = extract_ship_links_from_list(html)
        if not links:
            break
        ship_links.extend(links)
        time.sleep(args.sleep)

    ship_links = sorted(set(ship_links))
    print(f"[+] Found {len(ship_links)} ship detail pages on GUR.")

    vessels: List[Vessel] = []
    for i, url in enumerate(ship_links, 1):
        try:
            html = http_get(session, url)
            v = parse_ship_page(html, url)
            # Keep only ships with at least IMO or MMSI (collector needs MMSI; exporter can use IMO)
            if not v.imo and not v.mmsi:
                continue
            vessels.append(v)
        except Exception as e:
            print(f"[!] Failed {url}: {e}")
        time.sleep(args.sleep)
        if i % 50 == 0:
            print(f"[+] Parsed {i}/{len(ship_links)}...")

    # Write CSV
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category","name","imo","mmsi","sanctioned","sanctions","note"])
        for v in vessels:
            sanctioned = "true" if len(v.sanctions) > 0 else "false"
            w.writerow([
                "shadow_fleet",
                v.name,
                v.imo,
                v.mmsi,
                sanctioned,
                "; ".join(v.sanctions),
                f"GUR shadow-fleet list: {v.url}",
            ])

    print(f"[+] Wrote {args.out} with {len(vessels)} vessels.")
    print("[i] Reminder: aisstream_collector requires MMSI to match in real time. If some rows have blank MMSI, they won't be logged by the collector, but can still be used for weekly outputs if you obtain MMSI later.")

if __name__ == "__main__":
    main()
