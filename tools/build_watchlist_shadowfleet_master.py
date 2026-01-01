#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_watchlist_shadowfleet_master.py

Builds a watchlist in the exact format of your watchlist_shadowfleet.csv:
Columns: name, imo, detail_url, source

Sources (official / primary):
- UA GUR War & Sanctions: Shadow Fleet list + detail pages
- UK Sanctions List (CSV)
- EU designated vessels (DMA export of Annex XLII) (CSV)
- US OFAC SDN (CSV)

Outputs:
1) watchlist_shadowfleet_master.csv  (your format: name, imo, detail_url, source)
2) watchlist_shadowfleet_master_matching.csv (optional, includes mmsi too)

Dedupe priority: IMO > MMSI > normalized name.
"""

from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


UA_LIST_URL = "https://war-sanctions.gur.gov.ua/en/transport/shadow-fleet"
UA_BASE = "https://war-sanctions.gur.gov.ua"

UK_CSV_URL = "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.csv"
US_SDN_CSV_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"

EU_DMA_PAGE = "https://www.dma.dk/growth-and-framework-conditions/maritime-sanctions/sanctions-against-russia-and-belarus/eu-vessel-designations"


@dataclass
class Vessel:
    name: str = ""
    imo: str = ""
    mmsi: str = ""
    detail_url: str = ""
    source: str = ""


def digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def is_imo(s: str) -> bool:
    s = digits(s)
    return len(s) == 7

def is_mmsi(s: str) -> bool:
    s = digits(s)
    return len(s) == 9

def norm_name(s: str) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", "", s)
    return s

def dedupe_key(v: Vessel) -> Tuple[str, str]:
    if is_imo(v.imo):
        return ("IMO", digits(v.imo))
    if is_mmsi(v.mmsi):
        return ("MMSI", digits(v.mmsi))
    return ("NAME", norm_name(v.name))


def http_get(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.text


# ---------------------------
# UA (GUR) Shadow Fleet scrape
# ---------------------------
def ua_extract_detail(session: requests.Session, detail_url: str) -> Vessel:
    html = http_get(session, detail_url)
    soup = BeautifulSoup(html, "lxml")

    # Name: usually header
    h1 = soup.find(["h1", "h2"])
    name = h1.get_text(strip=True) if h1 else ""

    # Parse text for IMO/MMSI (robust)
    text = soup.get_text("\n", strip=True)

    imo = ""
    m = re.search(r"\bIMO\b\s*([0-9]{7})\b", text)
    if m:
        imo = m.group(1)

    mmsi = ""
    m = re.search(r"\bMMSI\b\s*([0-9]{9})\b", text)
    if m:
        mmsi = m.group(1)

    return Vessel(
        name=name,
        imo=imo,
        mmsi=mmsi,
        detail_url=detail_url,
        source="GUR War & Sanctions – Marine vessels"
    )


def ua_collect_all(session: requests.Session, per_page: int = 48, sleep_s: float = 0.12) -> List[Vessel]:
    """
    Iterate paginated list, collect vessel detail URLs, then scrape each detail page.
    Stops when a page yields no new detail links.
    """
    seen: set[str] = set()
    out: List[Vessel] = []
    page = 1

    while True:
        list_url = f"{UA_LIST_URL}?page={page}&per-page={per_page}"
        html = http_get(session, list_url)
        soup = BeautifulSoup(html, "lxml")

        detail_links: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/en/transport/shadow-fleet/" in href and re.search(r"/en/transport/shadow-fleet/\d+$", href):
                detail_links.append(urljoin(UA_BASE, href))

        detail_links = sorted(set(detail_links))
        new_links = [u for u in detail_links if u not in seen]

        if not new_links:
            break

        for u in new_links:
            seen.add(u)
            try:
                out.append(ua_extract_detail(session, u))
            except Exception as e:
                out.append(Vessel(detail_url=u, source="GUR War & Sanctions – Marine vessels", name="", imo="", mmsi="",))
            time.sleep(sleep_s)

        page += 1

    return out


# ---------------------------
# UK sanctions (CSV)
# ---------------------------
def uk_collect(session: requests.Session) -> List[Vessel]:
    csv_text = http_get(session, UK_CSV_URL)
    r = csv.DictReader(csv_text.splitlines())
    out: List[Vessel] = []

    for row in r:
        t = (row.get("Type") or row.get("type") or "").strip().lower()
        if t not in ("ship", "vessel"):
            continue

        name = (row.get("Name") or row.get("name") or "").strip()

        # IMO often in "OtherInformation"/remarks; scan full row blob
        blob = " | ".join([str(v) for v in row.values() if v])
        imo = ""
        m = re.search(r"\bIMO\b[: ]*([0-9]{7})\b", blob, flags=re.I)
        if m:
            imo = m.group(1)

        out.append(Vessel(
            name=name,
            imo=imo,
            mmsi="",
            detail_url="",
            source="UK Sanctions List (FCDO)"
        ))

    return out


# ---------------------------
# US OFAC SDN (CSV)
# ---------------------------
def us_collect(session: requests.Session) -> List[Vessel]:
    csv_text = http_get(session, US_SDN_CSV_URL)
    r = csv.DictReader(csv_text.splitlines())
    out: List[Vessel] = []

    for row in r:
        if (row.get("SDN Type") or "").strip().lower() != "vessel":
            continue

        name = (row.get("SDN Name") or "").strip()
        remarks = row.get("Remarks") or ""

        imo = ""
        m = re.search(r"\bIMO\s*([0-9]{7})\b", remarks)
        if m:
            imo = m.group(1)

        mmsi = ""
        m = re.search(r"\bMMSI\s*([0-9]{9})\b", remarks)
        if m:
            mmsi = m.group(1)

        out.append(Vessel(
            name=name,
            imo=imo,
            mmsi=mmsi,
            detail_url="",
            source="US OFAC SDN"
        ))

    return out


# ---------------------------
# EU designated vessels (DMA export page -> CSV link -> parse)
# ---------------------------
def eu_collect(session: requests.Session) -> List[Vessel]:
    html = http_get(session, EU_DMA_PAGE)
    soup = BeautifulSoup(html, "lxml")

    csv_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # pick a csv in page; DMA offers "Import versions" (CSV/JSON/XML)
        if href.lower().endswith(".csv"):
            csv_link = urljoin("https://www.dma.dk", href)
            break

    if not csv_link:
        return [Vessel(source="EU Annex XLII (DMA export)", detail_url=EU_DMA_PAGE)]

    csv_text = http_get(session, csv_link)
    r = csv.DictReader(csv_text.splitlines())
    out: List[Vessel] = []

    for row in r:
        name = (row.get("Vessel name") or row.get("Vessel Name") or row.get("vessel name") or "").strip()
        imo = digits(row.get("IMO nr.") or row.get("IMO") or row.get("imo") or "")
        if not is_imo(imo):
            imo = ""
        out.append(Vessel(
            name=name,
            imo=imo,
            mmsi="",
            detail_url=csv_link,
            source="EU Annex XLII (DMA export)"
        ))

    return out


# ---------------------------
# Merge / write
# ---------------------------
def merge(vessels: List[Vessel]) -> List[Vessel]:
    merged: Dict[Tuple[str, str], Vessel] = {}

    for v in vessels:
        v.imo = digits(v.imo)
        v.mmsi = digits(v.mmsi)
        k = dedupe_key(v)
        if not k[1]:
            continue

        if k not in merged:
            merged[k] = v
            continue

        m = merged[k]
        # fill gaps
        if not m.name and v.name:
            m.name = v.name
        if not m.imo and v.imo:
            m.imo = v.imo
        if not m.mmsi and v.mmsi:
            m.mmsi = v.mmsi
        if not m.detail_url and v.detail_url:
            m.detail_url = v.detail_url

        # merge sources
        if v.source and v.source not in m.source:
            m.source = f"{m.source}; {v.source}" if m.source else v.source

    return list(merged.values())


def write_watchlist_csv(path: str, rows: List[Vessel]) -> None:
    cols = ["name", "imo", "detail_url", "source"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for v in sorted(rows, key=lambda x: (x.imo or "9999999", norm_name(x.name))):
            w.writerow({
                "name": (v.name or "").strip(),
                "imo": digits(v.imo),
                "detail_url": (v.detail_url or "").strip(),
                "source": (v.source or "").strip(),
            })


def write_matching_csv(path: str, rows: List[Vessel]) -> None:
    # optional helper for your matching code: has mmsi too
    cols = ["name", "imo", "mmsi", "detail_url", "source"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for v in sorted(rows, key=lambda x: (x.imo or "9999999", x.mmsi or "999999999", norm_name(x.name))):
            w.writerow({
                "name": (v.name or "").strip(),
                "imo": digits(v.imo),
                "mmsi": digits(v.mmsi),
                "detail_url": (v.detail_url or "").strip(),
                "source": (v.source or "").strip(),
            })


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="watchlist_shadowfleet_master.csv")
    ap.add_argument("--out-matching", default="watchlist_shadowfleet_master_matching.csv")
    ap.add_argument("--sleep", type=float, default=0.12)
    ap.add_argument("--ua-per-page", type=int, default=48)
    args = ap.parse_args()

    s = requests.Session()
    s.headers.update({"User-Agent": "AIS-Lagebild/1.0 (watchlist builder)"})

    all_rows: List[Vessel] = []

    print("[1/4] UA GUR shadow fleet…")
    ua = ua_collect_all(s, per_page=args.ua_per_page, sleep_s=args.sleep)
    print(f"  UA rows: {len(ua)}")
    all_rows.extend(ua)

    print("[2/4] UK Sanctions List…")
    uk = uk_collect(s)
    print(f"  UK vessel rows: {len(uk)}")
    all_rows.extend(uk)

    print("[3/4] EU Annex XLII (DMA export)…")
    eu = eu_collect(s)
    print(f"  EU rows: {len(eu)}")
    all_rows.extend(eu)

    print("[4/4] US OFAC SDN (vessels)…")
    us = us_collect(s)
    print(f"  US vessel rows: {len(us)}")
    all_rows.extend(us)

    merged = merge(all_rows)
    print(f"[merge] unique keys: {len(merged)}")

    write_watchlist_csv(args.out, merged)
    write_matching_csv(args.out_matching, merged)
    print(f"[done] wrote:\n  {args.out}\n  {args.out_matching}")


if __name__ == "__main__":
    main()
