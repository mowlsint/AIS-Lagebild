# Wöchentliches AIS-Lagebild – Bausteine

## Dateien
- aisstream_collector.py  → sammelt 24/7 AIS-Events (OSINT, aisstream.io) in `logs/`
- make_weekly_lagebild_geojson.py → baut aus Logs + Watchlist ein Wochen-GeoJSON
- watchlist_template.csv → Vorlage, in Excel pflegen, dann als `watchlist.csv` speichern

## Installation
```bat
python -m pip install websockets
```

## Collector starten (im Projektordner)
```bat
python aisstream_collector.py --watchlist watchlist.csv --outdir logs --preset northsea_southbaltic
```

## Wochen-GeoJSON bauen (Beispiel)
```bat
python make_weekly_lagebild_geojson.py --watchlist watchlist.csv --in logs\*.jsonl --bbox -6 50 20.5 62 --week 2025-W51 --out lagebild_2025-W51.geojson
```
