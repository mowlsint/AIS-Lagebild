# Pipeline: Wöchentliches Lagebild (Nordsee + südliche Ostsee)

Du hast sauber gezeigt:
- AIS-Stream aus der BBox kommt an (Probe: 200/20s).
- Deine Shadow-Fleet-Watchlist hat in der Probe **0 Überschneidung** mit der BBox.

=> Watchlist-only Logging kann legitimerweise 0 Bytes bleiben.

## Lösung: Zweistufig

### Stufe 1 — Hintergrund-Sampler (BBox)
`aisstream_bbox_sampler.py`
- sammelt *alle* Schiffe in der BBox
- aber stark gedrosselt (Standard: 1 Punkt pro MMSI alle 30 Minuten)
- schreibt `logs/bbox_YYYY-MM-DD.jsonl`

Start:
```bat
python aisstream_bbox_sampler.py --outdir logs --preset northsea_southbaltic --min-seconds-per-ship 1800
```

### Stufe 2 — Wöchentlicher Export (GeoJSON)
`make_weekly_lagebild_from_bbox.py`
- liest die bbox-Logs für eine ISO-Woche (z.B. 2025-W51)
- markiert:
  - `shadow_fleet=true` wenn MMSI oder IMO in deiner GUR-Liste vorkommt
  - `ru_likely_mid273=true` wenn MMSI mit 273 beginnt (Indiz/Proxy)
- exportiert GeoJSON (Tracks + letzter Punkt pro Schiff)

Beispiel:
```bat
python make_weekly_lagebild_from_bbox.py --in "logs\bbox_*.jsonl" --week 2025-W51 --shadowfleet watchlist_shadowfleet.csv --out "exports\lagebild_2025-W51.geojson"
```

## uMap
In uMap: Daten → Importieren → GeoJSON auswählen (`exports\lagebild_2025-W51.geojson`)

Du kannst in uMap nach Properties filtern:
- `shadow_fleet = true`
- `ru_likely_mid273 = true`

## Tipp für Praxisbetrieb
- Lass den Sampler dauerhaft laufen (Task Scheduler beim Login/Startup).
- Export 1× pro Woche erzeugen und in uMap hochladen.
