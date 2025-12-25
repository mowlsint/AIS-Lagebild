# Tägliches Lagebild + 2 Farben (uMap)

## Was du bekommst
Du erzeugst pro Tag **zwei GeoJSON-Dateien**:
1) `lagebild_YYYY-MM-DD_shadowfleet.geojson`  -> Farbe 1 (z.B. Rot)
2) `lagebild_YYYY-MM-DD_ru_mid273.geojson`    -> Farbe 2 (z.B. Blau)

Regel:
- Wenn ein Schiff **Shadow Fleet** und **MID273** ist, landet es **nur** in Shadow Fleet.

## Warum "russische Flagge" nur Proxy ist
AIS sendet kein Flag. Wir nutzen **MMSI MID=273** als pragmatisches Indiz.
Das ist nicht 100% identisch mit Flagge, aber für ein OSINT-Lagebild oft brauchbar.

## Welche Sanktionsliste nutzt du?
Für Shadow Fleet Tagging nutzt du:
- **GUR war-sanctions (Ukraine) – Shadow Fleet** als OSINT-Quelle/Index.
- Feld `sanctions_regimes` im Output kommt **aus der GUR-Seite** (EU/UK/USA/… wie dort gelistet).

Wenn du statt dessen "amtlich" willst, wäre der nächste Schritt ein separater Verifikations-Job gegen:
- EU Consolidated Financial Sanctions List
- UK OFSI Consolidated List
- US OFAC SDN List
(technisch machbar, aber Matching über IMO/MMSI ist je nach Liste/Eintrag nicht immer trivial)

## uMap: zwei Farben
Importiere die beiden Dateien als **zwei Layer** und setze je Layer die Farbe.
Beschriftung: Feld `label`.

## Export-Befehl (manuell)
```bat
python make_daily_lagebild_from_bbox_v3.py --in "logs\bbox_*.jsonl" --date 2025-12-20 --tz Europe/Berlin --shadowfleet watchlist_shadowfleet.csv --outdir exports
```

## Export per Doppelklick (heute)
`RUN_daily_export_today.bat`
