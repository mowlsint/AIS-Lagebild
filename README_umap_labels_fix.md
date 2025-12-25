# uMap zeigt keine Namen – warum, und Fix

## Warum fehlen Namen?
AIS-PositionReports enthalten oft **nur MMSI + Position**. Schiffsnamen kommen über `ShipStaticData`,
die nicht für jedes Schiff in jeder Session/Zeitscheibe ankommt. Deshalb bleibt `name` manchmal leer.

uMap zeigt außerdem **Labels nicht automatisch** – selbst wenn `name` in den Properties steht.

## Schnelllösung in uMap (ohne Code)
1) Layer bearbeiten (Stift)  
2) **Stil** → **Beschriftung (Label)** → Feld auswählen:
   - `label` (empfohlen) oder `name`
3) Speichern

## Fix im Export-Script (empfohlen)
Nutze `make_weekly_lagebild_from_bbox_v2.py`:
- setzt immer `properties.label` (Fallback: `MMSI 123456789`)
- zieht Shadowfleet-Namen aus `watchlist_shadowfleet.csv`, wenn Match vorliegt

### Befehl
```bat
cd "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python make_weekly_lagebild_from_bbox_v2.py --in "logs\bbox_*.jsonl" --week 2025-W51 --shadowfleet watchlist_shadowfleet.csv --out "exports\lagebild_2025-W51.geojson"
```

Dann in uMap neu importieren oder Layer ersetzen.
