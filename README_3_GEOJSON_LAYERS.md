# 3 GeoJSON Layer für uMap (ein/ausblendbar)

## Ziel
Drei getrennte Layer importieren und in uMap ein/ausblenden:
- Schattenflotte (GUR Watchlist)
- "Russische Schiffe" (MMSI MID=273)
- "Aus russischen Häfen" (Lookback RU-Port-BBox), aber **nur** wenn weder Schattenflotte noch MID273

## Installation
Kopiere diese zwei Dateien nach:
C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild\

- make_daily_3_geojson_layers_v1.py
- RUN_export_today_3_geojson_layers_v1.bat

## Nutzung
1) Sampler laufen lassen (Fenster offen): RUN_sampler_v4_...
2) Export erzeugen: Doppelklick auf RUN_export_today_3_geojson_layers_v1.bat

Outputs in exports\:
- lagebild_YYYY-MM-DD_shadowfleet.geojson
- lagebild_YYYY-MM-DD_ru_mid273.geojson
- lagebild_YYYY-MM-DD_from_russia_ports_excluding_shadow_mid273.geojson

## uMap
Karte bearbeiten -> Ebenen -> Import data.
Pro Datei "In eine neue Ebene importieren", Ebene benennen, Farbe pro Ebene setzen.
