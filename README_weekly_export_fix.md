# Fix für make_weekly_lagebild_from_bbox.py

Du hattest:
`TypeError: unsupported operand type(s) for +: 'datetime.datetime' and 'NoneType'`

Ursache: Bug in `week_bounds()`.

## Lösung
1) Lade diese Datei herunter:
- `make_weekly_lagebild_from_bbox_FIXED.py`

2) Kopiere sie in deinen Projektordner und überschreibe die alte Datei:
`C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild\make_weekly_lagebild_from_bbox.py`

(Also: umbenennen/überschreiben, so dass die Datei exakt so heißt:
`make_weekly_lagebild_from_bbox.py`)

## Dann Export erneut ausführen
```bat
cd "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python make_weekly_lagebild_from_bbox.py --in "logs\bbox_*.jsonl" --week 2025-W51 --shadowfleet watchlist_shadowfleet.csv --out "exports\lagebild_2025-W51.geojson"
```
