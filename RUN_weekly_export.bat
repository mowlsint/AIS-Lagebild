@echo off
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
REM Example weekly export (adjust week)
python make_weekly_lagebild_from_bbox.py --in "logs\bbox_*.jsonl" --week 2025-W51 --shadowfleet watchlist_shadowfleet.csv --out "exports\lagebild_2025-W51.geojson"
pause
