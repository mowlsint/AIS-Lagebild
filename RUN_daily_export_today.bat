@echo off
REM Runs DAILY export for "today" (local machine date) and creates two GeoJSON layers.

cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i

python make_daily_lagebild_from_bbox_v3.py --in "logs\bbox_*.jsonl" --date %DATE% --tz Europe/Berlin --shadowfleet watchlist_shadowfleet.csv --outdir exports

echo.
echo Done. Output:
echo   exports\lagebild_%DATE%_shadowfleet.geojson
echo   exports\lagebild_%DATE%_ru_mid273.geojson
pause
