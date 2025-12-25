@echo off
REM Daily export for TODAY, 3 layers (shadowfleet, russia_routes, ru_mid273).
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i
python make_daily_lagebild_from_bbox_v6.py --in "logs\bbox_*.jsonl" --date %DATE% --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 14
echo.
echo Output:
echo   exports\lagebild_%DATE%_shadowfleet.geojson
echo   exports\lagebild_%DATE%_russia_routes.geojson
echo   exports\lagebild_%DATE%_ru_mid273.geojson
pause
