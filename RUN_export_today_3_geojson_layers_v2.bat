@echo off
REM Export 3 getrennte GeoJSON-Layer (mit properties.display)
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i
python make_daily_3_geojson_layers_v2.py --in "logs_v4\bbox_*.jsonl" --date %DATE% --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 10
echo.
echo Fertig. Dateien im Ordner exports\
pause
