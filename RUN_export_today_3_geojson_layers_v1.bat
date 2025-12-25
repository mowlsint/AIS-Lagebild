@echo off
REM Export 3 getrennte GeoJSON-Layer (shadowfleet / ru_mid273 / from_russia_ports excl shadow+mid273)
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i
python make_daily_3_geojson_layers_v1.py --in "logs_v4\bbox_*.jsonl" --date %DATE% --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 14
echo.
echo Fertig. Dateien im Ordner exports\
pause
