@echo off
REM Export TODAY: FROM_RUSSIA but excluding Shadow Fleet + MID273 (v4)
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i
python make_daily_from_russia_excluding_shadow_mid273_v4.py --in "logs_v4\bbox_*.jsonl" --date %DATE% --tz UTC --out exports --shadowfleet watchlist_shadowfleet.csv --lookback-days 10
echo.
echo Output:
echo   exports\lagebild_%DATE%_from_russia_excluding_shadow_mid273.geojson
pause
