@echo off
REM Export ONLY ships coming FROM Russia (Baltic + Arctic/North gateways) for TODAY.
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i
python make_daily_from_russia_from_bbox_v2.py --in "logs\bbox_*.jsonl" --date %DATE% --tz UTC --out exports --lookback-days 14
echo.
echo Output:
echo   exports\lagebild_%DATE%_from_russia.geojson
pause
