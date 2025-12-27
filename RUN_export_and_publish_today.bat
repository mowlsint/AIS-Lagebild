@echo off
setlocal enabledelayedexpansion

REM === CONFIG ===
set DATA_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild
set EXPORT_DIR=%DATA_ROOT%\exports

set REPO_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo
set REPO_PUBLIC=%REPO_ROOT%\public

REM === DATE ===
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyy-MM-dd\")"') do set DATE=%%i

echo === Export %DATE% ===
cd /d "%DATA_ROOT%"

python make_daily_3_geojson_layers_v2.py --in "logs_v4\bbox_*.jsonl" --date %DATE% --tz UTC --shadowfleet watchlist_shadowfleet.csv --outdir exports --lookback-days 10
if errorlevel 1 (
  echo ERROR: Export failed.
  exit /b 1
)

echo === Sync repo (pull before copy) ===
cd /d "%REPO_ROOT%"
git pull --rebase
if errorlevel 1 (
  echo ERROR: git pull --rebase failed. Fix repo state and retry.
  exit /b 1
)

echo === Copy to repo public/ ===
if not exist "%REPO_PUBLIC%" (
  echo ERROR: Repo public folder not found: %REPO_PUBLIC%
  exit /b 1
)

copy /Y "%EXPORT_DIR%\lagebild_%DATE%_shadowfleet.geojson" "%REPO_PUBLIC%\live_shadowfleet.geojson" >nul
copy /Y "%EXPORT_DIR%\lagebild_%DATE%_ru_mid273.geojson" "%REPO_PUBLIC%\live_ru_mid273.geojson" >nul
copy /Y "%EXPORT_DIR%\lagebild_%DATE%_from_russia_ports_excluding_shadow_mid273.geojson" "%REPO_PUBLIC%\live_from_russia_ports_excl_shadow.geojson" >nul

echo === Git publish ===
git add "public\live_*.geojson"
git commit -m "Update live layers %DATE% %TIME%" >nul 2>&1
git push
if errorlevel 1 (
  echo ERROR: git push failed.
  exit /b 1
)

echo DONE.
exit /b 0
