@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM  CONFIG
REM =========================================================
set "REPO=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo"

REM Wenn "git" im Task-Kontext nicht gefunden wird, hier den vollen Pfad setzen:
REM set "GIT=C:\Program Files\Git\bin\git.exe"
set "GIT=git"

REM Python ggf. fest setzen (optional):
REM set "PY=C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"
set "PY=python"

REM Commit Message
set "MSG=Automated publish: %date% %time%"

REM =========================================================
REM  ALWAYS RUN INSIDE REPO
REM =========================================================
cd /d "%REPO%" || exit /b 1

REM Optional: kleines Lebenszeichen
echo [%date% %time%] Started in: %CD%

REM =========================================================
REM  UPDATE FROM REMOTE (optional)
REM =========================================================
%GIT% pull --rebase
if errorlevel 1 exit /b 1

REM =========================================================
REM  RUN YOUR EXPORT PIPELINE
REM  (Passe diese Zeilen an deine echte Pipeline an)
REM =========================================================

REM Beispiel: falls du BATs kaskadierst:
REM call RUN_export_today_3_geojson_layers_v2.bat
REM if errorlevel 1 exit /b 1

REM Oder direkt Python:
REM %PY% make_daily_lagebild_from_bbox_v6.py
REM if errorlevel 1 exit /b 1

REM >>> DEIN bisheriger Export-Kram kommt hier rein <<<
REM call RUN_publish_live_layers_today.bat
REM if errorlevel 1 exit /b 1


REM =========================================================
REM  STAGE ONLY WHAT SHOULD BE PUBLISHED
REM  (kein snapshots/, kein logs/, kein _tmp_cam/, keine test.jpg etc.)
REM =========================================================

REM 1) Watchlists, wenn du willst:
%GIT% add -f watchlist_shadowfleet.csv

REM 2) Public Outputs (GeoJSON/CSV/JSON) – passe Muster an deine echten Dateien an:
%GIT% add -f public\*.geojson
%GIT% add -f public\*.csv
%GIT% add -f public\*.json

REM 3) Falls du Unterordner unter public nutzt:
%GIT% add -f public\**\*.geojson
%GIT% add -f public\**\*.csv
%GIT% add -f public\**\*.json

REM Wenn du bestimmte Dateien NIE publishen willst, regel das über .gitignore.
REM (Du hast das ja schon begonnen.)

REM =========================================================
REM  COMMIT+PUSH ONLY IF THERE ARE STAGED CHANGES
REM =========================================================
%GIT% diff --cached --quiet
if %errorlevel%==0 (
  echo [%date% %time%] No changes to commit.
  exit /b 0
)

%GIT% commit -m "%MSG%"
if errorlevel 1 exit /b 1

%GIT% push
if errorlevel 1 exit /b 1

echo [%date% %time%] Done.
exit /b 0

@echo off
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo" || exit /b 1
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
