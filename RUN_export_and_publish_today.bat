@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM  RUN_export_and_publish_today.bat
REM  - erzeugt die 3 Live-GeoJSONs direkt im Repo/public
REM  - commit + push nur wenn sich etwas geändert hat
REM =========================================================

REM === CONFIG ===
set "DATA_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
set "REPO_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo"
set "REPO_PUBLIC=%REPO_ROOT%\public"

REM Optional: falls git im Task-Kontext nicht gefunden wird:
REM set "GIT=C:\Program Files\Git\bin\git.exe"
set "GIT=git"

REM Optional: falls python im Task-Kontext nicht gefunden wird:
REM set "PY=C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"
set "PY=python"

REM === DATE (UTC) ===
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString(\"yyyy-MM-dd\")"') do set "DATE=%%i"

REM === SANITY ===
if not exist "%DATA_ROOT%" (
  echo ERROR: DATA_ROOT not found: %DATA_ROOT%
  exit /b 1
)
if not exist "%REPO_ROOT%" (
  echo ERROR: REPO_ROOT not found: %REPO_ROOT%
  exit /b 1
)
if not exist "%REPO_PUBLIC%" (
  echo ERROR: REPO_PUBLIC not found: %REPO_PUBLIC%
  exit /b 1
)

echo ========================================================
echo [%date% %time%] Export + Publish starting
echo   DATA_ROOT   = %DATA_ROOT%
echo   REPO_ROOT   = %REPO_ROOT%
echo   REPO_PUBLIC = %REPO_PUBLIC%
echo   DATE (UTC)  = %DATE%
echo ========================================================

REM =========================================================
REM  1) GENERATE GEOJSONS -> DIRECTLY INTO REPO/public
REM =========================================================
cd /d "%DATA_ROOT%" || exit /b 1

REM Erwartet Input-Logs hier:
REM   %DATA_ROOT%\logs_v4\bbox_*.jsonl
REM und schreibt Output nach %REPO_PUBLIC%
%PY% make_daily_3_geojson_layers_v2.py ^
  --in "logs_v4\bbox_*.jsonl" ^
  --date %DATE% ^
  --tz UTC ^
  --shadowfleet watchlist_shadowfleet.csv ^
  --outdir "%REPO_PUBLIC%" ^
  --lookback-days 10

if errorlevel 1 (
  echo ERROR: Export failed.
  exit /b 1
)

REM =========================================================
REM  2) ENSURE "LIVE" FILENAMES EXIST (RENAME IF NEEDED)
REM  (Falls dein Python-Skript bereits live_*.geojson schreibt,
REM   bleiben diese Befehle wirkungslos, wenn Quelle nicht existiert.)
REM =========================================================
REM Optional: Wenn dein Script date-dated Namen erzeugt, mappe hier um.
REM Beispiel (nur wenn die Quellen existieren):
if exist "%REPO_PUBLIC%\lagebild_%DATE%_shadowfleet.geojson" (
  copy /Y "%REPO_PUBLIC%\lagebild_%DATE%_shadowfleet.geojson" "%REPO_PUBLIC%\live_shadowfleet.geojson" >nul
)
if exist "%REPO_PUBLIC%\lagebild_%DATE%_ru_mid273.geojson" (
  copy /Y "%REPO_PUBLIC%\lagebild_%DATE%_ru_mid273.geojson" "%REPO_PUBLIC%\live_ru_mid273.geojson" >nul
)
if exist "%REPO_PUBLIC%\lagebild_%DATE%_from_russia_ports_excluding_shadow_mid273.geojson" (
  copy /Y "%REPO_PUBLIC%\lagebild_%DATE%_from_russia_ports_excluding_shadow_mid273.geojson" "%REPO_PUBLIC%\live_from_russia_ports_excl_shadow.geojson" >nul
)

REM =========================================================
REM  3) GIT PUBLISH
REM =========================================================
cd /d "%REPO_ROOT%" || exit /b 1

REM Pull (optional, aber gut wenn mehrere Prozesse schreiben)
%GIT% pull --rebase
if errorlevel 1 (
  echo ERROR: git pull --rebase failed.
  exit /b 1
)

REM Stage only what should be published (GeoJSON in public)
%GIT% add -f "public\*.geojson"
%GIT% add -f "public\**\*.geojson"

REM Commit+push only if there are staged changes
%GIT% diff --cached --quiet
if %errorlevel%==0 (
  echo [%date% %time%] No changes to commit.
  exit /b 0
)

set "MSG=Automated publish: %DATE% %TIME%"
%GIT% commit -m "%MSG%"
if errorlevel 1 (
  echo ERROR: git commit failed.
  exit /b 1
)

%GIT% push
if errorlevel 1 (
  echo ERROR: git push failed.
  exit /b 1
)

echo [%date% %time%] Done.
exit /b 0
