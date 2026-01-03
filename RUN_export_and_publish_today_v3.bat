@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM  RUN_export_and_publish_today_v2.bat (FIXED)
REM  - Exports GeoJSON from logs_v4\bbox_*.jsonl (flat sampler output)
REM  - Picks newest bbox_YYYY-MM-DD.jsonl day automatically
REM  - ALWAYS refreshes live_*.geojson from the dated outputs
REM  - Commits + pushes ONLY if there are changes
REM =========================================================

REM === CONFIG =================================================
set "DATA_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
set "REPO_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo"
set "REPO_PUBLIC=%REPO_ROOT%\public"

set "PY=python"
set "GIT=git"

REM Script for flat logs_v4 (ts_utc/mmsi/lat/lon)
set "SCRIPT=%DATA_ROOT%\make_daily_4_geojson_layers_v3.py"

REM Input logs
set "IN_GLOB=%DATA_ROOT%\logs_v4\bbox_*.jsonl"

REM Shadowfleet watchlist
set "SHADOW_CSV=%DATA_ROOT%\watchlist_shadowfleet.csv"
set "PRESANCTION_CSV=%DATA_ROOT%\watchlist_pre_sanction_kse.csv"

REM =========================================================
REM  0) SANITY CHECKS
REM =========================================================
if not exist "%REPO_ROOT%\" (
  echo ERROR: REPO_ROOT not found: "%REPO_ROOT%"
  exit /b 1
)
if not exist "%REPO_PUBLIC%\" (
  echo ERROR: REPO_PUBLIC not found: "%REPO_PUBLIC%"
  exit /b 1
)
if not exist "%SCRIPT%" (
  echo ERROR: Python script not found: "%SCRIPT%"
  exit /b 1
)
if not exist "%SHADOW_CSV%" (
  echo ERROR: Shadowfleet CSV not found: "%SHADOW_CSV%"
  exit /b 1
)

REM =========================================================
REM  1) SET TODAY = newest bbox_YYYY-MM-DD.jsonl found (CMD-only)
REM =========================================================
set "TODAY="
set "LATEST="

for /f "delims=" %%F in ('dir /b /o-d "%DATA_ROOT%\logs_v4\bbox_*.jsonl" 2^>nul') do (
  set "LATEST=%%~nF"
  goto :got_latest
)

:got_latest
if not defined LATEST (
  echo ERROR: No bbox_*.jsonl files found in "%DATA_ROOT%\logs_v4"
  exit /b 1
)

REM LATEST looks like bbox_2026-01-03
set "TODAY=%LATEST:bbox_=%"

echo [%date% %time%] Using data day: %TODAY%

REM =========================================================
REM  2) GENERATE LAYERS (writes dated outputs into repo/public)
REM =========================================================
echo [%date% %time%] Running python export...
"%PY%" "%SCRIPT%" --in "%IN_GLOB%" --date "%TODAY%" --shadowfleet "%SHADOW_CSV%" --presanction "%PRESANCTION_CSV%" --outdir "%REPO_PUBLIC%"
if errorlevel 1 (
  echo ERROR: Python export failed.
  exit /b 1
)

REM =========================================================
REM  3) ALWAYS REFRESH LIVE FILES FROM THE DATED OUTPUTS
REM     (prevents stale 45-byte empty live_* files)
REM =========================================================
set "DATED_SHADOW=%REPO_PUBLIC%\lagebild_%TODAY%_shadowfleet.geojson"
set "DATED_MID=%REPO_PUBLIC%\lagebild_%TODAY%_ru_mid273.geojson"
set "DATED_FROMRU=%REPO_PUBLIC%\lagebild_%TODAY%_from_russia_ports_excluding_shadow_mid273.geojson"
set "DATED_PRES=%REPO_PUBLIC%\lagebild_%TODAY%_pre_sanctioned.geojson"

if not exist "%DATED_SHADOW%" (
  echo ERROR: Missing dated shadowfleet output: "%DATED_SHADOW%"
  exit /b 1
)
if not exist "%DATED_MID%" (
  echo ERROR: Missing dated ru_mid273 output: "%DATED_MID%"
  exit /b 1
)
if not exist "%DATED_FROMRU%" (
  echo ERROR: Missing dated from_ru output: "%DATED_FROMRU%"
  exit /b 1
)

copy /Y "%DATED_SHADOW%" "%REPO_PUBLIC%\live_shadowfleet.geojson" >nul
copy /Y "%DATED_MID%" "%REPO_PUBLIC%\live_ru_mid273.geojson" >nul
copy /Y "%DATED_FROMRU%" "%REPO_PUBLIC%\live_from_russia_ports_excl_shadow.geojson" >nul
copy /Y "%DATED_PRES%" "%REPO_PUBLIC%\live_pre_sanctioned.geojson" >nul

REM Verify live outputs exist now
if not exist "%REPO_PUBLIC%\live_shadowfleet.geojson" (
  echo ERROR: live_shadowfleet.geojson missing
  exit /b 1
)
if not exist "%REPO_PUBLIC%\live_ru_mid273.geojson" (
  echo ERROR: live_ru_mid273.geojson missing
  exit /b 1
)
if not exist "%REPO_PUBLIC%\live_from_russia_ports_excl_shadow.geojson" (
  echo ERROR: live_from_russia_ports_excl_shadow.geojson missing
  exit /b 1
)
if not exist "%REPO_PUBLIC%\live_pre_sanctioned.geojson" (
  echo ERROR: live_pre_sanctioned.geojson missing
)

REM =========================================================
REM  4) GIT PUBLISH
REM =========================================================
echo [%date% %time%] Git publish...
cd /d "%REPO_ROOT%" || exit /b 1

REM Pull only if working tree is clean (avoid rebase error)
%GIT% diff --quiet
if errorlevel 1 (
  echo WARN: Working tree not clean; skipping git pull --rebase.
) else (
  %GIT% pull --rebase
  if errorlevel 1 (
    echo ERROR: git pull --rebase failed.
    exit /b 1
  )
)

REM Stage ALL GeoJSON under public (Windows-safe; no ** glob)
for /r "%REPO_PUBLIC%" %%F in (*.geojson) do (
  %GIT% add -f "%%F"
)

echo --- staged files ---
%GIT% diff --cached --name-only

REM Nothing staged? Then nothing to do.
%GIT% diff --cached --quiet
if not errorlevel 1 (
  echo [%date% %time%] No changes to commit.
  exit /b 0
)

set "MSG=Automated publish: %TODAY% %TIME%"
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