@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM RUN_export_and_publish_today_v3.bat  (FAST INPUT: last 10 days)
REM - Copies only the last N days of logs into a temp folder
REM - Runs make_daily_4_geojson_layers_v3.py on that reduced set
REM - Publishes dated + live GeoJSONs to /public
REM ============================================================

REM ---- CONFIG: adjust to your paths ----
set "DATA_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
set "REPO_ROOT=C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild_repo"
set "REPO_PUBLIC=%REPO_ROOT%\public"
set "LOG_DIR=%DATA_ROOT%\logs_v4"

REM Python executable (set explicitly if needed)
set "PY=python"

REM Scripts / inputs
set "SCRIPT=%DATA_ROOT%\make_daily_4_geojson_layers_v3.py"
set "SHADOW_CSV=%DATA_ROOT%\watchlist_shadowfleet.csv"
set "PRESANCTION_CSV=%DATA_ROOT%\watchlist_pre_sanction_kse.csv"

REM How many days of logs to include (inclusive, e.g. 10 = today + previous 9 days)
set "DAYS_BACK=10"

REM Track and RU-lookback logic inside the python script
REM (Script defaults: --lookback-days 10, --track-days 3)
set "LOOKBACK_DAYS=10"
set "TRACK_DAYS=3"

REM ---- Resolve TODAY (UTC date, matching your ts_utc=Z logging) ----
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString(''yyyy-MM-dd'')"') do set "TODAY=%%i"

echo [%date% %time%] Using data day (UTC): %TODAY%
echo [%date% %time%] Preparing last %DAYS_BACK% days of input logs...

REM ---- Prepare temp folder with only recent logs ----
set "TMP_IN=%REPO_ROOT%\tmp_in_recent"
if not exist "%TMP_IN%" mkdir "%TMP_IN%"

REM Clean temp folder
del /q "%TMP_IN%\bbox_*.jsonl" >nul 2>&1

REM Copy only bbox_YYYY-MM-DD.jsonl where date >= (UTC today - (DAYS_BACK-1))
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$logDir = '%LOG_DIR%';" ^
  "$tmpIn  = '%TMP_IN%';" ^
  "$daysBack = [int]'%DAYS_BACK%';" ^
  "$start = (Get-Date).ToUniversalTime().Date.AddDays(-($daysBack-1));" ^
  "Get-ChildItem -Path $logDir -Filter 'bbox_*.jsonl' |" ^
  "Where-Object { $_.BaseName -match '^bbox_(\d{4}-\d{2}-\d{2})$' -and ([datetime]$Matches[1]) -ge $start } |" ^
  "ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $tmpIn $_.Name) -Force }" ^
  >nul 2>&1

REM Count copied files
for /f %%i in ('powershell -NoProfile -Command "(Get-ChildItem -Path ''%TMP_IN%'' -Filter ''bbox_*.jsonl'' | Measure-Object).Count"') do set "COPIED=%%i"
echo [%date% %time%] Copied recent log files: %COPIED%

if "%COPIED%"=="0" (
  echo ERROR: No recent bbox_*.jsonl files found for last %DAYS_BACK% days in: %LOG_DIR%
  exit /b 1
)

REM Use reduced input glob
set "IN_GLOB=%TMP_IN%\bbox_*.jsonl"

REM ---- Ensure output dir exists ----
if not exist "%REPO_PUBLIC%" mkdir "%REPO_PUBLIC%"

echo [%date% %time%] Running python export...
"%PY%" "%SCRIPT%" --in "%IN_GLOB%" --date "%TODAY%" --shadowfleet "%SHADOW_CSV%" --presanction "%PRESANCTION_CSV%" --outdir "%REPO_PUBLIC%" --lookback-days %LOOKBACK_DAYS% --track-days %TRACK_DAYS%
if errorlevel 1 (
  echo ERROR: Python export failed.
  exit /b 1
)

REM ---- Publish live aliases from today's dated files ----
set "DATED_SHADOW=%REPO_PUBLIC%\lagebild_%TODAY%_shadowfleet.geojson"
set "DATED_MID273=%REPO_PUBLIC%\lagebild_%TODAY%_ru_mid273.geojson"
set "DATED_FROMRU=%REPO_PUBLIC%\lagebild_%TODAY%_from_russia_ports_excluding_shadow_mid273.geojson"
set "DATED_PRES=%REPO_PUBLIC%\lagebild_%TODAY%_pre_sanctioned.geojson"

if not exist "%DATED_SHADOW%" (echo ERROR: Missing %DATED_SHADOW% & exit /b 1)
if not exist "%DATED_MID273%" (echo ERROR: Missing %DATED_MID273% & exit /b 1)
if not exist "%DATED_FROMRU%" (echo ERROR: Missing %DATED_FROMRU% & exit /b 1)
if not exist "%DATED_PRES%" (echo ERROR: Missing %DATED_PRES% & exit /b 1)

copy /Y "%DATED_SHADOW%" "%REPO_PUBLIC%\live_shadowfleet.geojson" >nul
copy /Y "%DATED_MID273%" "%REPO_PUBLIC%\live_ru_mid273.geojson" >nul
copy /Y "%DATED_FROMRU%" "%REPO_PUBLIC%\live_from_russia_ports_excl_shadow.geojson" >nul
copy /Y "%DATED_PRES%" "%REPO_PUBLIC%\live_pre_sanctioned.geojson" >nul

echo [%date% %time%] OK. GeoJSONs updated in: %REPO_PUBLIC%
exit /b 0
