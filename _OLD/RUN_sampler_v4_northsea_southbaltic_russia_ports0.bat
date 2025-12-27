@echo off
REM Start sampler v4 (refined Kaliningrad/Baltiysk boxes to avoid Gdansk spillover)
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python aisstream_bbox_sampler_v4.py --outdir logs --preset northsea_southbaltic_russia_ports --min-seconds-per-ship 1800
pause
