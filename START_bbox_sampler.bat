@echo off
cd /d "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
REM Collect a thin background sample of ALL vessels in the BBox (throttled)
python aisstream_bbox_sampler.py --outdir logs --preset northsea_southbaltic --min-seconds-per-ship 1800
pause
