# Probe: Kommen AIS-Meldungen aus der BBox an?

Wenn dein Watchlist-Log 0 Bytes bleibt, klärt dieser Test in 20 Sekunden:
- **bekommst du überhaupt PositionReports** aus der Nordsee-/Ostsee-BBox?

## Nutzung
```bat
cd "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python aisstream_probe_bbox.py --preset northsea_southbaltic --seconds 20 --max 200 --out logs\probe.jsonl
```

## Interpretation
- `wrote >0 lines`: Stream kommt an → 0 Bytes im Watchlist-Log heißt: in der Zeit keine Watchlist-Treffer.
- `wrote 0 lines`: Stream kommt nicht an → dann ist es Netz/Proxy/Handshake/Throttling.
