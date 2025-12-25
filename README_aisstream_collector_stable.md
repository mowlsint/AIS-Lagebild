# aisstream_collector_stable.py

Für sporadische WebSocket-Probleme wie:
- `no close frame received or sent`
- `timed out during opening handshake`

… ist das meist Server/Netz/Proxy.  
Diese Version verbindet sich “netter” wieder und wartet länger, wenn aisstream throttlet.

## Nutzung
```bat
cd "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python aisstream_collector_stable.py --watchlist watchlist.csv --outdir logs --preset northsea_southbaltic
```

## Optionen
- `--open-timeout 60`  (Handschlag länger erlauben)
- `--alive-minutes 5`  (Heartbeat-Ausgabe)
- `--throttle-wait 180` (bei concurrent-limit länger warten)
