# Update: Russische Häfen inkl. Nordpolarmeer (Arktis) ergänzen

## Was wurde ergänzt?
Neben den Baltik-Ports sind jetzt auch enge BBoxen für arktische/nördliche Export-Gateways drin:

- Murmansk
- Arkhangelsk
- Varandey
- Sabetta (Yamal LNG)
- Dudinka

## Welche Dateien nutzen?
- Sampler: `aisstream_bbox_sampler_v3.py`
- Daily 3-layer Export: `make_daily_lagebild_from_bbox_v6.py`
- FROM_RUSSIA only: `make_daily_from_russia_from_bbox_v2.py`

## Wichtig
Damit "aus Russland" (FROM_RUSSIA) zuverlässig Treffer liefert, musst du RU-Port-BBoxen
überhaupt mitsammeln. Wenn du erst heute umstellst, kann es sein, dass in den ersten Tagen
noch wenige/keine Treffer rauskommen (Lookback-Fenster noch leer).

## Sampler neu starten (Fenster A)
Strg+C, dann:
```bat
python aisstream_bbox_sampler_v3.py --outdir logs --preset northsea_southbaltic_russia_ports --min-seconds-per-ship 1800
```

## Export (Fenster B)
- 3 Layer: `RUN_daily_export_today_v4_3layers.bat`
- Nur FROM_RUSSIA: `RUN_daily_from_russia_today_v2.bat`
