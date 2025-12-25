# Fix: "Aus Russland" False Positives bei Gdansk/Gdynia

## Warum kann das passieren?
Dein FROM_RUSSIA-Indikator ist: "AIS-Position war in einer RU-Port-BBox".
Die alte Kaliningrad/Baltiysk-BBox war zu breit und konnte Randbereiche Richtung
Bucht von Danzig/Vistula Spit mitnehmen.

## Fix (v4)
Kaliningrad/Baltiysk wird in zwei engere Boxen aufgeteilt:
- Baltiysk (KO): 19.70E..20.05E / 54.58N..54.75N
- Kaliningrad (lagoon): 20.35E..20.75E / 54.62N..54.78N

## Was tun?
1) Sampler auf v4 umstellen (Strg+C und neu starten):
   python aisstream_bbox_sampler_v4.py --outdir logs --preset northsea_southbaltic_russia_ports
2) Export-Script v4 nutzen:
   python make_daily_from_russia_excluding_shadow_mid273_v4.py ...

## Tipp: Lookback kann "korrekt" sein
Wenn ein Schiff tatsächlich in den letzten 14 Tagen in Kaliningrad/Baltiysk war,
ist FROM_RUSSIA korrekt – auch wenn es danach Polen angelaufen hat.
Prüfe in der GeoJSON:
- last_ru_seen_utc
- last_ru_port_box
