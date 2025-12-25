# Patched aisstream_collector.py

Du hattest: `Watchlist empty or no valid 9-digit MMSI.`

Ursachen sind meistens:
1) CSV ist mit `;` getrennt (typisch nach Excel speichern, deutsches Locale)
2) Deine OSINT-Liste enthält IMO, aber keine MMSI

Diese gepatchte Version:
- erkennt `,` oder `;` automatisch
- kann auch IMO-basierte Watchlists nutzen, indem sie MMSI↔IMO aus `ShipStaticData` lernt.

## Update in deinem Projektordner
1) Diese Datei herunterladen:
- `aisstream_collector.py` (patched)

2) In deinen Projektordner kopieren und die alte Version überschreiben:
`C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild`

## Start
```bat
cd "C:\Users\User\Documents\WSP\GIS-Analyse\AIS-Lagebild"
python aisstream_collector.py --watchlist watchlist.csv --outdir logs --preset northsea_southbaltic
```

Wenn du IMO-only Watchlists hast, siehst du:
`server-side MMSI filter disabled ...`

Das ist normal.
