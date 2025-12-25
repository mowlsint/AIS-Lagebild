# Watchlist erstellen: GUR (Shadow Fleet) + russisch geflaggt (ohne Handarbeit)

Du willst eine `watchlist.csv`, die der Collector benutzen kann, **ohne** dass du jedes Schiff manuell in Trackern suchst.

## Teil A — Shadow Fleet von der GUR-Website (automatisch)
1) Lege die Datei `gur_shadowfleet_to_watchlist.py` in deinen Projektordner.
2) Installiere die 2 Helferpakete (einmalig):
```bat
python -m pip install requests beautifulsoup4
```
3) Erzeuge die Shadow-Fleet-Watchlist:
```bat
python gur_shadowfleet_to_watchlist.py --out watchlist_shadowfleet.csv
```

Ergebnis: `watchlist_shadowfleet.csv` mit Spalten
`category,name,imo,mmsi,sanctioned,sanctions,note`

### Wichtig für den Collector:
Der Collector matcht **über MMSI**. Wenn auf der GUR-Seite bei manchen Einträgen keine MMSI steht, werden diese Schiffe im Live-Stream nicht matchen (für Wochenexport ist IMO trotzdem nützlich, aber MMSI ist Gold).

## Teil B — „russisch geflaggt“ (realistisch automatisieren)
AIS selbst enthält **kein Flag-Feld**. Nur MMSI.  
Darum gibt es zwei praktikable OSINT-Wege:

### Weg 1 (empfohlen): russisch geflaggt „aus der Woche heraus“ ableiten
- Du sammelst in der Woche **alle** Schiffe in der BBox (stark gedrosselt),
- und klassifizierst anschließend „russisch“ per **MMSI-MID = 273** (Indiz, nicht 100% Flag).
Das ist *für ein wöchentliches Lagebild* oft ausreichend.

> Wenn du das willst, baue ich dir einen Collector-Modus `--mode all` + Wochenreport „RU-likely“.

### Weg 2: Flag aus einer Registry/DB ziehen (meist paid)
- VesselFinder API, Lloyd’s List, IHS/Equasis usw.
- Sehr gut, aber i.d.R. nicht kostenlos.

## Teil C — Dateien zusammenführen
Wenn du mehrere Watchlists hast:
```bat
python merge_watchlists.py --out watchlist.csv watchlist_shadowfleet.csv watchlist_russian.csv
```

## Nächster sinnvoller Schritt
Erst Shadow Fleet sauber automatisieren (Teil A).  
Danach entscheiden wir, ob du RU-Flag als „MID-Indiz“ (Weg 1) oder via Datenbank (Weg 2) machen willst.
