## Kurzanleitung & Erläuterung

**Worum geht’s hier?**
Diese Risikomatrix ist ein Arbeitswerkzeug zur **strukturieren Erstbewertung** von Schiffen im Kontext maritimer **Hybridbedrohungen** (z. B. Shadow-Fleet-typische Muster, Verschleierung, Infrastruktur-Nähe, Manipulationsindikatoren). Sie ersetzt **keine** Lageanalyse, Beweissicherung oder rechtliche Würdigung, hilft aber dabei, Hinweise konsistent zu gewichten und mehrere Schiffe vergleichbar zu machen.

### 1) Grundprinzip der Matrix (Y × X)

Die Bewertung besteht aus zwei Achsen:

**Y-Achse: Struktur (L1–L5)**
Beschreibt, wie „auffällig“ ein Schiff bereits **strukturell** ist – also unabhängig vom aktuellen Verhalten. Beispiele: Flaggenprofil (Grey/Black/Unknown), häufige Identitätswechsel, opake Eigentums-/Managementkette, auffällige Versicherung/Class, Sanktionsbezug.

**X-Achse: Operative Indikatoren (X0–X5, in der Matrix als X0–X4 dargestellt)**
Beschreibt, wie stark die **operative Evidenz** ist – also was das Schiff **tatsächlich tut** oder welche Vorfallnähe/Anomalien vorliegen (AIS-Blackout, GNSS/AIS-Spoofing, loitering an Kabeln, STS, „dark activity“, Survey-Pattern, Nähe zu Militär etc.).

**Wichtig:** In der Tabelle gibt es rechts nur eine Spalte **„4/5 – Kritisch“**.
Das bedeutet: X=4 oder X=5 wird in der Darstellung **beide** in diese letzte Spalte einsortiert. Intern bleibt die feinere Abstufung (raw) erhalten (z. B. X5), die Matrix zeigt aber zusammengefasst (X4/5).

---

### 2) Datenqualität / Confidence (warum das Feld wichtig ist)

AIS und GNSS können gestört, manipuliert oder unvollständig sein. Daher gibt es die Auswahl **Datenqualität / Confidence**:

* **hoch (0.9):** AIS plus zusätzliche Quelle(n) bestätigt (z. B. Bild, Hafenmeldung, Dokumente, OSINT-Verifikation).
* **mittel (0.6):** AIS plausibel, teilweise bestätigt oder stimmig im Kontext.
* **niedrig (0.3):** nur AIS / unsicher / starke Unschärfe.

Die Confidence wirkt sich **auf die X-Achse** aus:
Bei geringer Confidence werden operative Hinweise vorsichtiger eingeordnet, um Fehlalarme zu reduzieren. (Ein „nur AIS“-Muster soll nicht automatisch so hoch gewertet werden wie ein bestätigter Vorfall.)

---

### 3) Schritt-für-Schritt: So nutzt du das Tool

**Schritt 1 – Basisdaten:**
Trage Schiffsname, Typ und Flaggenstaat ein.
Die Flagge setzt automatisch (als Anzeige) Grey/Black-Status.

**Schritt 2 – Struktur (Y):**
Setze strukturelle Hinweise.
Faustregel: Alles, was längerfristig zum „Profil“ gehört (Ownership, Flaggenwechsel, Sanktionen, Class/Insurance/Identity-Churn), gehört nach Y.

**Schritt 3 – Operative Indikatoren (X):**
Setze operative Hinweise.
Faustregel: Alles, was sich auf aktuelles Verhalten, Routen, Muster, Vorfälle und Infrastrukturbezug bezieht, gehört nach X.

**Schritt 4 – Schiff hinzufügen:**
Klick auf **„Schiff hinzufügen“**.
Das Schiff erscheint:

* als **Badge in der Matrix-Zelle** (Lx / X-Spalte)
* zusätzlich in der Liste „Erfasste Schiffe“ darunter (mit Risiko, X raw/Matrix, Confidence, Top-Gründe)

**Schritt 5 – Details & Freitextnotiz:**
Klick auf das Badge (oder in der Schiffsliste auf den Eintrag), um das **Details-Fenster** zu öffnen.
Dort kannst du eine **Freitextnotiz** speichern (z. B. Quellenlage, Maßnahmen, Bewertung, offene Fragen).
Die Notiz bleibt pro Schiff erhalten und kann jederzeit überschrieben werden.

**Schritt 6 – Export:**
Mit **„Alle als CSV“** exportierst du die erfassten Schiffe als Datei zur Weiterverarbeitung.

---

### 4) Wie du Ergebnisse interpretierst (ohne dich reinzureden)

* **Niedrig / Mittel** heißt nicht „harmlos“, sondern: *aktuell keine starke Evidenz oder strukturell unauffällig.*
* **Hoch / Sehr hoch** heißt: *relevante Hinweise in Struktur und/oder operativer Evidenz — Priorisierung für vertiefte Prüfung.*
* Eine **hohe Struktur (L4/L5)** ohne operative Kreuze kann sinnvoll sein (z. B. stark verschleiertes Profil).
* Eine **hohe X-Bewertung** sollte idealerweise durch **gute Confidence** gestützt werden, sonst ist das Ergebnis eher ein „Arbeitsverdacht“.

---

### 5) Typische Denkfehler, die die Matrix bewusst verhindert

* **„AIS sagt’s, also stimmt’s.“** → Confidence zwingt zur Quellen-Disziplin.
* **„Ein Hinweis = sicher.“** → X berücksichtigt Stärke und Kombinationen, nicht nur Anzahl.
* **„Flagge allein entscheidet alles.“** → Flagge beeinflusst Y, aber operative Evidenz bleibt separat.
* **„Viele Schiffe, kein Überblick.“** → Matrix + Liste schaffen Vergleichbarkeit und Sichtbarkeit.

---

### 6) Kurz-Haftnotiz für den Einsatz

**Wenn du nur 20 Sekunden hast:**

1. Flagge + Strukturhinweise setzen (Y).
2. Operative Anomalien setzen (X).
3. Confidence realistisch wählen.
4. „Schiff hinzufügen“.
5. Notiz im Pop-up: Quelle + nächste Maßnahme + offene Frage.

