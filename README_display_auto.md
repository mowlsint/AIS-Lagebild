# Auto-"display" für uMap Labels

Diese Dateien schreiben automatisch `properties.display` in jede GeoJSON.

**Wichtig:**
- `display` ist nur bei `feature = last_position` gesetzt.
- `feature = track` bekommt `display=""` (damit Labels nicht auf Linien kleben).

## uMap
In uMap pro Ebene als Beschriftung das Feld **display** wählen.

## Dateien
- make_daily_3_geojson_layers_v2.py  (+ RUN_export_today_3_geojson_layers_v2.bat)
- make_daily_lagebild_from_bbox_v7.py (+ RUN_daily_export_today_v7_3layers.bat)
- make_daily_from_russia_excluding_shadow_mid273_v5.py (+ RUN_export_from_russia_excl_shadow_mid273_v5_today.bat)
