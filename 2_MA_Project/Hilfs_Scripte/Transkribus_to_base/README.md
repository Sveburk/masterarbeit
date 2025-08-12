# Transkribus to Base Schema Converter

Dieses Skript konvertiert Transkribus XML-Ausgaben in ein Basis-Schema-Format (JSON) für die weitere Verarbeitung.

## Abhängigkeiten

Folgende Python-Pakete werden benötigt:
```
pip install pandas spacy rapidfuzz
python -m spacy download de_core_news_sm
```

## Verzeichnisstruktur

Die Transkribus-Daten sollten in folgender Struktur vorliegen:
```
/Transkribus_Verzeichnis/
  ├── 6489763/                 # 7-stellige Transkribus-ID
  │   └── Akte_078_pdf/        # Akte-Unterordner
  │       └── page/            # Enthält die XML-Dateien
  │           ├── 0001_p001.xml
  │           └── 0002_p002.xml
  └── weitere_IDs/
```

## Verwendung

1. Konfiguriere die Pfade in der Datei `transkribus_to_base_schema_alt.py`:
   - `TRANSKRIBUS_DIR`: Pfad zum Eingabeverzeichnis mit Transkribus-XML-Dateien
   - `OUTPUT_DIR`: Pfad zum Ausgabeverzeichnis für die erzeugten JSON-Dateien
   - `CSV_PATH_KNOWN_PERSONS`: Pfad zur CSV-Datei mit bekannten Personen
   - `PLACE_CSV_PATH`: Pfad zur CSV-Datei mit bekannten Orten

2. Ausführen des Skripts:
   ```
   python transkribus_to_base_schema_alt.py
   ```

## Debugging-Hinweise

Das Skript enthält umfangreiche Debug-Ausgaben, die beim Troubleshooting helfen können. 
Die extrahierten Entitäten (Personen, Orte, Organisationen und Daten) werden in der Konsole angezeigt.

## Problembehebung

Falls leere JSONs erzeugt werden:
1. Überprüfe die XML-Dateistruktur und die verwendeten Attribute
2. Überprüfe die Debug-Ausgaben zu den Extraktionsschritten
3. Überprüfe, ob die CSV-Dateien für Personen und Orte korrekt geladen werden