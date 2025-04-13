import pandas as pd
from rapidfuzz import process, fuzz
import logging

class PlaceMatcher:
    def __init__(self, csv_path, threshold=90):
        self.threshold = threshold
        try:
            self.places_df = pd.read_csv(csv_path, sep=";")
            self.known_name_map = self._build_known_place_map()
        except Exception as e:
            logging.error(f"Fehler beim Laden der Ortsdaten aus {csv_path}: {e}")
            self.places_df = pd.DataFrame()
            self.known_name_map = {}

    def _build_known_place_map(self):
        """Erstellt eine Liste aller Namen und alternativen Namen mit Zeilenbezug"""
        name_map = {}
        if self.places_df.empty:
            logging.warning("Orte-DataFrame ist leer. Keine Orte zum Abgleich verfügbar.")
            return name_map
            
        for idx, row in self.places_df.iterrows():
            try:
                names = set()
                # Hauptname hinzufügen
                if pd.notna(row.get("Name")):
                    names.add(str(row["Name"]).strip().lower())
                
                # Alternative Namen hinzufügen
                if pd.notna(row.get("Alternativer Ort Name")):
                    # Mehrere Alternativnamen durch Komma getrennt behandeln
                    # Aber vorsichtig mit Kommas in Namen sein (z.B. "Frankfurt a. M., Deutschland")
                    try:
                        # Wir gehen davon aus, dass Alternativnamen durch Semikolon getrennt sind
                        alt_names = str(row["Alternativer Ort Name"]).split(";")
                        for alt in alt_names:
                            if alt.strip():  # Leere Einträge überspringen
                                names.add(alt.strip())
                    except Exception as e:
                        # Fallback: Ganzen String als einen Namen nehmen
                        names.add(str(row["Alternativer Ort Name"]).strip())
                
                # Alle Namen der map hinzufügen
                for name in names:
                    if name:  # Leere Namen überspringen
                        name_map[name] = row.to_dict()
            except Exception as e:
                logging.warning(f"Fehler beim Verarbeiten einer Ortszeile: {e}")
                continue
                
        return name_map

    def match_place(self, input_place: str):
        """Fuzzy-Matching gegen alle bekannten Namen & Alternativnamen"""
        if not input_place or not input_place.strip().lower():
            return None
            
        if not self.known_name_map:
            logging.warning("Keine bekannten Orte zum Abgleich verfügbar.")
            return None
            
        try:
            # Verschiedene Fuzzy-Matching-Methoden kombinieren für bessere Ergebnisse
            # token_sort_ratio berücksichtigt Wortumstellungen (z.B. "Berlin Deutschland" vs "Deutschland Berlin")
            normalized_input = input_place.strip().lower()
            match, score, _ = process.extractOne(
                normalized_input,
                list(self.known_name_map.keys()),
                scorer=fuzz.token_sort_ratio
            )
            
            # Verschiedene Vertrauensstufen zurückgeben
            confidence = "low"
            if score >= self.threshold:
                confidence = "high"
            elif score >= 75:  # Mittlere Vertrauensstufe
                confidence = "medium"
                
            if score >= 75:  # Wir akzeptieren auch mittlere Matches, kennzeichnen sie aber
                matched_row = self.known_name_map[match]

                    # Cleaned dictionary für Place-Objekt
                cleaned_place_data = {
                    "name": matched_row.get("Name", "").strip(),
                    "alternate_place_name": matched_row.get("Alternativer Ort Name", "").strip(),
                    "geonames_id": str(int(matched_row["GeoNames"])) if pd.notna(matched_row.get("GeoNames")) else "",
                    "wikidata_id": str(matched_row.get("WikidataID", "")).strip() if pd.notna(matched_row.get("WikidataID", "")) else "",
                    "nodegoat_id": str(matched_row.get("nodegoat ID", "")).strip() if pd.notna(matched_row.get("nodegoat ID", "")) else "",  
                }


                return {
                    "matched_name": match,
                    "score": score,
                    "confidence": confidence,
                    "data": cleaned_place_data,
                }
            return None
    
        except Exception as e:
            logging.warning(f"Fehler beim Orts-Matching: {e}")
            return None


    def is_known_place(self, input_place: str):
        """Prüft, ob ein Ort bekannt ist (Wrapper um match_place)"""
        return self.match_place(input_place) is not None
