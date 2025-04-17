import pandas as pd
from rapidfuzz import process, fuzz
import logging
import re

class PlaceMatcher:
    def __init__(self, csv_path, threshold=85):
        self.threshold = threshold
        try:
            self.places_df = pd.read_csv(csv_path, sep=";")
            self.places_df.rename(columns=lambda x: x.strip(), inplace=True)
            self.places_df.rename(columns={
                "nodegoat id": "nodegoat_id",
                "Alternativer Ort Name": "alternate_name"
            }, inplace=True)
            print("[DEBUG] Spaltennamen nach Umbenennung:", self.places_df.columns.tolist())
            self.known_name_map = self._build_known_place_map()
        

        except Exception as e:
            logging.error(f"Fehler beim Laden der Ortsdaten aus {csv_path}: {e}")
            self.places_df = pd.DataFrame()
            self.known_name_map = {}

    def _normalize_place_name(self, name: str) -> str:
        name = name.lower()
        # Erst spezielle Zeichen durch Leerzeichen ersetzen
        name = re.sub(r"[/\-\.(),]", " ", name)
        # Dann andere Sonderzeichen entfernen
        name = re.sub(r"[^\w\s]", "", name)
        # Stopwörter entfernen (erweiterte Liste)
        name = re.sub(r"\b(am|an|bei|in|auf|von|zu|zum|zur|a|i|der|die|das|im|aus|und)\b", "", name)
        # Mehrfache Leerzeichen entfernen
        name = re.sub(r"\s+", " ", name)
        return name.strip()

    def _build_known_place_map(self):
        """
        Erstellt eine Map: normierter Ortsname → gemeinsames Groundtruth-Dict mit allen Varianten.
        Fügt auch Originalvarianten als Schlüssel hinzu für bessere Trefferquoten.
        """
        name_map = {}
        merged_places = {}  # Key: eindeutige ID, z. B. nodegoat_id oder GeoNames

        for idx, row in self.places_df.iterrows():
            try:
                def safe(val):
                    return str(val).strip() if pd.notna(val) else ""

                primary_name = safe(row.get("Name"))
                alt_name = safe(row.get("Alternativer Ort Name"))
                nodegoat_id = safe(row.get("nodegoat_id"))
                geonames_id = safe(row.get("GeoNames"))
                wikidata_id = safe(row.get("WikidataID"))

                # Eindeutige ID bestimmen
                place_id = nodegoat_id or geonames_id or wikidata_id
                if not place_id:
                    continue

                # Ortseintrag initialisieren
                if place_id not in merged_places:
                    merged_places[place_id] = {
                        "name": primary_name,
                        "geonames_id": geonames_id,
                        "wikidata_id": wikidata_id,
                        "nodegoat_id": nodegoat_id,
                        "all_variants": set()
                    }

                # Varianten sammeln
                if primary_name:
                    merged_places[place_id]["all_variants"].add(primary_name)
                if alt_name:
                    merged_places[place_id]["all_variants"].add(alt_name)

            except Exception as e:
                logging.warning(f"Fehler beim Parsen der Ortszeile (Index {idx}): {e}")
                continue

        # alle Varianten normalisieren und map aufbauen
        for place in merged_places.values():
            # Erstelle ein Standardeintrag für diesen Ort
            place_entry = {
                "matched_name": next(iter(place["all_variants"])),  # Erste Variante als Standard
                "all_variants": sorted(place["all_variants"]),
                "data": {
                    "name": place["name"],
                    "geonames_id": place["geonames_id"],
                    "wikidata_id": place["wikidata_id"],
                    "nodegoat_id": place["nodegoat_id"],
                }
            }
            
            # Füge normalisierte Namen hinzu
            for variant in place["all_variants"]:
                norm_name = self._normalize_place_name(variant)
                if norm_name and norm_name not in name_map:
                    name_map[norm_name] = place_entry
                
                # Füge auch den Originalnamen als Schlüssel hinzu (für exakte Matches)
                if variant.lower() not in name_map:
                    name_map[variant.lower()] = place_entry
            
        logging.info(f"Insgesamt {len(name_map)} Ortsnamen-Varianten im Index geladen")
        return name_map

    def match_place(self, input_place: str):
        if not input_place or not input_place.strip():
            return None

        if not self.known_name_map:
            logging.warning("Keine bekannten Orte zum Abgleich verfügbar.")
            return None

        try:
            normalized_input = self._normalize_place_name(input_place)
            
            # Debugging der Normalisierung und Varianten
            logging.debug(f"Normalisierte Eingabe: '{input_place}' → '{normalized_input}'")
            
            # 1. Direkter Versuch mit dem Originalnamen (lowercase)
            if input_place.lower() in self.known_name_map:
                entry = self.known_name_map[input_place.lower()]
                alternate_str = ";".join(sorted(entry["all_variants"]))
                logging.debug(f"Exakter Match für '{input_place}' → Nodegoat-ID: {entry['data'].get('nodegoat_id')}")
                return {
                    "matched_name": entry["matched_name"],
                    "matched_raw_input": input_place.strip(),
                    "score": 100,
                    "confidence": "exact_original",
                    "data": {
                        **entry["data"],
                        "alternate_place_name": alternate_str
                    }
                }

            # 2. Exakter Match mit normalisiertem Namen
            if normalized_input in self.known_name_map:
                entry = self.known_name_map[normalized_input]
                alternate_str = ";".join(sorted(entry["all_variants"]))
                logging.debug(f"Exakter normalisierter Match für '{input_place}' → Nodegoat-ID: {entry['data'].get('nodegoat_id')}")
                return {
                    "matched_name": entry["matched_name"],
                    "matched_raw_input": input_place.strip(),
                    "score": 100,
                    "confidence": "exact_normalized",
                    "data": {
                        **entry["data"],
                        "alternate_place_name": alternate_str
                    }
                }

            # 3. Mehrere Fuzzy-Strategien ausprobieren
            match_strategies = [
                ("token_sort_ratio", fuzz.token_sort_ratio),
                ("token_set_ratio", fuzz.token_set_ratio),
                ("partial_ratio", fuzz.partial_ratio)
            ]
            
            best_match = None
            best_score = 0
            best_method = ""
            
            for method_name, scorer in match_strategies:
                match, score, _ = process.extractOne(
                    normalized_input,
                    list(self.known_name_map.keys()),
                    scorer=scorer
                )
                
                if score > best_score:
                    best_match = match
                    best_score = score
                    best_method = method_name

            # 4. Wenn die beste Methode über dem Threshold liegt, zurückgeben
            if best_score >= self.threshold:
                entry = self.known_name_map[best_match]
                alternate_str = ";".join(sorted(entry["all_variants"]))
                logging.debug(f"Fuzzy Match gefunden für '{input_place}' mit Methode {best_method} → Nodegoat-ID: {entry['data'].get('nodegoat_id')}")
                return {
                    "matched_name": entry["matched_name"],
                    "matched_raw_input": input_place.strip(),
                    "score": best_score,
                    "confidence": f"fuzzy ({best_method})",
                    "data": {
                        **entry["data"],
                        "alternate_place_name": alternate_str
                    }
                }

            logging.debug(f"No match found for '{input_place}' (best score: {best_score})")
            return None

        except Exception as e:
            logging.warning(f"Fehler beim Orts-Matching: {e}")
            return None

        except Exception as e:
            logging.warning(f"Fehler beim Orts-Matching: {e}")
            return None
    def is_known_place(self, input_place: str):
        """Prüft, ob ein Ort bekannt ist (Wrapper um match_place)"""
        return self.match_place(input_place) is not None
