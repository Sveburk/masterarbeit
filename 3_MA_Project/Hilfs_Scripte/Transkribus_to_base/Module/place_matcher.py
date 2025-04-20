import pandas as pd
from rapidfuzz import process, fuzz
import logging
import re
from typing import List, Dict, Any, Optional, Tuple
def sanitize_id(v) -> str:
    # Leere Strings oder NaN bleiben leer
    if pd.isna(v) or v == "":
        return ""
    try:
        # z.B. "2867714.0" → "2867714"
        return str(int(float(v)))
    except Exception:
        return ""

class PlaceMatcher:
    def __init__(self, csv_path, threshold=85):
        self.threshold = threshold
        try:
            self.places_df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
            self.places_df.rename(columns=lambda x: x.strip(), inplace=True)
            self.places_df.rename(columns={
                "nodegoat ID": "nodegoat_id",
                "GeoNames": "geonames_id",
                "WikidataID": "wikidata_id",
                "Alternativer Ort Name": "alternate_place_name",
                "Name": "name"
            }, inplace=True)

            for col in ("geonames_id", "wikidata_id"):
                if col in self.places_df.columns:
                    self.places_df[col] = self.places_df[col].apply(sanitize_id)

            print("[DEBUG] Spaltennamen nach Umbenennung:", self.places_df.columns.tolist())
            self.known_name_map = self._build_known_place_map()
        
        except Exception as e:
            logging.error(f"Fehler beim Laden der Ortsdaten aus {csv_path}: {e}")
            self.places_df = pd.DataFrame()
            self.known_name_map = {}

    def _normalize_place_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r"[/\-\.(),]", " ", name)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\b(am|an|bei|in|auf|von|zu|zum|zur|a|i|der|die|das|im|aus|und)\b", "", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()

    def _build_known_place_map(self):
        name_map = {}
        merged_places = {}

        for idx, row in self.places_df.iterrows():
            try:
                def safe(val):
                    return str(val).strip() if pd.notna(val) else ""

                primary_name = safe(row.get("name"))
                alt_name = safe(row.get("alternate_place_name"))
                nodegoat_id = safe(row.get("nodegoat_id"))
                geonames_id = safe(row.get("geonames_id"))
                wikidata_id = safe(row.get("wikidata_id"))

                place_id = nodegoat_id or geonames_id or wikidata_id
                if not place_id:
                    continue

                if place_id not in merged_places:
                    merged_places[place_id] = {
                        "name": primary_name,
                        "geonames_id": geonames_id,
                        "wikidata_id": wikidata_id,
                        "nodegoat_id": nodegoat_id,
                        "all_variants": set()
                    }

                if primary_name:
                    merged_places[place_id]["all_variants"].add(primary_name)
                if alt_name:
                    merged_places[place_id]["all_variants"].add(alt_name)

            except Exception as e:
                logging.warning(f"Fehler beim Parsen der Ortszeile (Index {idx}): {e}")
                continue

        for place in merged_places.values():
            place_entry = {
                "matched_name": next(iter(place["all_variants"])),
                "all_variants": sorted(place["all_variants"]),
                "data": {
                    "name": place["name"],
                    "geonames_id": place["geonames_id"],
                    "wikidata_id": place["wikidata_id"],
                    "nodegoat_id": place["nodegoat_id"],
                }
            }

            for variant in place["all_variants"]:
                norm_name = self._normalize_place_name(variant)
                if norm_name and norm_name not in name_map:
                    name_map[norm_name] = place_entry
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
            logging.debug(f"Normalisierte Eingabe: '{input_place}' → '{normalized_input}'")

            if input_place.lower() in self.known_name_map:
                entry = self.known_name_map[input_place.lower()]
                alternate_str = ";".join(sorted(entry["all_variants"]))
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

            if normalized_input in self.known_name_map:
                entry = self.known_name_map[normalized_input]
                alternate_str = ";".join(sorted(entry["all_variants"]))
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

            if best_score >= self.threshold:
                entry = self.known_name_map[best_match]
                alternate_str = ";".join(sorted(entry["all_variants"]))
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

            return None

        except Exception as e:
            logging.warning(f"Fehler beim Orts-Matching: {e}")
            return None

    def is_known_place(self, input_place: str):
        return self.match_place(input_place) is not None
    

    def deduplicate_places(
            self,
            raw_places: List[Dict[str, Any]],
            document_id: Optional[str] = None
        ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            """
            Entfernt Duplikate in raw_places und teilt auf in:
            - matched: Orte mit nodegoat_id
            - unmatched: Orte ohne nodegoat_id, ergänzt um document_id
            Dubletten (gleiche nodegoat_id oder normalisierter Name) werden einmalig ausgegeben.
            """
            seen = set()
            matched = []
            unmatched = []

            for pl in raw_places:
                # Key: nodegoat_id wenn vorhanden, sonst normalisierte Name
                key = pl.get("data", {}).get("nodegoat_id") or self._normalize_place_name(pl.get("matched_raw_input", pl.get("name", "")))
                if not key or key in seen:
                    continue
                seen.add(key)

                if pl.get("data", {}).get("nodegoat_id"):
                    matched.append(pl)
                else:
                    entry = pl.copy()
                    if document_id:
                        entry["document_id"] = document_id
                    unmatched.append(entry)

            return matched, unmatched
