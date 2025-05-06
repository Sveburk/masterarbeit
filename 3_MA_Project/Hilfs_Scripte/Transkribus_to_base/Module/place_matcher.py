import pandas as pd
from rapidfuzz import process, fuzz
import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from Module.document_schemas import Place
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
    def __init__(self, csv_path, threshold=80):
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
        name = name.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
        name = re.sub(r"[/\-\.(),]", " ", name)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\b(am|an|bei|in|auf|von|zu|zum|zur|a|i|der|die|das|im|aus|und|unsere)\b", "", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()
    
    def _build_match_result(self, entry, raw_input, score, method):
        alternate_str = ";".join(entry["all_variants"])
        return {
            "matched_name": entry["matched_name"],
            "matched_raw_input": raw_input.strip(),
            "score": score,
            "confidence": method,
            "data": {
                **entry["data"],
                "alternate_place_name": alternate_str
            }
        }


    

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

            def _insert_if_better(key: str, new_entry: dict):
                existing = name_map.get(key)
                new_has_id = new_entry["data"].get("nodegoat_id") or new_entry["data"].get("geonames_id") or new_entry["data"].get("wikidata_id")
                old_has_id = existing and (existing["data"].get("nodegoat_id") or existing["data"].get("geonames_id") or existing["data"].get("wikidata_id"))
                if key not in name_map or (new_has_id and not old_has_id):
                    name_map[key] = new_entry

            for variant in place["all_variants"]:
                norm_name = self._normalize_place_name(variant)
                _insert_if_better(norm_name, place_entry)
                _insert_if_better(variant.lower(), place_entry)

        logging.info(f"Insgesamt {len(name_map)} Ortsnamen-Varianten im Index geladen")
        if "muenchen" in name_map:
            print("[DEBUG] Finaler 'muenchen'-Eintrag:", name_map["muenchen"])
        return name_map



    def match_place(self, input_place: str):
        if not input_place or not input_place.strip():
            print(f"[DEBUG] Kein Match für '{input_place}' obwohl name_map-Eintrag existiert? → key: '{normalized_input}', has: {normalized_input in self.known_name_map}")

            return None

        try:
            normalized_input = self._normalize_place_name(input_place)
            print(f"[DEBUG] Versuche Matching für Eingabe-Ort: '{input_place}' (normalisiert: '{normalized_input}')")
            variants = [v for v in self.known_name_map.keys() if len(v) > 3]

            # 1) Exact match first (original)
            if normalized_input in self.known_name_map:
                entry = self.known_name_map[normalized_input]
                return self._build_match_result(entry, input_place, 100, "exact_normalized")

            # 2. Fallback: lowercase raw name
            raw_lower = input_place.lower()
            if raw_lower in self.known_name_map:
                entry = self.known_name_map[raw_lower]
                return self._build_match_result(entry, input_place, 100, "exact_raw")


            # 3) Partial-Fuzzy auf raw (mit Längencheck!)
            best_match, best_score, _ = process.extractOne(
                normalized_input,
                [v for v in self.known_name_map.keys() if len(v)>3],
                scorer=fuzz.partial_ratio
            )
            print(f"[DEBUG] (partial fuzzy) Best Match für '{input_place.strip()}': '{best_match}' mit Score {best_score}")

            if best_score >= self.threshold:
                if len(best_match) >= 0.5 * len(input_place):
                    entry = self.known_name_map[best_match]
                    return self._build_match_result(entry, input_place, best_score, f"fuzzy_partial ({best_score})")
                else:
                    print(f"[DEBUG] Best Match '{best_match}' ist zu kurz für Eingabe '{input_place}'")

            # 4) Andere Fuzzy-Strategien auf normalized
            match_strategies = [
                ("token_sort_ratio", fuzz.token_sort_ratio),
                ("token_set_ratio", fuzz.token_set_ratio),
                ("partial_ratio", fuzz.partial_ratio)
            ]

            best_match, best_score, best_method = None, 0, ""
            for method_name, scorer in match_strategies:
                candidate, score, _ = process.extractOne(
                    normalized_input,
                    variants,
                    scorer=scorer
                )
                if score > best_score:
                    best_match, best_score, best_method = candidate, score, method_name

            if best_score >= self.threshold:
                entry = self.known_name_map[best_match]
                return self._build_match_result(entry, input_place, best_score, f"fuzzy ({best_method})")

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
        - matched: alle Treffer (auch fuzzy ohne nodegoat_id)
        - unmatched: nur die mit matched_name=None
        
        Optimierte Version mit nodegoat_id-Priorisierung und Gruppierung
        """
        place_groups = {}
        matched = []
        unmatched = []

        # 1. Phase: Gruppiere Orte nach nodegoat_id oder normalisiertem Namen
        for pl in raw_places:
            data = pl.get("data", {})
            # Primärer Schlüssel: nodegoat_id (falls vorhanden) oder normalisierter Name
            key = data.get("nodegoat_id") or self._normalize_place_name(pl.get("matched_raw_input", ""))
            
            if not key:
                continue
                
            if key not in place_groups:
                place_groups[key] = []
            place_groups[key].append(pl)
        
        # 2. Phase: Wähle für jede Gruppe den besten Eintrag aus
        for key, group in place_groups.items():
            # Priorisiere Einträge mit nodegoat_id
            entries_with_id = [p for p in group if p.get("data", {}).get("nodegoat_id")]
            
            if entries_with_id:
                # Nimm den Eintrag mit dem höchsten Score
                best_entry = sorted(entries_with_id, key=lambda p: p.get("score", 0), reverse=True)[0]
                
                # Sammle alle original inputs für alternate_place_name
                orig_inputs = set()
                for entry in group:
                    orig_input = entry.get("matched_raw_input", "")
                    if orig_input and orig_input != best_entry.get("matched_name", ""):
                        orig_inputs.add(orig_input)
                
                # Füge Originaleinträge zu alternate_place_name hinzu, wenn nicht schon dort
                if orig_inputs:
                    alt_names = best_entry.get("data", {}).get("alternate_place_name", "").split(";")
                    alt_names.extend(orig_inputs)
                    alt_names = [n.strip() for n in alt_names if n.strip()]
                    best_entry["data"]["alternate_place_name"] = ";".join(set(alt_names))
                
                matched.append(best_entry)
            else:
                # Kein Eintrag mit ID, prüfe ob es matched einträge gibt
                matched_entries = [p for p in group if p.get("matched_name")]
                if matched_entries:
                    # Höchster Score zuerst
                    best_entry = sorted(matched_entries, key=lambda p: p.get("score", 0), reverse=True)[0]
                    matched.append(best_entry)
                else:
                    # Kein Match gefunden, nimm einfach den ersten Eintrag
                    entry = group[0].copy()
                    if document_id:
                        entry["document_id"] = document_id
                    unmatched.append(entry)

        return matched, unmatched
    
    def _extract_name_only(self, raw: str) -> str:
        return raw.split(",", 1)[0]
        
    def enrich_and_deduplicate(
        self,
        raw_places: List[Dict[str, Any]],
        document_id: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Full pipeline:
         1) match_place auf jeden raw_place["name"] anwenden
         2) deduplicate_places auf das Ergebnis
        """
        enriched = []
        for raw in raw_places:
            place_str = raw.get("name", "").strip()
            match = self.match_place(place_str)
            if match:
                enriched.append(match)
            else:
                enriched.append({
                    "matched_name": None,
                    "matched_raw_input": place_str,
                    "score": 0,
                    "confidence": "none",
                    "data": {
                        "name": "",
                        "geonames_id": "",
                        "wikidata_id": "",
                        "nodegoat_id": ""
                        }

                })
        return self.deduplicate_places(enriched, document_id=document_id)
    


def mentioned_places_from_custom_data(
    custom_data: Dict[str, Any],
    full_doc_id: str,
    place_matcher: Any,  # erwartet eine Instanz von PlaceMatcher
    get_place_name_fn = None  # Optional: Funktion zur Namensbereinigung
) -> List[Place]:
    """
    Extrahiert deduplizierte Place-Objekte aus custom_data["places"].

    Args:
        custom_data: Dictionary mit Custom-Tags inkl. "places"
        full_doc_id: ID des Dokuments (für Logging, Matching etc.)
        place_matcher: Instanz von PlaceMatcher (mit deduplicate_places-Methode)
        get_place_name_fn: optionale Funktion zur Extraktion des besten Ortsnamens
    
    Returns:
        Liste von Place-Objekten
    """
    raw_places = [
        {
            "matched_name": pl.get("matched_name", pl.get("name", "")),
            "matched_raw_input": pl.get("original_input", pl.get("name", "")),
            "score": pl.get("match_score", 0),
            "confidence": pl.get("confidence", "unknown"),
            "data": {
                "name": get_place_name_fn(pl) if get_place_name_fn else pl.get("name", ""),
                "alternate_place_name": pl.get("alternate_name", ""),
                "geonames_id": pl.get("geonames_id", ""),
                "wikidata_id": pl.get("wikidata_id", ""),
                "nodegoat_id": pl.get("nodegoat_id", "")
            }
        }
        for pl in custom_data.get("places", [])
    ]

    matched_places, _ = place_matcher.deduplicate_places(raw_places, document_id=full_doc_id)

    return [
        Place(
            name=mp["data"].get("name", ""),
            type="",  # ggf. später differenzieren
            alternate_place_name=mp["data"].get("alternate_place_name", ""),
            geonames_id=mp["data"].get("geonames_id", ""),
            wikidata_id=mp["data"].get("wikidata_id", ""),
            nodegoat_id=mp["data"].get("nodegoat_id", "")
        )
        for mp in matched_places
    ]