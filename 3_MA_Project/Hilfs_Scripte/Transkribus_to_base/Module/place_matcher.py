import pandas as pd
from rapidfuzz import process, fuzz
import logging
import requests
import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Tuple
from Module.document_schemas import Place
#=====================================
# region #Vorbereitung der Datenbasis

def sanitize_id(v) -> str:
    """
    Gibt eine ID exakt so zurück, wie sie im CSV steht – als String.
    Kein Entfernen von '.0', keine Umwandlung zu int, keine Kürzung.
    Nur leere oder NaN-Werte werden gefiltert.
    """
    if pd.isna(v) or str(v).strip() == "":
        return ""
    return str(v).strip()

def safe_split_semicolon(val):
    """
    Gibt eine Liste von Strings zurück, egal ob Input String, List oder Dict ist.
    - Bei String: .split(";")
    - Bei List:  einfach zurückgeben
    - Bei Dict:  Werte als Liste zurückgeben
    - Sonst:     leere Liste
    """
    if isinstance(val, str):
        return [v.strip() for v in val.split(";") if v.strip()]
    elif isinstance(val, list):
        return val
    elif isinstance(val, dict):
        # Im Fehlerfall: alles als String nehmen
        print(f"[WARN] safe_split_semicolon: Dict statt String/List – Inhalt: {val}")
        return list(val.values())
    else:
        print(f"[WARN] safe_split_semicolon: Unerwarteter Typ: {type(val)} – Inhalt: {val}")
        return []


def extract_places_from_textline_custom(custom: str) -> List[Dict[str, str]]:
    """
    Extrahiert alle Orte (und optionale IDs, placeName etc.) aus einem Custom-String.
    Rückgabe: Liste von Dicts mit allen vorhandenen Feldern.
    """
    place_pattern = r"place\s*\{([^}]*)\}"
    results = []
    for m in re.finditer(place_pattern, custom):
        place_str = m.group(1)
        d = {}
        print(type(place_str), place_str)
        # Hier die sichere Funktion nutzen:
        for field in safe_split_semicolon(place_str):
            if ":" in field:
                k, v = field.split(":", 1)
                d[k.strip()] = v.strip()
        results.append(d)
    return results

def extract_place_lines_from_xml(xml_root: ET.Element) -> List[Dict[str, Any]]:
    lines = []
    for tl in xml_root.findall(".//{*}TextLine"):
        custom = tl.attrib.get("custom", "")
        if "place" in custom:
            unicode_el = tl.find(".//{*}Unicode")
            line_text = unicode_el.text.strip() if unicode_el is not None and unicode_el.text else ""
            place_pattern = r"(place|creation_place|recipient_place)\s*\{([^}]*)\}"
            place_tags = []
            for m in re.finditer(place_pattern, custom):
                tag = m.group(1)
                fields_str = m.group(2)
                d = {"tag": tag}
                for field in safe_split_semicolon(fields_str):
                    if isinstance(field, str) and ":" in field:
                        k, v = field.split(":", 1)
                        d[k.strip()] = v.strip()
                    else:
                        print(f"[WARN] Unerwarteter Typ/Feld in place_str: {field} ({type(field)})")

                # Extrahiere Namen aus offset/length, falls vorhanden
                if "offset" in d and "length" in d and line_text:
                    try:
                        offset = int(d["offset"])
                        length = int(d["length"])
                        raw_name = line_text[offset:offset+length]
                        d["raw_extractedName"] = raw_name
                        # initialer extracted
                        d["extracted_placeName"] = raw_name
                    except Exception:
                        d["raw_extractedName"] = ""
                        d["extracted_placeName"] = ""
                # placeName explizit im Tag überschreibt nur extracted_placeName,
                # raw_extractedName bleibt erhalten
                if "placeName" in d and d["placeName"]:
                    d["extracted_placeName"] = d["placeName"]
                place_tags.append(d)
            lines.append({
                "line_text": line_text,
                "places": place_tags
            })
    return lines

# endregion

#=====================================
# region #Initialisierung des PlaceMatchers
class PlaceMatcher:
    def safe_split_semicolon(self, value):
        if isinstance(value, str):
            return value.split(";")
        elif isinstance(value, list):
            return value
        elif isinstance(value, dict):
            return list(value.values())
        else:
            return []

    def __init__(self, csv_path, threshold=80, geonames_login="demo"):
        self.unmatched_places: List[Dict[str, Any]] = []
        """
        Initialisiert den PlaceMatcher mit Pfad zur CSV-Datei, Match-Threshold und optionalem Geonames-Login.
        
        Args:
            csv_path (str): Pfad zur CSV-Datei mit bekannten Orten.
            threshold (int): Fuzzy-Matching-Schwelle.
            geonames_login (str): Benutzername für Geonames API.
        """
        self.threshold = threshold
        self.geonames_login = geonames_login  # NEU: Login für API-Lookup

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

            self.alt_name_to_main = {}
            for _, row in self.places_df.iterrows():
                alt_names_raw = str(row.get("alternate_place_name", "")).lower()
                alt_names = [name.strip() for name in safe_split_semicolon(alt_names_raw) if name.strip()]                
                main_name = row.get("name")
                for alt in alt_names:
                    self.alt_name_to_main[alt] = main_name

            self.known_name_map = self._build_known_place_map()

        except Exception as e:
            logging.error(f"Fehler beim Laden der Ortsdaten aus {csv_path}: {e}")
            self.places_df = pd.DataFrame()
            self.known_name_map = {}
    
        self.surrounding_place_lines: List[Dict[str, Any]] = []
    
    def log_unmatched_place(self, name: str, reason: str, geonames_id=None, wikidata_id=None, nodegoat_id=None):
        """
        Dedupliziert und speichert einen nicht übernommenen Ort in self.unmatched_places
        """
        key = (name.strip().lower(), geonames_id or "", wikidata_id or "", nodegoat_id or "")
        already_logged = any(
            (entry["input"].strip().lower(), entry.get("geonames_id", ""), entry.get("wikidata_id", ""), entry.get("nodegoat_id", ""))
            == key
            for entry in self.unmatched_places
        )
        if not already_logged:
            self.unmatched_places.append({
                "input": name.strip(),
                "geonames_id": geonames_id or "",
                "wikidata_id": wikidata_id or "",
                "nodegoat_id": nodegoat_id or "",
                "reason": reason
            })
    #endregion    
    
    #======================================
    #region Normierung und Kontext-Matching

    def _normalize_place_name(self, name: str) -> str:
        name = name.lower()
        name = name.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
        name = re.sub(r"[/\-\.(),]", " ", name)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\b(am|an|bei|in|auf|von|zu|zum|zur|a|i|der|die|das|im|aus|und|unsere)\b", "", name)
        name = re.sub(r"\s+", " ", name)
        return name.strip()
    def _generate_combined_place_names(self, window_size: int = 3) -> set:
        """
        Erzeugt kombinierte Ortsnamen aus benachbarten Zeilen ±1 und Wörter ±window_size.
        Nutzt self.surrounding_place_lines.
        """
        def tokenize(line):
            if isinstance(line, dict):
                if "line_text" in line:
                    line = line["line_text"]
                else:
                    print(f"[WARN] Kein String in tokenize: {line} ({type(line)})")
                    return []
            if not isinstance(line, str):
                print(f"[WARN] Kein String in tokenize: {line} ({type(line)})")
                return []
            return [w.strip(".,;()[]").lower() for w in line.split() if w.strip(".,;()[]")]


        lines_tokens = [tokenize(line) for line in self.surrounding_place_lines]
        combined_names = set()

        for i, tokens in enumerate(lines_tokens):
            # Mehrwortkombis in eigener Zeile
            for start in range(len(tokens)):
                for end in range(start, min(start + window_size, len(tokens))):
                    combined_names.add(" ".join(tokens[start:end+1]))

            # Kombinationen mit Nachbarzeilen (±1)
            neighbors = []
            if i > 0:
                neighbors.append(lines_tokens[i-1])
            if i < len(lines_tokens) - 1:
                neighbors.append(lines_tokens[i+1])

            for neighbor_tokens in neighbors:
                for start1 in range(len(tokens)):
                    for end1 in range(start1, min(start1 + window_size, len(tokens))):
                        window1 = tokens[start1:end1+1]

                        for start2 in range(len(neighbor_tokens)):
                            for end2 in range(start2, min(start2 + window_size, len(neighbor_tokens))):
                                window2 = neighbor_tokens[start2:end2+1]

                                combined_names.add(" ".join(window1 + window2))
                                combined_names.add("-".join(window1 + window2))

        return combined_names
    #endregiom
    #======================================
    #region matching-strategien

    def _build_match_result(self, entry, raw_input, score, method):
        alternate_str = ";".join(entry["all_variants"])
        data = entry["data"].copy()

        # Nur ergänzen, wenn leer – keine Überschreibung erlauben
        if not data.get("geonames_id"):
            api_geonames_id = lookup_geonames(raw_input, username=self.geonames_login)
            if api_geonames_id:
                data["geonames_id"] = api_geonames_id
                data["needs_review"] = True
                print(f"[API-MATCH] Geonames-ID gefunden für '{raw_input}': {api_geonames_id}")

        if not data.get("wikidata_id"):
            api_wikidata_id = lookup_wikidata(raw_input)
            if api_wikidata_id:
                data["wikidata_id"] = api_wikidata_id
                data["needs_review"] = True
                print(f"[API-MATCH] Wikidata-ID gefunden für '{raw_input}': {api_wikidata_id}")

        data = enrich_with_wikidata_if_missing(data)

        return {
            "matched_name": entry["matched_name"],
            "matched_raw_input": raw_input.strip(),
            "score": score,
            "confidence": method,
            "data": {
                **data,
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
                    alt_names_split = [n.strip() for n in safe_split_semicolon(alt_name) if n.strip()]
                    for alt in alt_names_split:
                        merged_places[place_id]["all_variants"].add(alt)


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

            # Kombinierte Ortsnamen als Schlüssel hinzufügen
            for variant in place["all_variants"]:
                norm_name = self._normalize_place_name(variant)
                _insert_if_better(norm_name, place_entry)
                _insert_if_better(variant.lower(), place_entry)

                if "-" in variant:
                    norm_dash = self._normalize_place_name(variant)
                    _insert_if_better(norm_dash, place_entry)
                    _insert_if_better(variant.lower(), place_entry)
                if " " in variant:
                    norm_space = self._normalize_place_name(variant)
                    _insert_if_better(norm_space, place_entry)
                    _insert_if_better(variant.lower(), place_entry)
                
            variants_list = sorted(place["all_variants"])
            for i, v1 in enumerate(variants_list):
                norm_v1 = self._normalize_place_name(v1)
                for j, v2 in enumerate(variants_list):
                    if i != j:
                        norm_v2 = self._normalize_place_name(v2)
                        combo_space = f"{norm_v1} {norm_v2}"
                        combo_dash  = f"{norm_v1}-{norm_v2}"
                        _insert_if_better(combo_space, place_entry)
                        _insert_if_better(combo_dash, place_entry)

        logging.info(f"Insgesamt {len(name_map)} Ortsnamen-Varianten im Index geladen")
        return name_map

    #endregion

    #=========================
#region # Matching-Logik und Kontextverarbeitung
    def _match_combined_place_from_context(self, input_place: str) -> Optional[List[Dict[str, Any]]]:
        combined_names = self._generate_combined_place_names()

        normalized_input = self._normalize_place_name(input_place)

        for combined_name in combined_names:
            # Prüfe nur Kombis, die den input_place enthalten (oder ggf. andere Heuristik)
            if normalized_input in combined_name or combined_name in normalized_input:
                # Suche in Groundtruth in der Spalte 'alternate_place_name'
                matched_main_name = self._find_main_place_for_alternative(combined_name)
                if matched_main_name:
                    norm_main = self._normalize_place_name(matched_main_name)
                    entry = self.known_name_map.get(norm_main)
                    if entry:
                        # Baue den Match Result zurück
                        return [self._build_match_result(entry, combined_name, 105, "combined_context_match")]
        return None

    def _find_main_place_for_alternative(self, alt_name: str) -> Optional[str]:
        return self.alt_name_to_main.get(alt_name.lower().strip())



    def match_place(self, input_place: str) -> Optional[List[Dict[str, Any]]]:
        if not self.is_valid_place_name(input_place):
            print(f"[DEBUG] Dropping invalid place input: '{input_place}'")
            self.log_unmatched_place(input_place, "invalid_name_heuristic")
            return None
        for line in self.surrounding_place_lines:
            if isinstance(line, dict):
                for tag in line.get("places", []):
                    # wir vergleichen jetzt mit raw_extractedName, nicht mit placeName!
                    if tag.get("raw_extractedName") == input_place and tag.get("placeName"):
                        real_name = tag["placeName"]
                        # Ground-Truth-Lookup im DataFrame
                        gt_df = self.places_df[self.places_df["name"] == real_name]
                        if not gt_df.empty:
                            row = gt_df.iloc[0].to_dict()
                            return [ self._build_match_result(
                                entry={
                                    "matched_name": row["name"],
                                    "all_variants": [row["name"]],
                                    "data": row
                                },
                                raw_input=input_place,
                                score=120,
                                method="custom_tag_placeName"
                            ) ]
            if not input_place or not input_place.strip():
                print(f"[DEBUG] Empty input_place: '{input_place}'")
                return None

        try:
            normalized_input = self._normalize_place_name(input_place)

            # 0) Kombinierte Ortsnamen aus Kontext
            combined_match = self._match_combined_place_from_context(input_place)
            if combined_match:
                return combined_match  # 🔁 LISTEN-WRAPPING

            variants = [v for v in self.known_name_map.keys() if len(v) > 3]

            # 1) Exact match (normalized)
            if normalized_input in self.known_name_map:
                entry = self.known_name_map[normalized_input]
                return [self._build_match_result(entry, input_place, 100, "exact_normalized")]

            # 2) Fallback: exact match (raw lowercased)
            raw_lower = input_place.lower()
            if raw_lower in self.known_name_map:
                entry = self.known_name_map[raw_lower]
                return [self._build_match_result(entry, input_place, 100, "exact_raw")]

            # 3) Partial-Fuzzy auf raw
            best_match, best_score, _ = process.extractOne(
                normalized_input,
                variants,
                scorer=fuzz.partial_ratio
            )
            print(f"[DEBUG] (partial fuzzy) Best Match für '{input_place.strip()}': '{best_match}' mit Score {best_score}")

            if best_score >= self.threshold:
                if len(best_match) >= 0.5 * len(input_place):
                    entry = self.known_name_map[best_match]
                    return [self._build_match_result(entry, input_place, best_score, f"fuzzy_partial ({best_score})")]
                else:
                    print(f"[DEBUG] Best Match '{best_match}' ist zu kurz für Eingabe '{input_place}'")
                    self.log_unmatched_place(input_place, "best match too short")
                    return None

            # 4) Weitere Fuzzy-Matchstrategien
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
                return [self._build_match_result(entry, input_place, best_score, f"fuzzy ({best_method})")]

            # 5) Geonames/Wikidata Lookup
            print(f"[INFO] Kein lokaler Treffer für Ort: '{input_place}'. Versuche Geonames/Wikidata-Lookup.")
            geonames_result_id = lookup_geonames(input_place, username=self.geonames_login)
            wikidata_result_id = lookup_wikidata(input_place)

            # Zusammenbauen des Datenobjekts
            unmatched_entry = {
                "matched_name": None,
                "matched_raw_input": input_place.strip(),
                "score": 0,
                "confidence": "external_lookup",
                "data": {
                    "name": input_place.strip(),
                    "geonames_id": geonames_result_id or "",
                    "wikidata_id": wikidata_result_id or "",
                    "nodegoat_id": "",
                    "alternate_place_name": "",
                    "needs_review": True
                }
            }

            # In die unmatched-Liste eintragen
            self.log_unmatched_place(
                name=input_place,
                reason="not in Groundtruth; external lookup used",
                geonames_id=geonames_result_id,
                wikidata_id=wikidata_result_id,
                nodegoat_id=""
            )


            return [unmatched_entry]


        except Exception as e:
            logging.warning(f"Fehler beim Orts-Matching: {e}")
            print(f"[ERROR] Fehler beim Matching von '{input_place}': {e}")
            return None

    def is_known_place(self, input_place: str):
        result = self.match_place(input_place)
        return bool(result)  
    

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
                    alt_name_field = best_entry.get("data", {}).get("alternate_place_name", "")

                    # Sicherstellen, dass wir eine Liste von Namen bekommen, egal ob input String, List oder Dict
                    if isinstance(alt_name_field, str):
                        alt_names = safe_split_semicolon(alt_name_field)
                    elif isinstance(alt_name_field, list):
                        alt_names = alt_name_field
                    elif isinstance(alt_name_field, dict):
                        print(f"[WARN] alternate_place_name ist Dict, kein String! Inhalt: {alt_name_field}")
                        alt_names = list(alt_name_field.values())
                    else:
                        print(f"[WARN] Unerwarteter Typ für alternate_place_name: {type(alt_name_field)} Inhalt: {alt_name_field}")
                        alt_names = []

                    alt_names.extend(orig_inputs)
                    # Einmal alles in Strings umwandeln und sauber trimmen
                    alt_names = [str(n).strip() for n in alt_names if n and str(n).strip()]
                    # Duplikate entfernen, wieder als String speichern
                    best_entry["data"]["alternate_place_name"] = ";".join(sorted(set(alt_names)))

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
        if isinstance(raw, dict):
            raw = raw.get("name") or raw.get("extracted_placeName") or ""
        return str(raw).split(",", 1)[0]
    def is_valid_place_name(self, name: str) -> bool:
        """
        Verhindert offensichtlichen Datenmüll bei Ortsnamen.
        """
        name = name.strip()

        if not name:
            return False

        # Keine reinen Zahlen oder Jahreszahlen
        if re.fullmatch(r'\d{2,4}(/\d{2,4})?', name):
            return False

        # Bindestrich-Fragment ohne Vokal
        if '-' in name and not re.search(r'[aeiouäöüAEIOUÄÖÜ]', name):
            return False

        # Muss mindestens 3 Zeichen haben
        if len(name) < 1:
            return False

        # Muss Buchstaben enthalten
        if not re.search(r'[A-Za-zÄÖÜäöü]', name):
            return False

        # Blacklist
        blacklist = {"geburtstag", "märz", "jahrgang", "ge-", "geb-", "den."}
        if name.lower() in blacklist:
            return False

        return True

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
        print(f"[DEBUG] Matching '{place_str}' → {match} type: {type}")
        if match:
            enriched.extend(match)  # wichtig: statt append
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

    # ✅ SCHUTZ: Flache Liste sicherstellen (falls irgendwo append statt extend passiert ist)
    enriched = [item for sublist in enriched for item in (sublist if isinstance(sublist, list) else [sublist])]

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


def match_place_with_custominfo(place_matcher: PlaceMatcher, line_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Erwartet ein Dict: {"line_text": ..., "place_tags": [...]}
    Nutzt place_tags für IDs oder placeName, und merged ggf. den Zeilentext als Variante.
    Gibt immer eine Liste von Matches zurück.
    """
    matches = []

    # 1. Priorisiere Wikidata/Geonames im Tag
    for tag in line_entry.get("place_tags", []):
        gt_entry = None
        id_val = None

        # -- Wikidata bevorzugt --
        if "wikiData" in tag or "wikidata" in tag:
            id_val = tag.get("wikiData") or tag.get("wikidata")
            gt_df = place_matcher.places_df[place_matcher.places_df["wikidata_id"] == id_val]
            if not gt_df.empty:
                row = gt_df.iloc[0].to_dict()
                row["matched_name"] = row.get("name", "")
                row["matched_raw_input"] = tag.get("placeName", line_entry.get("line_text", ""))
                row["score"] = 120
                row["confidence"] = "custom_tag_wikidata"
                matches.append({
                    "matched_name": row["matched_name"],
                    "matched_raw_input": row["matched_raw_input"],
                    "score": row["score"],
                    "confidence": row["confidence"],
                    "data": row,
                })
                continue  # **Niemals Fuzzy, wenn Wikidata passt!**
            else:
                # Wikidata-ID nicht im Groundtruth → trotzdem aufnehmen
                matches.append({
                    "matched_name": tag.get("placeName", ""),
                    "matched_raw_input": line_entry.get("line_text", ""),
                    "score": 100,
                    "confidence": "custom_tag_wikidata",
                    "data": {
                        "name": tag.get("placeName", ""),
                        "wikidata_id": id_val,
                        "alternate_place_name": line_entry.get("line_text", ""),
                        "matched_name": tag.get("placeName", ""),
                        "matched_raw_input": line_entry.get("line_text", ""),
                        "score": 100,
                        "confidence": "custom_tag_wikidata",
                    },
                })
                continue

        # -- Geonames als zweite Priorität --
        elif "geonames" in tag or "geonames_id" in tag:
            id_val = tag.get("geonames") or tag.get("geonames_id")
            gt_df = place_matcher.places_df[place_matcher.places_df["geonames_id"] == id_val]
            if not gt_df.empty:
                row = gt_df.iloc[0].to_dict()
                row["matched_name"] = row.get("name", "")
                row["matched_raw_input"] = tag.get("placeName", line_entry.get("line_text", ""))
                row["score"] = 110
                row["confidence"] = "custom_tag_geonames"
                matches.append({
                    "matched_name": row["matched_name"],
                    "matched_raw_input": row["matched_raw_input"],
                    "score": row["score"],
                    "confidence": row["confidence"],
                    "data": row,
                })
                continue

        # -- placeName als dritter Versuch --
        elif "placeName" in tag:
            name = tag.get("placeName")
            name_matches = place_matcher.match_place(name)
            if name_matches:
                for nm in name_matches:
                    nm_data = nm["data"].copy()
                    nm_data["matched_name"] = nm.get("matched_name")
                    nm_data["matched_raw_input"] = nm.get("matched_raw_input")
                    nm_data["score"] = 105
                    nm_data["confidence"] = "custom_tag_placeName"
                    matches.append({
                        "matched_name": nm_data["matched_name"],
                        "matched_raw_input": nm_data["matched_raw_input"],
                        "score": nm_data["score"],
                        "confidence": nm_data["confidence"],
                        "data": nm_data,
                    })
                continue

    # Fallback: Wenn kein Tag-Match → Zeilentext als Fuzzy
    if not matches:
        fallback_matches = place_matcher.match_place(line_entry.get("line_text", ""))
        if fallback_matches:
            for fm in fallback_matches:
                matches.append(fm)

    # Immer eine **Liste** zurückgeben
    return matches


def lookup_geonames(place_name: str, username: str) -> Optional[str]:
    url = "http://api.geonames.org/searchJSON"
    params = {
        "q": place_name,
        "maxRows": 1,
        "username": username,
        "lang": "de"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if data.get("geonames"):
            return str(data["geonames"][0].get("geonameId", ""))
    except Exception as e:
        print(f"[WARN] Geonames-Fehler bei '{place_name}': {e}")
    return None



def enrich_with_wikidata_if_missing(place: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ergänzt Wikidata-ID, falls noch keine vorhanden und ein spezifischer Wikidata-Match bekannt ist.
    Beispiel: 'Pfänder' → 'Q698608'
    """
    known_wikidata_matches = {
        "pfänder": "Q698608",
        # weitere manuelle Ausnahmen hier ergänzen
    }

    name_norm = place.get("name", "").lower().strip()
    if not place.get("wikidata_id") and name_norm in known_wikidata_matches:
        place["wikidata_id"] = known_wikidata_matches[name_norm]
        print(f"[WIKIDATA-FIX] Ergänze Wikidata-ID für '{place['name']}': {place['wikidata_id']}")
    return place

def lookup_wikidata(place_name: str) -> Optional[str]:
    """
    Fragt Wikidata via SPARQL ab, ob es einen Ortseintrag mit diesem Label gibt.
    Gibt die Q-ID zurück, falls gefunden.
    """
    query = f"""
    SELECT ?place WHERE {{
      ?place rdfs:label "{place_name}"@de .
      ?place wdt:P31/wdt:P279* wd:Q486972 .  # wd:Q486972 = 'human settlement'
    }}
    LIMIT 1
    """
    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "Transkribus2Base/1.0 (mailto:YOUR_MAIL@example.com)"
    }

    try:
        response = requests.get(url, params={"query": query}, headers=headers, timeout=10)
        response.raise_for_status()  # Wirft Exception bei HTTP-Fehlern
        results = response.json().get("results", {}).get("bindings", [])
        if results:
            qid = results[0]["place"]["value"].split("/")[-1]
            print(f"[WIKIDATA] Treffer für '{place_name}': {qid}")
            return qid
        else:
            print(f"[WIKIDATA] Kein Wikidata-Treffer für '{place_name}'")
    except Exception as e:
        print(f"[WARN] Wikidata-Fehler bei '{place_name}': {e}")

    return None

def consolidate_places(custom_data: Dict[str, Any],
                       place_matcher: PlaceMatcher
                      ) -> Dict[str, Any]:
    """
    Nimmt custom_data mit einer Liste aller place-Tags (egal ob
    'place', 'creation_place' oder 'recipient_place') und
    liefert custom_data inklusive deduplizierter, gematchter Felder
    'creation_place' und 'recipient_place'.

    Erwartet in custom_data:
      custom_data['places_raw'] = [
        {'tag': 'place',            'raw': 'Murg'},
        {'tag': 'creation_place',   'raw': 'Murg (Baden)'},
        {'tag': 'recipient_place',  'raw': 'Berlin'},
        ...
      ]

    Gibt zurück:
      custom_data plus
      custom_data['creation_place']  = <matched_place_dict> oder None
      custom_data['recipient_place'] = <matched_place_dict> oder None
      custom_data['places_matched']  = [<matched_place_dict>, ...]
    """
    matched_by_raw = {}

    for entry in custom_data.get("places_raw", []):
        raw = entry["raw"].strip()
        if raw and raw not in matched_by_raw:
            try:
                m = place_matcher.match_place(raw)
                if isinstance(m, list) and m:
                    best = max(m, key=lambda x: x.get("score", 0))
                    matched_by_raw[raw] = best.get("data", {})
                elif isinstance(m, dict):  # Fallback für ältere Rückgabe
                    matched_by_raw[raw] = m.get("data", {})
                else:
                    matched_by_raw[raw] = {"name": raw, "needs_review": True}
            except Exception as e:
                print(f"[WARN] Fehler bei Ortsmatching für '{raw}': {e}")
                matched_by_raw[raw] = {"name": raw, "needs_review": True, "error": str(e)}

    unique = {}
    for raw, data in matched_by_raw.items():
        key = data.get("nodegoat_id") or data.get("name", "").lower()
        if not key:
            continue
        if key not in unique:
            unique[key] = data
        elif data.get("nodegoat_id") and not unique[key].get("nodegoat_id"):
            unique[key] = data

    creation = None
    recipient = None
    for entry in custom_data.get("places_raw", []):
        raw = entry["raw"].strip()
        tag = entry["tag"]
        data = matched_by_raw.get(raw)
        if not data:
            continue
        key = data.get("nodegoat_id") or data.get("name", "").lower()

        if tag == "creation_place" and unique.get(key):
            creation = unique[key]
        if tag == "recipient_place" and unique.get(key):
            recipient = unique[key]

    custom_data["places_matched"] = list(unique.values())
    custom_data["creation_place"] = creation
    custom_data["recipient_place"] = recipient
    return custom_data

