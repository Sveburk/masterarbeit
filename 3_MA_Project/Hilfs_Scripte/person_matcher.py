"""
Person Matcher - Fuzzy name matching for person entities

This module provides functions for fuzzy matching of person names to handle
spelling variations, nicknames, OCR errors, and different name orderings.
"""

from typing import List, Dict, Tuple, Optional, Any
import pandas as pd
from rapidfuzz import fuzz, process
import re
import os

# Pfad zur CSV-Datei mit bekannten Personen
CSV_PATH_KNOWN_PERSONS = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Datenbank_Metadaten_Stand_08.04.2025/Metadata_Person-Metadaten_Personen.csv"

def get_matching_thresholds() -> Dict[str, int]:
    """
    Liefert die Schwellenwerte (Thresholds) für die fuzzy Namensabgleiche.

    Returns:
        Dictionary mit individuellen Thresholds für 'forename' und 'familyname'
    """
    return {
        "forename": 90,
        "familyname": 85
    }


# Lade bekannte Personen aus der CSV
def load_known_persons_from_csv(csv_path: str = CSV_PATH_KNOWN_PERSONS) -> List[Dict[str, str]]:
    """
    Lädt die bekannten Personen aus der CSV-Datei.
    
    Args:
        csv_path: Pfad zur CSV-Datei
        
    Returns:
        Liste von Personen-Dictionaries mit 'forename' und 'familyname' Schlüsseln
    """
    if not os.path.exists(csv_path):
        print(f"Warnung: CSV-Datei nicht gefunden: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path, sep=";")
        persons = []
        
        # Extrahiere relevante Spalten
        for _, row in df.iterrows():
            forename = row.get("schema:givenName", "")
            familyname = row.get("schema:familyName", "")
            
            # Überspringe Einträge ohne Namen
            if not isinstance(forename, str):
                forename = ""
            if not isinstance(familyname, str):
                familyname = ""
                
            forename = forename.strip()
            familyname = familyname.strip()
            
            if forename or familyname:  # Mindestens ein Namensteil muss vorhanden sein
                person = {
                    "forename": forename,
                    "familyname": familyname,
                    "id": row.get("Lfd. No.", ""),
                    "home": row.get("schema:homeLocation", ""),
                    "birth_date": row.get("schema:birthDate", ""),
                    "death_date": row.get("schema:deathDate", ""),
                    "death_place": row.get("db:deathPlace", ""),
                    "alternate_name": row.get("schema:alternateName", "")
                }
                persons.append(person)
        
        return persons
    except Exception as e:
        print(f"Fehler beim Laden der CSV-Datei: {e}")
        return []

# Lade die bekannten Personen beim Modul-Import
KNOWN_PERSONS = load_known_persons_from_csv()

# Common German nicknames and name variations
NICKNAME_MAP = {
    # First names
    "albert": ["al", "bert"],
    "alexander": ["alex", "sasha", "sascha"],
    "alfred": ["fred", "freddy"],
    "andreas": ["andy", "andré"],
    "anton": ["toni", "tony"],
    "bernard": ["bernd", "bernie"],
    "christian": ["chris", "christl"],
    "daniel": ["dan", "danny"],
    "dieter": ["didi"],
    "eduard": ["edi", "eddy", "edy"],
    "ernst": ["erni"],
    "ferdinand": ["ferdi", "fred"],
    "franz": ["franzi", "franzl"],
    "friedrich": ["fritz", "fredi", "freddy"],
    "georg": ["jörg", "schorsch"],
    "gerhard": ["gerd", "gerdi", "hardy"],
    "gottfried": ["friedl", "gottfr"],
    "günther": ["günter", "gunter", "gunther"],
    "heinrich": ["heiner", "heinz", "henry"],
    "helmut": ["helm", "helmi"],
    "herbert": ["herb", "herbi", "herbie", "herby"],
    "hermann": ["hermi"],
    "johannes": ["hans", "hansi", "johann", "hannes"],
    "josef": ["joseph", "jupp", "sepp", "seppl"],
    "jürgen": ["jürg", "jurgen", "jurg"],
    "karl": ["carl", "kalli", "charly", "charlie"],
    "konrad": ["conrad", "conny", "konny"],
    "kurt": ["curt"],
    "ludwig": ["lutz"],
    "manfred": ["manni", "manny", "fred"],
    "max": ["maxi", "maximilian"],
    "michael": ["michi", "michel", "mike", "michl"],
    "nikolaus": ["klaus", "niko", "nico", "nicolas", "nikolai"],
    "norbert": ["norbi"],
    "otto": ["otti"],
    "paul": ["paula", "pauli"],
    "peter": ["pete", "petri", "piet"],
    "philipp": ["phil", "phillip", "filip"],
    "rainer": ["reiner", "reinhard", "reinhardt"],
    "richard": ["rick", "richi", "richie", "richy"],
    "robert": ["rob", "robby", "robin"],
    "rudolf": ["rolf", "rudi", "rudolph"],
    "siegfried": ["sigi", "siggi"],
    "stefan": ["stephan", "steffen", "steff"],
    "theodor": ["theo"],
    "thomas": ["tom", "tommy", "thom"],
    "walter": ["wolfi", "walti", "waldi"],
    "werner": ["weiner", "werni"],
    "wilhelm": ["willi", "willy", "will"],
    "wolfgang": ["wolf", "wolfi", "wolfy"],
    
    # Female names
    "adelheid": ["adel", "adele", "heidi"],
    "angela": ["angie", "angelika"],
    "anna": ["anni", "anny", "anneli", "anneliese"],
    "barbara": ["bärbel", "babsi", "barbi"],
    "brigitte": ["gitta", "gitti", "birgit"],
    "charlotte": ["lotte", "lottie", "charlie"],
    "christine": ["christina", "christl", "tina"],
    "dorothea": ["dora", "doris", "dörte"],
    "elisabeth": ["elise", "lisa", "lisbeth", "liesl"],
    "eleonore": ["eleanor", "lenore", "elli"],
    "elfriede": ["elfi", "elfie"],
    "emma": ["emmi", "emi"],
    "franziska": ["fanni", "franzi", "sissi"],
    "gabriele": ["gabi", "gabriela"],
    "gertrude": ["gerti", "gertrud", "trude", "trudi"],
    "gisela": ["gisi"],
    "hanna": ["hannah", "johanna"],
    "hedwig": ["hedy"],
    "helene": ["helena", "leni", "leny"],
    "henriette": ["henny", "jette"],
    "hildegard": ["hilde", "hildi"],
    "ilse": ["ilsa"],
    "ingeborg": ["inge", "ingrid"],
    "irene": ["irina", "reni"],
    "johanna": ["hanna", "hannah", "johanne"],
    "juliane": ["julia", "julie"],
    "karoline": ["caroline", "carola", "karolina"],
    "katharina": ["katarina", "kathrin", "kathi", "katrin", "kati", "katja"],
    "klara": ["clara", "klärchen"],
    "magdalena": ["magda", "lena", "lenchen"],
    "margarethe": ["margareta", "greta", "gretchen", "gretel", "meta"],
    "maria": ["marie", "mary", "mariechen", "mia"],
    "marianne": ["marion", "miriam"],
    "martha": ["marta"],
    "mathilde": ["matilda", "tilda", "hilde"],
    "monika": ["moni", "monica"],
    "renate": ["renata", "reni"],
    "rosemarie": ["rosi", "rosie"],
    "sabine": ["bine", "sabina"],
    "sophie": ["sofia", "sofie"],
    "stefanie": ["stephani", "steffi", "steffy"],
    "susanne": ["susi", "susanna", "suse"],
    "ursula": ["ursel", "uschi"],
    "veronika": ["vroni", "vera", "nika"],
    "waltraud": ["traudl", "waltraut"],
}

# Expand the nickname map to allow lookup by nickname
EXPANDED_NICKNAME_MAP = {}
for name, nicknames in NICKNAME_MAP.items():
    EXPANDED_NICKNAME_MAP[name] = name
    for nickname in nicknames:
        EXPANDED_NICKNAME_MAP[nickname] = name

# Common OCR errors in German names
OCR_ERRORS = {
     "ü": ["u", "ii", "il", "li"],
     "ä": ["a", "ii", "il", "li"],
     "ö": ["o", "ii", "il", "li"],
     "ß": ["ss", "sz", "s", "b"],
#     "m": ["rn", "nn", "in", "ni"],
#     "w": ["vv", "v"],
#     "n": ["ii", "il", "li"],
#     "h": ["b", "lh", "hl"],
#     "b": ["h", "lb", "bl"],
#     "c": ["e", "o"],
#     "e": ["c", "o"],
#     "i": ["l", "1", "j"],
#     "l": ["i", "1", "t"],
#     "rn": ["m"],
#     "cl": ["d"],
#     "d": ["cl"],
 }
def normalize_name_with_title(name: str) -> tuple[str, str]:
    """
    Trennt Titel/Anrede vom Namen und gibt beides zurück.
    
    Args:
        name (str): z. B. "Herr Dr. Alfons Zimmermann"
    
    Returns:
        Tuple aus (bereinigtem Namen, erkannter Titel), z. B. ("Alfons Zimmermann", "Herr Dr")
    """
    honorifics = [
        r"Herrn?", r"Frau", r"Fräulein", r"Witwe",
        r"Dr", r"Prof", r"Professor", r"Studienrat", r"Oberstudienrat",
        r"Dipl[-\.]?Ing", r"Ing", r"Lic\.? phil\.?", r"Lic\.? rer\.? pol\.?",
        r"Ehrenmitglied", r"Dirigent", r"Sänger", r"Musiklehrer", r"Chormeister", r"Kapellmeister", r"Vorstand"
    ]
    
    pattern = re.compile(rf"\b({'|'.join(honorifics)})\.?", re.IGNORECASE)

    # Titel extrahieren – diesmal sicher über finditer()
    titles_found = [m.group(0).strip() for m in pattern.finditer(name)]
    title = " ".join(titles_found).strip()

    # Titel entfernen aus dem Originalnamen
    cleaned = pattern.sub("", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned, title

def normalize_name(name: str) -> str:
    """
    Entfernt temporär Titel/Anreden zur besseren Vergleichbarkeit.
    Für vollständige Trennung nutze `normalize_name_with_title`.
    """

    
    if not name:
        return ""
    
    # Kleinbuchstaben, Whitespace
    normalized = name.lower().strip()

    # Titel & Anrede entfernen (auch generischere Fälle)
    honorifics = [
        "herr", "herrn", "frau", "fräulein", "witwe",
        "dr", "dr.","dr. ", "prof", "prof.",
        "obermusikmeister", "ehrenmitglied", "ehrenpräsident", "chorleiter", "dirigent"
    ]
    for honorific in honorifics:
        if normalized.startswith(honorific + " "):
            normalized = normalized[len(honorific):].strip()
    
    # Sonderzeichen und doppelte Leerzeichen raus
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    # Nickname-Mapping
    if normalized in EXPANDED_NICKNAME_MAP:
        return EXPANDED_NICKNAME_MAP[normalized]
    
    return normalized


def get_name_variations(name: str) -> List[str]:
    """
    Generate possible variations of a name to handle OCR errors
    and spelling variations.
    
    Args:
        name: The name to generate variations for
        
    Returns:
        A list of name variations
    """
    if not name:
        return []
    
    normalized = normalize_name(name)
    variations = [normalized]
    
    # Add nickname variations
    if normalized in EXPANDED_NICKNAME_MAP:
        canonical = EXPANDED_NICKNAME_MAP[normalized]
        variations.append(canonical)
        variations.extend(NICKNAME_MAP.get(canonical, []))
    
    # Add OCR error variations (limit to reasonable number)
    ocr_variations = [normalized]
    for char, replacements in OCR_ERRORS.items():
        if char in normalized:
            for replacement in replacements:
                for var in ocr_variations.copy():
                    ocr_variations.append(var.replace(char, replacement))
    
    #variations.extend(ocr_variations[:10])  # Limit to avoid explosion
    
    # Remove duplicates while preserving order
    unique_variations = []
    seen = set()
    for var in variations:
        if var not in seen:
            unique_variations.append(var)
            seen.add(var)
    
    return unique_variations

##### Test Fuzzy ####

def fuzzy_match_name(name: str, candidates: List[str], threshold: int = 80) -> Tuple[Optional[str], int]:
    """
    Verbesserte fuzzy_match_name-Funktion mit rapidfuzz.
    Vergleicht normalisierte Namen mit fuzz.ratio und wählt den besten Match aus.

    Args:
        name: Der zu vergleichende Name
        candidates: Liste von Kandidatennamen
        threshold: Minimale Ähnlichkeit, um als Match zu gelten

    Returns:
        Tuple aus (bester Match, Score) oder (None, 0), wenn kein Match gefunden
    """
    if not name or not candidates:
        return None, 0

    normalized_name = normalize_name(name)
    best_match = None
    best_score = 0

    for candidate in candidates:
        normalized_candidate = normalize_name(candidate)
        score = fuzz.ratio(normalized_name, normalized_candidate)
        

        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= threshold:
        return best_match, best_score
    return None, 0

def safe_str_lower_strip(val: Any) -> str:
    """
    Gibt einen sicher normalisierten String zurück (klein, getrimmt),
    auch wenn val None, float oder NaN ist.
    """
    if isinstance(val, str):
        return val.lower().strip()
    return ""


def match_person(
    person: Dict[str, str], 
    candidates: List[Dict[str, str]] = None
) -> Tuple[Optional[Dict[str, str]], int]:

    thresholds = get_matching_thresholds()

    if not person:
        return None, 0

    # Extrahiere und normalisiere
    forename = str(person.get("forename", "") or "").strip()
    familyname = str(person.get("familyname", "") or "").strip()
    alternate_name = str(person.get("alternate_name", "") or "").strip()

    normalized_forename = normalize_name(forename)
    normalized_familyname = normalize_name(familyname)
    normalized_alternate_name = normalize_name(alternate_name)

    if candidates is None:
        candidates = KNOWN_PERSONS

    if not candidates:
        return None, 0

    # Spezialfall: Gedrehter Vor-/Nachname
    for candidate in candidates:
        if (
            normalize_name(forename) == normalize_name(str(candidate.get("familyname", ""))) and
            normalize_name(familyname) == normalize_name(str(candidate.get("forename", "")))
        ):
            return candidate, 100

    # Initialisiere für Fallback
    best_match = None
    best_score = 0

    # (weiter mit fuzzy matching etc.)
 
    # Spezialfall: Nur ein Vor- oder Nachname vorhanden
    if forename and not familyname:
        for candidate in candidates:
            cand_forename = str(candidate.get("forename","") or "").strip()
            cand_alt = str(candidate.get("alternate_name","") or "").strip()
            cand_variants = [cand_forename]
            if cand_alt and cand_alt != cand_forename:
                cand_variants.append(cand_alt)
            _, score = fuzzy_match_name(forename, cand_variants, threshold=thresholds["forename"])
            if score > best_score:
                best_score = score
                best_match = candidate
        return best_match, best_score

    if familyname and not forename:
        for candidate in candidates:
            cand_familyname = str(candidate.get("familyname","") or "").strip()
            _, score = fuzzy_match_name(familyname, [cand_familyname], threshold=thresholds["familyname"])
            if score > best_score:
                best_score = score
                best_match = candidate
        return best_match, best_score
    
    for candidate in candidates:
        candidate_forename = str(candidate.get("forename","") or "").strip()
        candidate_alternate = str(candidate.get("alternate_name", "") or "").strip()

        candidate_familyname = str(candidate.get("familyname","") or "").strip()
                
        input_alt = str(person.get("alternate_name","") or "").strip().lower()
        raw_cand_alt = candidate.get("alternate_name", "")
        cand_alt = str(raw_cand_alt).strip().lower() if isinstance(raw_cand_alt, str) else ""
        if input_alt and cand_alt and input_alt != cand_alt:
            continue  # Unterschiedlich? → Kein Match erlaubt!
        # Wenn nur einer gesetzt ist → auch zu unsicher
        if (input_alt and not cand_alt) or (cand_alt and not input_alt):
            continue

        # Vorname-Varianten inkl. Alternativnamen
        candidate_forename_variants = [candidate_forename]
        if candidate_alternate and candidate_alternate != candidate_forename:
            candidate_forename_variants.append(candidate_alternate)

        # Skip leere Kandidaten
        if not candidate_forename and not candidate_familyname:
            continue

        # Family name matching
        family_score = 0
        if familyname and candidate_familyname:
            _, family_score = fuzzy_match_name(familyname, [candidate_familyname], threshold=thresholds["familyname"])


        # Forename matching
        forename_score = 0
        if forename and candidate_forename_variants:
            _, forename_score = fuzzy_match_name(forename, candidate_forename_variants,threshold=thresholds["forename"])

        # Kombiniere Scores: ausgewogeneres Verhältnis zwischen Vor- und Nachname
        combined_score = (family_score * 0.6) + (forename_score * 0.4)

        # ⚠️ Behandlung von alternate_name
        raw_alt = person.get("alternate_name", "")
        input_alt = str(raw_alt).strip().lower() if isinstance(raw_alt, str) else ""
        cand_alt = safe_str_lower_strip(candidate.get("alternate_name", ""))
        
        # Bewertung der Alternativnamen (gerichtet)
        input_alt = str(person.get("alternate_name","") or "").strip().lower()
        cand_alt = safe_str_lower_strip(candidate.get("alternate_name",""))

        alternate_name_score = 0

        if input_alt and cand_alt:
            if input_alt == cand_alt:
                alternate_name_score = 100  # Volle Übereinstimmung
            else:
                alternate_name_score = 0    # Unterschiedlich → keine Übereinstimmung
        elif input_alt or cand_alt:
            alternate_name_score = 50      # Nur einer hat einen alternate_name

        # Kombinierte Bewertung mit alternate_name integriert
        combined_score = (
            family_score * 0.5 + 
            forename_score * 0.4 + 
            alternate_name_score * 0.3     # 10 % Gewicht für alternate_name
        )


        # Bestes Match speichern
        if combined_score > best_score:
            best_score = combined_score
            best_match = candidate

    return best_match, best_score

def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: List[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    if not persons:
        return []

    if known_candidates is None:
        known_candidates = KNOWN_PERSONS

    normalized_persons = []
    normalized_names_seen = set()
    unique_persons = []  # ← das hat gefehlt

    for person in persons:
        forename = str(person.get("forename", "") or "").strip()
        familyname = str(person.get("familyname", "") or "").strip()
        altname = str(person.get("alternate_name", "") or "").strip()
        role = str(person.get("role", "") or "").strip()

        if not forename and not familyname:
            continue

        # ✨ Titel extrahieren
        cleaned_name, extracted_title = normalize_name_with_title(forename)
        extracted_forename = cleaned_name

        # Setze extracted_title als eigenes Feld „title“
        title = extracted_title


        # aktualisierte Normalisierung für Matching-Schlüssel
        norm_forename = normalize_name(extracted_forename)
        norm_familyname = normalize_name(familyname)
        norm_altname = normalize_name(altname)

        name_keys = {
            f"{norm_forename} {norm_familyname} {norm_altname}",
            f"{norm_familyname} {norm_forename} {norm_altname}"
        }

        if name_keys & normalized_names_seen:
            continue

        normalized_names_seen.update(name_keys)

        normalized_person = person.copy()
        normalized_person["forename"] = extracted_forename
        normalized_person["familyname"] = familyname
        normalized_person["alternate_name"] = altname
        normalized_person["title"] = title
        normalized_person["role"] = role  

        normalized_persons.append(normalized_person)

    # jetzt deduplizieren mit merge und fuzzy matching
    for person in normalized_persons:
        match_known, score_known = match_person(person, candidates=known_candidates)

        if match_known and score_known >= 90:
            unique_persons.append(match_known)
            continue

        # zuerst versuchen: genaues Match (inkl. gedreht)
        match_unique, score_unique = match_person(person, candidates=unique_persons)

        # auch gedrehte Namen direkt abfangen
        for existing in unique_persons:
            if (
                normalize_name(person["forename"]) == normalize_name(existing["familyname"]) and
                normalize_name(person["familyname"]) == normalize_name(existing["forename"])
            ):
                match_unique = existing
                score_unique = 100
                break

        if match_unique is None:
            unique_persons.append(person)
        else:
            match_idx = unique_persons.index(match_unique)
            merged = merge_person_records(person, match_unique)
            unique_persons[match_idx] = merged


    return unique_persons


def merge_person_records(person1: Dict[str, str], person2: Dict[str, str]) -> Dict[str, str]:
    """
    Merge two person records, keeping the most complete information.
    
    Args:
        person1: First person record
        person2: Second person record
        
    Returns:
        A merged person record
    """
    merged = {}
    
    # Fields to merge
    fields = ["forename", "familyname", "role", "associated_place", "associated_organisation", "alternate_name", "title"]
    
    for field in fields:
        value1 = str(person1.get(field,"") or "").strip()
        value2 = str(person2.get(field,"") or "").strip()
        
        # Choose the non-empty value, or the longer one if both are populated
        if value1 and not value2:
            merged[field] = value1
        elif value2 and not value1:
            merged[field] = value2
        elif len(value1) >= len(value2):
            merged[field] = value1
        else:
            merged[field] = value2
    
    # Add any other fields that might be present in either record
    all_keys = set(person1.keys()) | set(person2.keys())
    for key in all_keys:
        if key not in fields:
            value1 = person1.get(key, "")
            value2 = person2.get(key, "")
            
            if value1 and not value2:
                merged[key] = value1
            elif value2 and not value1:
                merged[key] = value2
            elif len(str(value1)) >= len(str(value2)):
                merged[key] = value1
            else:
                merged[key] = value2
    
    return merged
def compare_with_ground_truth(
    persons_to_check: List[Dict[str, str]],
    ground_truth: List[Dict[str, str]] = KNOWN_PERSONS
) -> pd.DataFrame:
    """
    Vergleicht eine Liste von Personen mit der Ground-Truth-Personenliste (z. B. aus CSV)
    und gibt einen DataFrame mit Match-Ergebnissen und Scores zurück.

    Args:
        persons_to_check: Liste von zu überprüfenden Personen
        ground_truth: Liste von bekannten Personen (Ground Truth)

    Returns:
        Pandas DataFrame mit Vergleichsergebnissen
    """
    results = []
    for person in persons_to_check:
        best_match, score = match_person(person, candidates=ground_truth)
        result = {
        "Input_Forename": person.get("forename", ""),
        "Input_Familyname": person.get("familyname", ""),
        "Input_AltName": person.get("alternate_name", ""),
        "Input_Title": person.get("title", ""),
        "Matched_Forename": best_match.get("forename", "") if best_match else None,
        "Matched_Familyname": best_match.get("familyname", "") if best_match else None,
        "Matched_AltName": best_match.get("alternate_name", "") if best_match else None,
        "Matched_Title": best_match.get("title", "") if best_match else None,
        "Match_ID": best_match.get("id", "") if best_match else None,
        "Score": score
    }


        results.append(result)
    
    df_results = pd.DataFrame(results)
    return df_results


    
def main():
    """Test the fuzzy matching with some examples."""
    test_data = [
         {"forename": "Alfons", "familyname": "Zimmermann", "role": "", "alternate_name": ""}, # Normal
         {"forename": "Fräulein Lina", "familyname": "Fingerdick", "role": "", "alternate_name": ""}, #Normal
         {"forename": "Otto", "familyname": "Bollinger", "role": "", "alternate_name": ""}, # Normal
         {"forename": "", "familyname": "Otto", "role": "", "alternate_name": ""}, #Einzelname
         {"forename": "Otte", "familyname": "Boilinger", "role": "", "alternate_name": ""},  # OCR error
         {"forename": "O.", "familyname": "Bollinger", "role": "", "alternate_name": ""}, # Initiale Abkürzung
         {"forename": "Otho", "familyname": "Bolinger", "role": "", "alternate_name": ""},  # Spelling variation
         {"forename": "Lina", "familyname": "Fingerdik", "role": "", "alternate_name": ""},  # Spelling variation
         {"forename": "Herrn Alfons", "familyname": "Zimmermann", "role": "", "alternate_name": ""}, # Anrede Test
         {"forename": "Zimmermann", "familyname": "Alfons", "role": "", "alternate_name": ""}, # Gedrehter Name Test
         {"forename": "Dr. Emil", "familyname": "Hosp", "role": "", "alternate_name": ""},  # Titel-Test
         {"forename": "Emil", "familyname": "Dr. Hosp", "role": "", "alternate_name": ""},  # Titel-Test gedreht
         {"forename": "", "familyname": "Dr. Münch", "role": "", "alternate_name": ""},  # Titel-Test
         {"forename": "Hermann", "familyname": "Binkert", "role": "", "alternate_name": "Junior"},  # Sohn
         {"forename": "Hermann", "familyname": "Binkert", "role": "", "alternate_name": "Senior"},  # Vater
    ]
    
    deduplicated = deduplicate_persons(test_data, known_candidates=KNOWN_PERSONS)
    df_comparison = compare_with_ground_truth(deduplicated)

    print(f"Original: {len(test_data)} records")
    print(f"Deduplicated: {len(deduplicated)} records")
    print("\nDeduplicated persons:")

    recognized_persons = []  # ← Liste hier initialisieren

    for person in deduplicated:
        fn = person.get("forename", "")
        ln = person.get("familyname", "")
        alt = person.get("alternate_name", "")
        title = person.get("title", "")
        role = person.get("role", "")
        parts = [f"{fn} {ln}"]
        if title:
            parts.append(f"[Titel: {title}]")
        if alt:
            parts.append(f"(aka: {alt})")
        if role and role != title:
            parts.append(f"[Rolle: {role}]")
        person_str = " ".join(parts)
        print(f"  {person_str}")
        recognized_persons.append(person_str)

    print("\nAlle erkannten Personen als Liste:")
    for i, person_str in enumerate(recognized_persons, 1):
        print(f"{i}. {person_str}")

    test_matches = [
        ({"forename": "Otto", "familyname": ""}, {"forename": "", "familyname": "Otto"}),
        ({"forename": "Hans", "familyname": "Schmidt"}, {"forename": "Johann", "familyname": "Schmidt"}),
        ({"forename": "Wilhelm", "familyname": "Müller"}, {"forename": "Willi", "familyname": "Mueller"}),
        ({"forename": "Zimmermann", "familyname": "Alfons"}, {"forename": "Alfons", "familyname": "Zimmermann"}),
    ]

    print("\nTest matches:")
    for person1, person2 in test_matches:
        match, score = match_person(person1, [person2])
        print(f"  {person1['forename']} {person1['familyname']} ↔ {person2['forename']} {person2['familyname']}: {score}%")

    df_comparison = compare_with_ground_truth(deduplicated)
    print("\nVergleich mit Ground Truth:")
    print(df_comparison)
    df_comparison.to_csv("person_match_results.csv", index=False)

if __name__ == "__main__":
    main()