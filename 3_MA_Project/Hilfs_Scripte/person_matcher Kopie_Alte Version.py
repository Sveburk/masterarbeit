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
        "forename": 80,
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

def normalize_name(name: str) -> str:
    """
    Normalize a name by converting to lowercase, removing special characters,
    and standardizing spelling variations (z. B. OCR oder Ehrentitel).
    
    Args:
        name: The name to normalize
        
    Returns:
        The normalized name
    """
    
    if not name:
        return ""
    
    # Kleinbuchstaben, Whitespace
    normalized = name.lower().strip()

    # Titel & Anrede entfernen (auch generischere Fälle)
    honorifics = [
        "herr", "herrn", "frau", "fräulein", "witwe",
        "dr", "dr.", "prof", "prof.",
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

    forename = str(person.get("forename", "") or "").strip()
    familyname = str(person.get("familyname", "") or "").strip()
    alternate_name = str(person.get("alternate_name", "") or "").strip()

    if candidates is None:
        candidates = KNOWN_PERSONS

    if not candidates:
        return None, 0

    # Kandidatenvariationen vorbereiten
    person_forename_variants = get_name_variations(forename)
    person_familyname_variants = get_name_variations(familyname)

    best_match = None
    best_score = 0

    for candidate in candidates:
        cand_forename = str(candidate.get("forename", "") or "").strip()
        cand_familyname = str(candidate.get("familyname", "") or "").strip()
        cand_alt = str(candidate.get("alternate_name", "") or "").strip()

        # Kandidatenvarianten generieren
        cand_forename_variants = get_name_variations(cand_forename)
        cand_familyname_variants = get_name_variations(cand_familyname)

        # Vorname matchen
        max_forename_score = max(
            fuzz.ratio(normalize_name(pf), normalize_name(cf))
            for pf in person_forename_variants
            for cf in cand_forename_variants
        ) if forename else 0

        # Nachname matchen
        max_familyname_score = max(
            fuzz.ratio(normalize_name(pf), normalize_name(cf))
            for pf in person_familyname_variants
            for cf in cand_familyname_variants
        ) if familyname else 0

        # Alternate Name
        alt_score = 0
        input_alt = normalize_name(alternate_name)
        cand_alt_norm = normalize_name(cand_alt)

        if input_alt and cand_alt_norm:
            alt_score = 100 if input_alt == cand_alt_norm else 0
        elif input_alt or cand_alt_norm:
            alt_score = 50  # einer gesetzt, einer nicht

        combined_score = (
            max_familyname_score * 0.5 +
            max_forename_score * 0.4 +
            alt_score * 0.3
        )

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

    unique_persons = []

    for person in persons:
        forename = str(person.get("forename", "") or "").strip()
        familyname = str(person.get("familyname", "") or "").strip()
        altname = str(person.get("alternate_name", "") or "").strip()

        if not forename and not familyname:
            continue

        # 1. Versuch: gegen CSV (Ground Truth)
        match_known, score_known = match_person(person, candidates=known_candidates)
        if match_known and score_known >= 90:
            unique_persons.append(match_known)
            continue

        # 2. Versuch: gegen interne Liste (eigene Duplikate)
        match_internal, score_internal = match_person(person, candidates=unique_persons)
        if match_internal and score_internal >= 85:  # leicht toleranter
            idx = unique_persons.index(match_internal)
            merged = merge_person_records(match_internal, person)
            unique_persons[idx] = merged
        else:
            unique_persons.append(person)

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
    fields = ["forename", "familyname", "role", "associated_place", "associated_organisation", "alternate_name"]
    
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
            "Matched_Forename": best_match.get("forename", "") if best_match else None,
            "Matched_Familyname": best_match.get("familyname", "") if best_match else None,
            "Matched_AltName": best_match.get("alternate_name", "") if best_match else None,
            "Match_ID": best_match.get("id", "") if best_match else None,
            "Score": score
        }
        results.append(result)
    
    df_results = pd.DataFrame(results)
    return df_results


    
def main():
    """Test the fuzzy matching with some examples."""
    test_data = [
         {"forename": "Otto", "familyname": "Bollinger", "role": "", "alternate_name": ""},
         {"forename": "", "familyname": "Otto", "role": "", "alternate_name": ""},
         {"forename": "Otte", "familyname": "Boilinger", "role": "", "alternate_name": ""},  # OCR error
         {"forename": "O.", "familyname": "Bollinger", "role": "", "alternate_name": ""},
         {"forename": "Otho", "familyname": "Bolinger", "role": "", "alternate_name": ""},  # Spelling variation
         {"forename": "Lina", "familyname": "Fingerdick", "role": "", "alternate_name": ""},
         {"forename": "Lina", "familyname": "Fingerdik", "role": "", "alternate_name": ""},  # Spelling variation
         {"forename": "Alfons", "familyname": "Zimmermann", "role": "", "alternate_name": ""},
         {"forename": "Herrn Alfons", "familyname": "Zimmermann", "role": "", "alternate_name": ""},
         {"forename": "Zimmermann", "familyname": "Alfons", "role": "", "alternate_name": ""},
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
        person_str = f"{fn} {ln} (aka: {alt})"
        print(f"  {person_str}")
        recognized_persons.append(person_str)

    # Zusätzliche Ausgabe aller erkannten Personen als Liste
    print("\nAlle erkannten Personen als Liste:")
    for i, person_str in enumerate(recognized_persons, 1):
        print(f"{i}. {person_str}")


    # Test individual matching
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
    print(df_comparison)
    df_comparison.to_csv("person_match_results.csv", index=False)


if __name__ == "__main__":
    main()