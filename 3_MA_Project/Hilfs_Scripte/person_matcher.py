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
    "m": ["rn", "nn", "in", "ni"],
    "w": ["vv", "v"],
    "n": ["ii", "il", "li"],
    "h": ["b", "lh", "hl"],
    "b": ["h", "lb", "bl"],
    "c": ["e", "o"],
    "e": ["c", "o"],
    "i": ["l", "1", "j"],
    "l": ["i", "1", "t"],
    "rn": ["m"],
    "cl": ["d"],
    "d": ["cl"],
}

def normalize_name(name: str) -> str:
    """
    Normalize a name by converting to lowercase, removing special characters,
    and standardizing spelling variations.
    
    Args:
        name: The name to normalize
        
    Returns:
        The normalized name
    """
    if not name:
        return ""
    
    # Convert to lowercase and remove extra whitespace
    normalized = name.lower().strip()
    
    # Remove honorifics and titles
    honorifics = ["herr", "frau", "herrn", "dr.", "prof.", "dr ", "prof "]
    for honorific in honorifics:
        if normalized.startswith(honorific):
            normalized = normalized[len(honorific):].strip()
    
    # Remove special characters and extra spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Map to canonical name if it's a known nickname
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
    
    variations.extend(ocr_variations[:10])  # Limit to avoid explosion
    
    # Remove duplicates while preserving order
    unique_variations = []
    seen = set()
    for var in variations:
        if var not in seen:
            unique_variations.append(var)
            seen.add(var)
    
    return unique_variations

def fuzzy_match_name(name: str, candidates: List[str], threshold: int = 70) -> Tuple[Optional[str], int]:
    """
    Find the best fuzzy match for a name among a list of candidates.
    
    Args:
        name: The name to match
        candidates: A list of candidate names to match against
        threshold: The minimum similarity score (0-100) to consider a match
        
    Returns:
        A tuple of (best_match, score) or (None, 0) if no match above threshold
    """
    if not name or not candidates:
        return None, 0
    
    normalized_name = normalize_name(name)
    if not normalized_name:
        return None, 0
    
    # Generate variations of the name
    name_variations = get_name_variations(normalized_name)
    
    # Find best match across all variations
    best_match = None
    best_score = 0
    
    for candidate in candidates:
        normalized_candidate = normalize_name(candidate)
        if not normalized_candidate:
            continue
        
        # Try different fuzzy matching algorithms
        for var in name_variations:
            ratio = fuzz.ratio(var, normalized_candidate)
            token_sort_ratio = fuzz.token_sort_ratio(var, normalized_candidate)
            token_set_ratio = fuzz.token_set_ratio(var, normalized_candidate)
            
            # Take the best score
            score = max(ratio, token_sort_ratio, token_set_ratio)
            
            if score > best_score:
                best_score = score
                best_match = candidate
    
    if best_score >= threshold:
        return best_match, best_score
    return None, 0

def match_person(
    person: Dict[str, str], 
    candidates: List[Dict[str, str]] = None, 
    threshold: int = 70
) -> Tuple[Optional[Dict[str, str]], int]:
    """
    Match a person against a list of candidate persons using fuzzy matching
    on both forename and familyname. If no candidates are provided, uses KNOWN_PERSONS.
    
    Args:
        person: A dictionary with 'forename' and 'familyname' keys
        candidates: A list of dictionaries with 'forename' and 'familyname' keys (default: KNOWN_PERSONS from CSV)
        threshold: The minimum similarity score to consider a match
        
    Returns:
        A tuple of (best_match, score) or (None, 0) if no match above threshold
    """
    if not person:
        return None, 0
        
    # Wenn keine Kandidaten angegeben sind, nutze die bekannten Personen aus der CSV
    if candidates is None:
        candidates = KNOWN_PERSONS
        
    if not candidates:
        return None, 0
    
    forename = person.get("forename", "").strip()
    familyname = person.get("familyname", "").strip()
    
    # Handle empty names
    if not forename and not familyname:
        return None, 0
    
    best_match = None
    best_score = 0
    
    for candidate in candidates:
        candidate_forename = candidate.get("forename", "").strip()
        candidate_familyname = candidate.get("familyname", "").strip()
        
        # Skip empty candidates
        if not candidate_forename and not candidate_familyname:
            continue
        
        # Family name matching
        family_score = 0
        if familyname and candidate_familyname:
            _, family_score = fuzzy_match_name(
                familyname, [candidate_familyname], threshold=50
            )
        
        # Forename matching
        forename_score = 0
        if forename and candidate_forename:
            _, forename_score = fuzzy_match_name(
                forename, [candidate_forename], threshold=50
            )
        
        # Calculate combined score based on available name parts
        combined_score = 0
        if familyname and candidate_familyname:
            if forename and candidate_forename:
                # Both parts available - weight family name more than first name
                combined_score = (family_score * 0.7) + (forename_score * 0.3)
            else:
                # Only family names - use family score directly
                combined_score = family_score
        elif forename and candidate_forename:
            # Only forenames - use first name score with penalty
            combined_score = forename_score * 0.8  # Penalty for missing family name
        
        # Check for reverse order (first/last name swapped)
        reverse_family_score = 0
        reverse_forename_score = 0
        
        if familyname and candidate_forename:
            _, reverse_family_score = fuzzy_match_name(
                familyname, [candidate_forename], threshold=50
            )
            
        if forename and candidate_familyname:
            _, reverse_forename_score = fuzzy_match_name(
                forename, [candidate_familyname], threshold=50
            )
        
        # Calculate reverse score
        reverse_score = 0
        if reverse_family_score > 0 and reverse_forename_score > 0:
            reverse_score = (reverse_family_score * 0.6) + (reverse_forename_score * 0.4)
            # Apply penalty for name reversal
            reverse_score *= 0.9
        
        # Take best of normal or reverse matching
        score = max(combined_score, reverse_score)
        
        if score > best_score:
            best_score = score
            best_match = candidate
    
    if best_score >= threshold:
        return best_match, best_score
    return None, 0

def deduplicate_persons(persons: List[Dict[str, str]], threshold: int = 70) -> List[Dict[str, str]]:
    """
    Deduplicate a list of persons using fuzzy matching.
    
    Args:
        persons: A list of person dictionaries with 'forename' and 'familyname' keys
        threshold: The minimum similarity score to consider a match
        
    Returns:
        A deduplicated list of persons
    """
    if not persons:
        return []
    
    # Normalize and filter
    normalized_persons = []
    for person in persons:
        forename = person.get("forename", "").strip()
        familyname = person.get("familyname", "").strip()
        
        # Skip empty persons
        if not forename and not familyname:
            continue
        
        # Create a copy with normalized names
        normalized_person = person.copy()
        normalized_person["forename"] = forename
        normalized_person["familyname"] = familyname
        normalized_persons.append(normalized_person)
    
    # Deduplicate
    unique_persons = []
    
    for person in normalized_persons:
        # Try to match with existing unique persons
        match, score = match_person(person, unique_persons, threshold)
        
        if match is None:
            # No match found, add as new unique person
            unique_persons.append(person)
        else:
            # Match found, but keep the more complete record
            match_idx = unique_persons.index(match)
            merged_person = merge_person_records(person, match)
            unique_persons[match_idx] = merged_person
    
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
    fields = ["forename", "familyname", "role", "associated_place", "associated_organisation"]
    
    for field in fields:
        value1 = person1.get(field, "").strip()
        value2 = person2.get(field, "").strip()
        
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

def main():
    """Test the fuzzy matching with some examples."""
    test_data = [
        {"forename": "Otto", "familyname": "Bollinger", "role": ""},
        {"forename": "", "familyname": "Otto", "role": ""},
        {"forename": "Otte", "familyname": "Boilinger", "role": ""},  # OCR error
        {"forename": "O.", "familyname": "Bollinger", "role": ""},
        {"forename": "Otho", "familyname": "Bolinger", "role": ""},  # Spelling variation
        {"forename": "Lina", "familyname": "Fingerdick", "role": ""},
        {"forename": "Lina", "familyname": "Fingerdik", "role": ""},  # Spelling variation
        {"forename": "Alfons", "familyname": "Zimmermann", "role": ""},
        {"forename": "Herrn Alfons", "familyname": "Zimmermann", "role": ""},  # With honorific
        {"forename": "Zimmermann", "familyname": "Alfons", "role": ""},  # Reversed
    ]
    
    deduplicated = deduplicate_persons(test_data)
    
    print(f"Original: {len(test_data)} records")
    print(f"Deduplicated: {len(deduplicated)} records")
    print("\nDeduplicated persons:")
    for person in deduplicated:
        print(f"  {person['forename']} {person['familyname']}")
    
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

if __name__ == "__main__":
    main()