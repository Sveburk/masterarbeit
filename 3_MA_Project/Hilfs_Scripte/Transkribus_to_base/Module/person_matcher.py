"""
Module: Module.person_matcher

Person Matcher - Fuzzy name matching adapted for specific CSV structure

This module provides functions for fuzzy matching of person names from a custom CSV
including specific metadata fields and using robust fuzzy matching techniques.
"""

from typing import List, Dict, Tuple, Optional, Any
import pandas as pd
from rapidfuzz import fuzz
import re
import os

# ------------------------------------------------------------------------------
# Konfiguration & Konstanten
# ------------------------------------------------------------------------------
default_read_opts = dict(sep=";", dtype=str, keep_default_na=False)
CSV_PATH_KNOWN_PERSONS = os.path.expanduser(
    "~/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-person.csv"
)
UNMATCHABLE_SINGLE_NAMES = {"otto", "döbele", "doebele"}

# ------------------------------------------------------------------------------
# Schwellenwerte
# ------------------------------------------------------------------------------
def get_matching_thresholds() -> Dict[str, int]:
    return {"forename": 80, "familyname": 85}

# ------------------------------------------------------------------------------
# Laden bekannter Personen
# ------------------------------------------------------------------------------
def load_known_persons_from_csv(path: str = CSV_PATH_KNOWN_PERSONS) -> List[Dict[str, str]]:
    def safe_strip(val: Any) -> str:
        return str(val).strip() if isinstance(val, str) else str(val) if pd.notna(val) else ""

    try:
        df = pd.read_csv(path, **default_read_opts)
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns={
            "Vorname": "forename",
            "Nachname": "familyname",
            "Alternativer Vorname": "alternate_name",
            "nodegoat ID": "nodegoat_id",
            "[Wohnort] Location Reference - Object ID": "associated_place",
            "[Geburt] Date Start": "birth_date",
            "[Tod] Date Start": "death_date",
            "[Mitgliedschaft] in Organisation": "organisation"
        }, inplace=True)
        string_columns = [
            "forename", "familyname", "alternate_name", "nodegoat_id",
            "associated_place", "birth_date", "death_date", "organisation"
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna("").str.strip()
        persons: List[Dict[str, str]] = []
        for _, row in df.iterrows():
            persons.append({
                "forename": safe_strip(row.get("forename")),
                "familyname": safe_strip(row.get("familyname")),
                "alternate_name": safe_strip(row.get("alternate_name")),
                "id": safe_strip(row.get("nodegoat_id")),
                "home": safe_strip(row.get("associated_place")),
                "birth_date": safe_strip(row.get("birth_date")),
                "death_date": safe_strip(row.get("death_date")),
                "organisation": safe_strip(row.get("organisation"))
            })
        print(f"[DEBUG] Geladene Personen: {len(persons)}")
        return persons
    except Exception as e:
        print(f"[ERROR] Fehler beim Laden der Personendaten aus {path}: {e}")
        return []


KNOWN_PERSONS: List[Dict[str, str]] = load_known_persons_from_csv()
# Nickname-Map wie ursprünglich definiert (kann angepasst werden)
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
EXPANDED_NICKNAME_MAP: Dict[str, str] = {nick: canon for canon, nicks in NICKNAME_MAP.items() for nick in nicks}
OCR_ERRORS: Dict[str, List[str]] = {
    "ü": ["u", "ue"], "ä": ["a", "ae"], "ö": ["o", "oe"], "ß": ["ss"]
}
KNOWN_PERSONS: List[Dict[str, str]] = load_known_persons_from_csv()

# ------------------------------------------------------------------------------
# Normalisierung
# ------------------------------------------------------------------------------

def normalize_name_string(name: str) -> str:
    if not name:
        return ""
    s = name.lower().strip()
    s = re.sub(r"\b(dr|prof|herr|herrn|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s*", "", s)
    s = re.sub(r"[^\w\s]", "", s)
    return EXPANDED_NICKNAME_MAP.get(s, s)


def is_initial(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]\.?", s.strip()))


def normalize_name(name: str) -> Dict[str, str]:
    if not name:
        return {"title": "", "forename": "", "familyname": ""}
    s = name.strip().strip('"')
    m = re.match(r"(?i)\b(dr|prof|herr|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s+", s)
    title = m.group(1).capitalize() if m else ""
    s = re.sub(r"(?i)\b(dr|prof|herr|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s+", "", s)
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = EXPANDED_NICKNAME_MAP.get(s, s)
    parts = s.split()
    if len(parts) == 1:
        return {"title": title, "forename": parts[0].capitalize(), "familyname": ""}
    return {"title": title, "forename": parts[0].capitalize(), "familyname": " ".join(parts[1:]).capitalize()}

# ------------------------------------------------------------------------------
# Match-Funktionen
# ------------------------------------------------------------------------------

def fuzzy_match_name(name: str, candidates: List[str], threshold: int) -> Tuple[Optional[str], int]:
    best_match, best_score = None, 0
    norm = normalize_name_string(name)
    for c in candidates:
        score = fuzz.ratio(normalize_name_string(c), norm)
        if score > best_score:
            best_match, best_score = c, score
    return (best_match, best_score) if best_score >= threshold else (None, 0)


def match_person(
    person: Dict[str, str],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS
) -> Tuple[Optional[Dict[str, str]], int]:
    fn_in = person.get("forename", "").strip()
    ln_in = person.get("familyname", "").strip()
    thresholds = get_matching_thresholds()
    best_match, best_score = None, 0

    for cand in candidates:
        fn_can = cand.get("forename", "").strip()
        ln_can = cand.get("familyname", "").strip()

        if is_initial(fn_in) and fn_can and fn_in[0].upper() == fn_can[0].upper():
            fn_score = 100
        else:
            _, fn_score = fuzzy_match_name(fn_in, [fn_can, cand.get("alternate_name","")], thresholds["forename"])

        if is_initial(ln_in) and ln_can and ln_in[0].upper() == ln_can[0].upper():
            ln_score = 100
        else:
            _, ln_score = fuzzy_match_name(ln_in, [ln_can], thresholds["familyname"])

        combined = ln_score * 0.5 + fn_score * 0.4 / 0.9
        if combined > best_score:
            best_score, best_match = combined, cand

        if normalize_name_string(fn_in) == normalize_name_string(ln_can) \
        and normalize_name_string(ln_in) == normalize_name_string(fn_can):
            return cand, 100

    if best_score >= max(thresholds.values()):
        return best_match, int(best_score)

    if fn_in and not ln_in and fn_score >= thresholds["forename"]:
        return best_match, int(fn_score)
    if ln_in and not fn_in and ln_score >= thresholds["familyname"]:
        return best_match, int(ln_score)

    return None, 0

# ------------------------------------------------------------------------------
# Deduplication
# ------------------------------------------------------------------------------
def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: Optional[List[Dict[str, str]]] = None
) -> List[Dict[str, str]]:
    seen: set = set()
    unique: List[Dict[str, str]] = []
    candidates = known_candidates or KNOWN_PERSONS

    for p in persons:
        key = f"{normalize_name_string(p.get('forename',''))} {normalize_name_string(p.get('familyname',''))}"
        if key in seen:
            continue
        seen.add(key)

        match, score = match_person(p, candidates)
        if match and score >= 90:
            enriched = {
                **match,
                "match_score": score,
                "confidence": "fuzzy"
            }
            unique.append(enriched)
        else:
            p["match_score"] = score
            p["confidence"]  = "none"
            unique.append(p)

    return unique

# ------------------------------------------------------------------------------
# Detail-Info
# ------------------------------------------------------------------------------
def get_best_match_info(
    person: Dict[str, str],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS
) -> Dict[str, Any]:
    match, score = match_person(person, candidates)
    return {
        "matched_forename": match.get("forename") if match else None,
        "matched_familyname": match.get("familyname") if match else None,
        "matched_title": match.get("title") if match and match.get("title") else None,
        "match_id": match.get("id") if match else None,
        "score": score
    }

# ------------------------------------------------------------------------------
# Extract Person Data
# ------------------------------------------------------------------------------
def extract_person_data(row: Dict[str, Any]) -> Dict[str, str]:
    if row.get('forename') and row.get('familyname'):
        return {
            'forename': row.get('forename', '').strip(),
            'familyname': row.get('familyname', '').strip(),
            'alternate_name': row.get('alternate_name', '').strip(),
            'title': row.get('title', '').strip(),
            'nodegoat_id': row.get('nodegoat_id', '').strip(),
            'home': row.get('home', '').strip(),
            'birth_date': row.get('birth_date', '').strip(),
            'death_date': row.get('death_date', '').strip(),
            'organisation': row.get('organisation', '').strip(),
        }
    name_field = (row.get('name') or row.get('Name') or '').strip()
    m = re.match(r'^(?P<anrede>Herrn?|Frau|Fräulein|Dr\.?|Prof\.?|Witwe)\s+(?P<rest>.+)$', name_field, flags=re.IGNORECASE)
    if m:
        name_field = m.group('rest').strip()
    parts = normalize_name(name_field)
    return {
        'forename': parts.get('forename', ''),
        'familyname': parts.get('familyname', ''),
        'alternate_name': row.get('alternate_name', '').strip(),
        'title': parts.get('title', ''),
        'nodegoat_id': row.get('id', '').strip(),
        'home': row.get('home', '').strip(),
        'birth_date': row.get('birth_date', '').strip(),
        'death_date': row.get('death_date', '').strip(),
        'organisation': row.get('organisation', '').strip(),
    }

# ------------------------------------------------------------------------------
# Split und Enrich
# ------------------------------------------------------------------------------
def split_and_enrich_persons(
    raw_persons: List[Dict[str, str]],
    document_id: Optional[str] = None,
    candidates: Optional[List[Dict[str, str]]] = None
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    seen = set()
    matched, unmatched = [], []
    cand_list = candidates or KNOWN_PERSONS

    for p in raw_persons:
        nid = p.get("nodegoat_id", "").strip()
        key = nid or f"{normalize_name_string(p['forename'])} {normalize_name_string(p['familyname'])}"
        if not key or key in seen:
            continue
        seen.add(key)

        match, score = match_person(p, candidates=cand_list)
        print(f"[DEBUG] Person-Match: {p.get('forename')} {p.get('familyname')} -> Score: {score}")

        if match and score >= 90:
            enriched = {
                **p,
                "forename": match.get("forename", p["forename"]),
                "familyname": match.get("familyname", p["familyname"]),
                "nodegoat_id": match.get("nodegoat_id"),
                "alternate_name": match.get("alternate_name", ""),
                "title": match.get("title", p.get("title", "")),
                "match_score": score,
                "confidence": "fuzzy"
            }
            print(f"[DEBUG] Matched person enriched with score {score}: {enriched.get('forename')} {enriched.get('familyname')}")
            matched.append(enriched)
        else:
            entry = p.copy()
            if document_id:
                entry["document_id"] = document_id
            entry["match_score"] = score
            entry["confidence"] = "none"
            print(f"[DEBUG] Unmatched person with score {score}: {entry.get('forename')} {entry.get('familyname')}")
            unmatched.append(entry)

    return matched, unmatched
