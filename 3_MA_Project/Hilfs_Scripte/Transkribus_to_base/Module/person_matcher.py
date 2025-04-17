
"""
Person Matcher - Fuzzy name matching adapted for specific CSV structure

This module provides functions for fuzzy matching of person names from a custom CSV
including specific metadata fields and using robust fuzzy matching techniques.
"""

from typing import List, Dict, Tuple, Optional, Any
import pandas as pd
df: pd.DataFrame
from rapidfuzz import fuzz
import re
import os


HONORIFICS = {"herr", "herrn", "frau", "fräulein", "witwe", "pg", "pg.", "Parteigenosse"}



# Einheitliche Leseoptionen für alle CSV-Inputs
default_read_opts = dict(sep=";", dtype=str, keep_default_na=False)


# Pfad zur neuen CSV-Datei
CSV_PATH_KNOWN_PERSONS = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-person.csv"

def get_matching_thresholds() -> Dict[str, int]:
    return {"forename": 80, "familyname": 85}

# Lade bekannte Personen aus der neuen CSV


def load_known_persons_from_csv(csv_path: str = CSV_PATH_KNOWN_PERSONS) -> List[Dict[str, str]]:
    """
    Lädt die Personendaten aus der CSV und liefert eine Liste von Dictionaries.
    Leer-/NaN-Werte werden in leere Strings umgewandelt, und wir strippen alle String-Felder.
    """
    if not os.path.exists(csv_path):
        print(f"Warnung: CSV-Datei nicht gefunden: {csv_path}")
        return []

    try:
        raw_df = pd.read_csv(csv_path, **default_read_opts)
    except Exception as e:
        print(f"Fehler beim Laden der CSV-Datei: {e}")
        return []

    df: pd.DataFrame = raw_df.fillna("")  


    df = df.fillna("")  # NaNs sicher entfernen

    persons: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        row_dict = {k: v.strip() if isinstance(v, str) else v for k, v in row.to_dict().items()}

        forename   = row_dict.get("Vorname", "")
        familyname = row_dict.get("Nachname", "")
        if not (forename or familyname):
            continue

        person = {
            "forename":        forename,
            "familyname":      familyname,
            "alternate_name":  row.get("Alternativer Vorname", ""),
            "id":              row.get("nodegoat ID", ""),
            "home":            row.get("[Wohnort] Location Reference - Object ID", ""),
            "birth_date":      row.get("[Geburt] Date Start", ""),
            "death_date":      row.get("[Tod] Date Start", ""),
            "organisation":    row.get("[Mitgliedschaft] in Organisation", "")
        }
        persons.append(person)

    return persons

# Initialisiere bekannte Personen
KNOWN_PERSONS = load_known_persons_from_csv()

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
EXPANDED_NICKNAME_MAP = {nickname: canonical for canonical, nicknames in NICKNAME_MAP.items() for nickname in nicknames}

# OCR Fehler Korrekturen wie definiert
OCR_ERRORS = {"ü": ["u", "ii", "il", "li"], 
              "ä": ["a", "ii", "il", "li"], 
              "ö": ["o", "ii", "il", "li"], 
              "ß": ["ss", "sz", "s", "b"]}

def normalize_name_string(name: str) -> str:
    """
    Gibt eine bereinigte Namenszeichenkette zurück, bei der Titel entfernt, 
    Sonderzeichen gestripped und Nickname-Mapping angewendet wurde.
    Wird z. B. für Fuzzy-Matching verwendet.
    """
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r"\b(dr|prof|herr|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s*", "", name)
    name = re.sub(r'[^\w\s]', '', name)
    return EXPANDED_NICKNAME_MAP.get(name, name)


def normalize_name(name: str) -> dict:
    """
    Entfernt Titel (z. B. 'Dr.', 'Herr', 'Prof.') aus einem Namen,
    bereinigt Sonderzeichen und gibt ein Dictionary mit Titel, Vorname und Nachname zurück.
    """
    if not name:
        return {"title": "", "forename": "", "familyname": ""}

    name = name.strip().strip('"')
    title_match = re.match(r"(?i)\b(dr|prof|herr|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s+", name)
    title = title_match.group(1).capitalize() if title_match else ""

    # Titel entfernen für weitere Analyse
    name = re.sub(r"(?i)\b(dr|prof|herr|frau|fräulein|witwe|dirigent|ehrenmitglied)\.?\s+", "", name)
    name = name.lower().strip()
    name = re.sub(r'[^\w\s]', '', name)

    # Nickname-Mapping
    name = EXPANDED_NICKNAME_MAP.get(name, name)

    parts = name.split()
    if len(parts) == 1:
        return {"title": title, "forename": parts[0].capitalize(), "familyname": ""}
    elif len(parts) >= 2:
        return {
            "title": title,
            "forename": parts[0].capitalize(),
            "familyname": " ".join(parts[1:]).capitalize()
        }

    return {"title": title, "forename": "", "familyname": ""}


def fuzzy_match_name(name: str, candidates: List[str], threshold: int) -> Tuple[Optional[str], int]:
    best_match, best_score = None, 0
    normalized_name = normalize_name_string(name)

    for candidate in candidates:
        score = fuzz.ratio(normalize_name_string(candidate), normalized_name)
        if score > best_score:
            best_score, best_match = score, candidate

    return (best_match, best_score) if best_score >= threshold else (None, 0)

def match_person(
    person: Dict[str, str],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS
) -> Tuple[Optional[Dict[str, str]], int]:
    """
    Liefert (beste_Person, Score) oder (None, 0)

    – blockiert vorab „unmatchable“ Einzel‑Namen (Otto / Döbele / Doebele)
    – erkennt gedrehte Vor‑/Nachnamen
    – verhindert das Spezial‑Miss‑Match Otto ↔ Ott
    – überspringt leere Kandidaten
    """
    thresholds = get_matching_thresholds()
    best_match, best_score = None, 0

    # ------------------------------------------------------------------
    # 1) Vorab‑Prüfung auf verbotene Einzel‑Namen
    # ------------------------------------------------------------------
    forename   = (person.get("forename", "")   or "").strip()
    familyname = (person.get("familyname", "") or "").strip()

    norm_fn = normalize_name_string(forename)
    norm_ln = normalize_name_string(familyname)


    UNMATCHABLE_SINGLE_NAMES = {"otto", "döbele", "doebele"}
    if forename.lower() in UNMATCHABLE_SINGLE_NAMES and not familyname:
        print(f"[DEBUG] ❌ Einzelname {forename} geblockt")
        return None, 0
    if familyname.lower() in UNMATCHABLE_SINGLE_NAMES and not forename:
        print(f"[DEBUG] ❌ Einzelname {familyname} geblockt")
        return None, 0

    # ------------------------------------------------------------------
    # 2) Reguläres Matching
    # ------------------------------------------------------------------
    for candidate in candidates:
        cand_fn  = (candidate.get("forename", "")        or "").strip()
        cand_alt = (candidate.get("alternate_name", "")  or "").strip()
        cand_ln  = (candidate.get("familyname", "")      or "").strip()

        # ⚠️ leere Kandidaten sofort überspringen
        if not cand_fn and not cand_ln:
            continue

        cand_fn_norm = normalize_name_string(cand_fn)
        cand_ln_norm = normalize_name_string(cand_ln)

        # ✨ Spezialregel: Otto ↔ Ott verhindern
        if {"otto", "ott"} == {norm_fn, cand_ln_norm} or {"otto", "ott"} == {norm_ln, cand_fn_norm}:
            continue

        # ✅ Gedrehter Vor‑/Nachname (Maximal‑Treffer)
        if norm_fn == cand_ln_norm and norm_ln == cand_fn_norm:
            return candidate, 100

        # ---------- reguläre Score‑Berechnung ----------
        _, fn_score = fuzzy_match_name(forename, [cand_fn, cand_alt], thresholds["forename"])
        _, ln_score = fuzzy_match_name(familyname, [cand_ln],          thresholds["familyname"])
        combined = fn_score * 0.4 + ln_score * 0.6

    # ------------------------------------------------------------------
    # ✂️  Honorifics & Rollen vor dem Matching entfernen
    # ------------------------------------------------------------------
    HONORIFICS    = {"herr", "herrn", "frau", "fräulein", "witwe"}
    ROLE_KEYWORDS = {"dirigent", "vereinsführer", "chorleiter", "ehrenmitglied"}

    # a) Anrede steht im Forename‑Feld?  →  als Titel merken, Forename leeren
    if forename.lower().rstrip(".") in HONORIFICS:
        person["title"]   = forename.capitalize().rstrip(".")
        forename          = ""
        person["forename"] = ""
        
    if not forename and " " in familyname:
        tokens = familyname.split()
        # mind. 2 Tokens → 1. Token = Vorname, Rest = Nachname
        if len(tokens) >= 2:
            forename, familyname = tokens[0], " ".join(tokens[1:])
            person["forename"]   = forename
            person["familyname"] = familyname

        norm_fn = normalize_name_string(forename)
        norm_ln = normalize_name_string(familyname)



    # b) Rollen­keyword als erstes Token im Familyname?  →  als Rolle ablegen
    first_token = familyname.split()[0].lower() if familyname else ""
    if first_token in ROLE_KEYWORDS and not forename:
        person["role"]       = first_token.capitalize()
        familyname           = ""                    # Name unbekannt
        person["familyname"] = ""

    # ------------------------------------------------------------------
    # 1) Vorab‑Prüfung auf verbotene Einzel‑Namen
    # ------------------------------------------------------------------

        if combined > best_score:
            best_score, best_match = combined, candidate

    if best_score >= 90:
        return best_match, int(best_score)
    
    

    # ------------------------------------------------------------------
    # 3) Fallback: nur Vor‑ oder nur Nachname  (Blacklist gilt schon oben)
    # ------------------------------------------------------------------
    if forename and not familyname:
        for candidate in candidates:
            cand_fn  = candidate.get("forename", "") or ""
            cand_alt = candidate.get("alternate_name", "") or ""
            _, score = fuzzy_match_name(forename, [cand_fn, cand_alt], thresholds["forename"])
            if score and score >= thresholds["forename"]:
                print(f"[DEBUG] 🟡 Match nur Vorname: {forename} ↔ {cand_fn} (Score: {score})")
                return candidate, score

    if familyname and not forename:
        for candidate in candidates:
            cand_ln = candidate.get("familyname", "") or ""
            _, score = fuzzy_match_name(familyname, [cand_ln], thresholds["familyname"])
            if score and score >= thresholds["familyname"]:
                print(f"[DEBUG] 🟡 Match nur Nachname: {familyname} ↔ {cand_ln} (Score: {score})")
                return candidate, score

    print(f"[DEBUG] ❌ Kein Match (max Score: {int(best_score)})")
    return None, 0



from typing import List, Dict, Optional

def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: Optional[List[Dict[str, str]]] = None
) -> List[Dict[str, str]]:
    """
    Entfernt Duplikate aus der Liste von Personen und führt ein Fuzzy-Matching
    gegen eine Liste bekannter Kandidaten durch.

    Args:
        persons: Roh-Liste von {'forename': ..., 'familyname': ..., ...}-Dicts
        known_candidates: Liste von Kandidaten-Dicts; falls None, wird KNOWN_PERSONS verwendet

    Returns:
        Liste von eindeutigen Personen-Dicts (match oder original)
    """
    unique_persons: List[Dict[str, str]] = []
    seen: set = set()
    # Fallback auf globale KNOWN_PERSONS, falls keine übergeben wurden
    candidates = known_candidates if known_candidates is not None else KNOWN_PERSONS

    for person in persons:
        # Normiere den Schlüssel, damit Otto Bollinger nicht doppelt auftaucht
        norm_fn = normalize_name(person.get("forename", ""))["forename"]
        norm_ln = normalize_name(person.get("familyname", ""))["familyname"]
        norm_name = f"{norm_fn} {norm_ln}"

        if norm_name in seen:
            continue

        # Versuche ein Fuzzy-Match
        match, score = match_person(person, candidates=candidates)
        if match and score >= 90:
            unique_persons.append(match)
        else:
            unique_persons.append(person)

        seen.add(norm_name)

    return unique_persons


# Testfunktion für die Überprüfung der Implementierung
def main():
    test_persons = [
        {"forename": "Otto", "familyname": "Bollinger"},
        {"forename": "Otte", "familyname": "Boilinger"},
        {"forename": "Lina", "familyname": "Fingerdick"},
        {"forename": "Alfons", "familyname": "Zimmermann"},
    ]

    deduplicated = deduplicate_persons(test_persons)
    for p in deduplicated:
        print(p)

if __name__ == "__main__":
    main()

def get_best_match_info(
    person: Dict[str, str],
    candidates: List[Dict[str, str]]
) -> Dict[str, Any]:
    """
    Gibt Detailinformationen zum besten Match zurück.
    """
    match, score = match_person(person, candidates)
    return {
        "matched_forename": match.get("forename", "") if match else None,
        "matched_familyname": match.get("familyname", "") if match else None,
        "matched_title": match.get("title", "") if match else None,
        "match_id": match.get("id", "") if match else None,
        "score": score
    }
