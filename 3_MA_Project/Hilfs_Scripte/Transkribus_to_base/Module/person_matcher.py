import os
import re
import unicodedata
import uuid
from typing import List, Dict, Tuple, Optional, Union, Any
import pandas as pd
from collections import defaultdict
from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein

from Module.document_schemas import Person

from Module.Assigned_Roles_Module import (
    POSSIBLE_ROLES,
    ROLE_AFTER_NAME_RE,
    ROLE_BEFORE_NAME_RE,
    map_role_to_schema_entry,
    ROLE_MAPPINGS_DE,

)




#============================================================================
#   BLACKLIST & CONFIGURATION
#============================================================================
# Vereine/Rollen, die niemals als Personenname gelten dürfen
# Nutze die vollständige Liste aus der CSV via Assigned_Roles_Module
ROLE_TOKENS = { r.lower() for r in POSSIBLE_ROLES }

# Und zusätzlich Anrede‑Titel (kannst Du beliebig erweitern)
TITLE_TOKENS = {"herr", "herrn", "frau", "fräulein", "dr", "prof", "der", "pg", "pg."}

# ----- Gesamt‑Blacklist -------
NON_PERSON_TOKENS = ROLE_TOKENS.union(TITLE_TOKENS)
UNMATCHABLE_SINGLE_NAMES = {"otto", "döbele", "doebele"}


#
INITIAL_SURNAME = re.compile(r'^(?P<initial>[A-Z])\.?\s+(?P<surname>[A-ZÄÖÜ][a-zäöüß]+)$')

SOLE_ROLE = set(POSSIBLE_ROLES)
SINGLE_NAME = re.compile(r'^[A-ZÄÖÜ][a-zäöüß]+$')




#============================================================================
#============================================================================
#============================================================================
#============================================================================

def get_matching_thresholds() -> dict[str, int]:
    return {"forename": 80, "familyname": 85}

# CSV-Pfad für bekannte Personen
CSV_PATH_KNOWN_PERSONS = os.path.expanduser(
    "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-person.csv"
)

def load_known_persons_from_csv(path: str = CSV_PATH_KNOWN_PERSONS) -> List[Dict[str, str]]:
    def safe_strip(val: Any) -> str:
        return str(val).strip() if isinstance(val, str) and pd.notna(val) else ""
    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns={
            "Vorname": "forename",
            "Nachname": "familyname",
            "Alternativer Vorname": "alternate_name",
            "nodegoat ID": "nodegoat_id",
            "[Wohnort] Location Reference - Object ID": "home",
            "[Geburt] Date Start": "birth_date",
            "[Tod] Date Start": "death_date",
            "[Mitgliedschaft] in Organisation": "organisation"
        }, inplace=True)
        for col in ["forename","familyname","alternate_name","nodegoat_id",
                    "home","birth_date","death_date","organisation"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()
        persons = []
        for _, r in df.iterrows():
            persons.append({
                "forename": safe_strip(r["forename"]),
                "familyname": safe_strip(r["familyname"]),
                "alternate_name": safe_strip(r.get("alternate_name","")),
                "nodegoat_id": safe_strip(r.get("nodegoat_id","")),
                "home": safe_strip(r.get("home","")),
                "birth_date": safe_strip(r.get("birth_date","")),
                "death_date": safe_strip(r.get("death_date","")),
                "organisation": safe_strip(r.get("organisation","")),
            })
        return persons
    except Exception as e:
        print(f"[ERROR] Fehler beim Laden der Personendaten aus {path}: {e}")
        return []

KNOWN_PERSONS = load_known_persons_from_csv()

# Groundtruth-Namenslisten für Review-Erkennung vorbereiten
GROUNDTRUTH_SURNAMES = {p["familyname"].lower() for p in KNOWN_PERSONS if p["familyname"]}
GROUNDTRUTH_FORENAMES = {p["forename"].lower() for p in KNOWN_PERSONS if p["forename"]}

def appears_in_groundtruth(name: str) -> bool:
    """
    Prüft, ob ein Name in bekannten Vor- oder Nachnamen des Groundtruths auftaucht.
    """
    n = name.strip().lower()
    return n in GROUNDTRUTH_SURNAMES or n in GROUNDTRUTH_FORENAMES


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
EXPANDED_NICKNAME_MAP: Dict[str,str] = {
    nick: canon
    for canon,nicks in NICKNAME_MAP.items()
    for nick in nicks
}
OCR_ERRORS: Dict[str,List[str]] = {
    "ü": ["u","ue"], "ä": ["a","ae"], "ö": ["o","oe"], "ß": ["ss"]
}



#=============================================================================
from rapidfuzz.distance import Levenshtein
from typing import List, Tuple, Optional

def ocr_error_match(
    name: str,
    candidates: List[str]
) -> Tuple[Optional[str], int, float]:
    """
    Versucht, name gegen eine Liste von Kandidaten nur über reine
    Levenshtein-Distanz zu matchen (OCR‑Fehler-Erkennung).

    Args:
        name: erkannter (evtl. falsch geschriebener) Name
        candidates: Liste korrekter Namen

    Returns:
        best_match: der Kandidat mit der geringsten Distanz (None, wenn keine Kandidaten)
        best_dist: rohe Levenshtein-Distanz zwischen name und best_match
        score: normalisierter Ähnlichkeits-Score in [0,100]
    """
    name_lower = name.lower().strip()
    best_match = None
    best_dist = None

    for cand in candidates:
        cand_lower = cand.lower().strip()
        dist = Levenshtein.distance(name_lower, cand_lower)
        if best_dist is None or dist < best_dist:
            best_match, best_dist = cand, dist

    if best_match is None:
        return None, 0, 0.0

    # normalisierter Score: 100 * (1 - dist / max_len)
    max_len = max(len(name_lower), len(best_match))
    score = (1 - best_dist / max_len) * 100 if max_len > 0 else 0.0

    return best_match, best_dist, score


# ----------------------------------------------------------------------------
# Rollennamen strippen
# ----------------------------------------------------------------------------
def strip_roles_from_name(raw_name: str) -> Tuple[str, List[str]]:
    """
    Trennt bekannte Rollennamen ab und gibt bereinigten Namen sowie Liste normierter Rollen.
    """
    # 1) Whitespace trimmen & Punkt, Komma, Doppelpunkt, Semikolon am Ende entfernen
    name = raw_name.strip().rstrip(".,:;")
    lower = name.lower()
    found: List[str] = []

    # Suffix "..., Rolle"
    for key in sorted(POSSIBLE_ROLES, key=len, reverse=True):
        pat = rf"(?:,\s*)?{re.escape(key)}$"
        if re.search(pat, lower):
            canon = map_role_to_schema_entry(key)
            found.append(canon)
            lower = re.sub(pat, "", lower).strip(' ,')
            break

    # Prefix "Rolle ..."
    for key in sorted(POSSIBLE_ROLES, key=len, reverse=True):
        pat = rf"^{re.escape(key)}\s+"
        if re.search(pat, lower):
            canon = map_role_to_schema_entry(key)
            found.append(canon)
            lower = re.sub(pat, "", lower).strip()
            break

    # 2) verbleibenden Namen title‑cased ausgeben
    cleaned = lower.title()
    return cleaned, found


#============================================================================
#   NORMALISIERUNG
#============================================================================
def normalize_name_string(name: str) -> str:
    """
    Lowercase, strip titles, remove diacritics, remove punctuation,
    then map nicknames.
    """
    s = name.lower().strip()
    # Titel entfernen
    s = re.sub(r"\b(dr|prof|herrn?|frau|fräulein|witwe)\.?\s*", "", s)
    # Unicode-NFKD und Diakritika entfernen
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # alles außer Wort- und Leerzeichen löschen
    s = re.sub(r"[^\w\s]", "", s)
    # Nickname-Mapping
    return EXPANDED_NICKNAME_MAP.get(s, s)


def is_initial(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]\.?", s.strip()))

def normalize_name(name: str) -> Dict[str,str]:
    """
    Zerlegt einen voll­ständigen Namen in title, forename und familyname.
    """
    if not name:
        return {"title": "", "forename": "", "familyname": ""}
    s = name.strip()
    # Titel extrahieren
    m = re.match(r"(?i)\\b(dr|prof|herr|frau|fräulein|witwe)\\.?\\s+", s)
    title = m.group(1).capitalize() if m else ""
    s = re.sub(r"(?i)\\b(dr|prof|herr|frau|fräulein|witwe)\\.?\\s+", "", s)
    parts = re.sub(r"[^\w\s]", "", s).split()
    if not parts:
        # komplett nichts übrig, gib leere Namen zurück
        return {"title": title, "forename": "", "familyname": ""}
    if len(parts) == 1:
        return {"title": title, "forename": parts[0].capitalize(), "familyname": ""}
    return {
        "title": title,
        "forename": parts[0].capitalize(),
        "familyname": " ".join(parts[1:]).capitalize()
    }


#============================================================================
#   Levenshtein-Fallback
#============================================================================
def ocr_error_match(
    name: str,
    candidates: List[str]
) -> Tuple[Optional[str], int, float]:
    """
    Versucht, name gegen Kandidaten rein per Levenshtein zu matchen.
    """
    name_lower = name.lower().strip()
    best_match = None
    best_dist = None
    for cand in candidates:
        cand_lower = cand.lower().strip()
        dist = Levenshtein.distance(name_lower, cand_lower)
        if best_dist is None or dist < best_dist:
            best_match, best_dist = cand, dist
    if best_match is None:
        return None, 0, 0.0
    max_len = max(len(name_lower), len(best_match))
    score = (1 - best_dist / max_len) * 100 if max_len > 0 else 0.0
    return best_match, best_dist, score
# ----------------------------------------------------------------------------
# Fuzzy Matching
# ----------------------------------------------------------------------------
def fuzzy_match_name(name: str, candidates: List[str], threshold: int) -> Tuple[Optional[str],int]:
    best, score = None, 0
    norm = normalize_name_string(name)
    for c in candidates:
        sc = fuzz.ratio(normalize_name_string(c), norm)
        if sc > score:
            best, score = c, sc
    return (best, score) if score>=threshold else (None,0)


def match_person(
    person: Dict[str, str],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS
) -> Tuple[Optional[Dict[str, str]], int]:
    """
    Erweitertes Matching mit folgenden Szenarien:
    1) Klassische Normalisierung, Rollenerkennung, Initial-Fallbacks.
    2) Klassisches Fuzzy-Matching (auch mit Swap).
    3) Levenshtein-Fallbacks.
    4) Neue Szenarien:
       a) Nur Nachname + Rolle
       b) Nur Rolle
       c) Nichts vorhanden
       d) Vertauschter Vor-/Nachname
       e) Rolle als Name
    """
    

    fn_raw = (person.get("forename") or "").strip()
    ln_raw = (person.get("familyname") or "").strip()
    role_raw = (person.get("role") or "").strip()

    fn_stripped, roles = strip_roles_from_name(fn_raw)
    fn = re.sub(r"[():]", "", fn_stripped).strip()
    ln = ln_raw.split(",", 1)[0].strip() if ln_raw else ""
    print(f"[DEBUG] Starte Matching für: forename='{fn}', familyname='{ln}', role='{role_raw}'")
    tokens = [w.strip(",:;.").lower() for w in fn.split()]


    context = person.get("content_transcription", "") or ""
    fn_in_unmatchable = fn.lower() in UNMATCHABLE_SINGLE_NAMES
    ln_in_unmatchable = ln.lower() in UNMATCHABLE_SINGLE_NAMES

    if (fn_in_unmatchable and not ln) or (ln_in_unmatchable and not fn):
        full = fn + " " + ln
        inv  = ln + " " + fn
        if re.search(rf"\b{re.escape(full.strip())}\b", context, flags=re.IGNORECASE) or \
        re.search(rf"\b{re.escape(inv.strip())}\b", context, flags=re.IGNORECASE):
            print(f"[DEBUG] Kontext rettet '{fn}'/'{ln}' durch Fund von '{full}' oder '{inv}' im Text")
        else:
            print(f"[DEBUG] Unmatchable single name: '{fn}' / '{ln}' → keine Nodegoat-ID, aber wird übernommen.")
        return None, 0


    # Blacklist-Token ohne Nachname
    if any(t in NON_PERSON_TOKENS for t in tokens) and not ln:
        print(f"[DEBUG] Dropping because token in blacklist: fn='{fn}', ln='{ln}'")
        return None, 0

    # Token ist nur Rolle (und kein Nachname)
    if not ln and fn.lower() in ROLE_TOKENS:
        print(f"[DEBUG] Dropping token as name (role detected): fn='{fn}', ln='{ln}'")
        return None, 0

    # Verwende die vollständige Rolle-Tokens aus der CSV statt hardcoded Liste
    if not ln and fn.lower() in ROLE_TOKENS:
        print(f"[DEBUG] Dropping token as name (role detected): fn='{fn}', ln='{ln}'")
        return None, 0

    thr = get_matching_thresholds()
    norm_fn, norm_ln = normalize_name_string(fn), normalize_name_string(ln)

    # 1) Initial-Fallback
    if is_initial(fn):
        init = fn[0].upper()
        fam_norm = normalize_name_string(ln)
        for c in candidates:
            if c["forename"] and c["forename"][0].upper() == init \
               and normalize_name_string(c["familyname"]) == fam_norm:
                return c, 95

    # 2) Klassisches Fuzzy-Matching
    best, best_score = None, 0
    for c in candidates:
        fn_c, ln_c = c["forename"], c["familyname"]
        fn_score = (
            100
            if is_initial(fn) and fn_c and fn[0].upper() == fn_c[0].upper()
            else fuzzy_match_name(fn, [fn_c, c.get("alternate_name", "")], thr["forename"])[1]
        )
        ln_score = (
            100
            if is_initial(ln) and ln_c and ln[0].upper() == ln_c[0].upper()
            else fuzzy_match_name(ln, [ln_c], thr["familyname"])[1]
        )
        combo = 0.4 * fn_score + 0.6 * ln_score
        if combo > best_score:
            best_score, best = combo, c
        # Vor-/Nachnamen-Swap
        if norm_fn == normalize_name_string(ln_c) and norm_ln == normalize_name_string(fn_c):
            return c, 100

    if best_score >= max(thr.values()):
        return best, int(best_score)

    # 3a) Levenshtein: Vorname stimmt, Nachname fast gleich
    for c in candidates:
        if normalize_name_string(fn) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(ln.lower(), c["familyname"].lower()) <= 1:
                return c, 90

    # 3b) Levenshtein: Swap-Erkennung
    for c in candidates:
        if normalize_name_string(ln) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(fn.lower(), c["familyname"].lower()) <= 1:
                return c, 90

    # 3c) Nur Nachname + Fuzzy/Levenshtein
    if not fn and ln:
        for c in candidates:
            ln_score = fuzzy_match_name(ln, [c["familyname"]], thr["familyname"])[1]
            if ln_score >= thr["familyname"]:
                return c, ln_score

    # 4) Nur Rolle bekannt (versuche Matching auf Rolle)
    if not fn and not ln and role_raw:
        for c in candidates:
            if role_raw.lower() in (c.get("role", "").lower(), c.get("alternate_name", "").lower()):
                return c, 80

    # 5) Rolle als Vorname/Nachname erkannt (z. B. „Ehrenvorsitzender Burger“)
    if fn.lower() in NON_PERSON_TOKENS and ln:
        for c in candidates:
            if normalize_name_string(ln) == normalize_name_string(c["familyname"]):
                return c, 85
    if ln.lower() in NON_PERSON_TOKENS and fn:
        for c in candidates:
            if normalize_name_string(fn) == normalize_name_string(c["familyname"]):
                return c, 85

    # 6) Nur Familienname (mit Levenshtein als letzter Fallback)
    if not fn and ln:
        fams = [c["familyname"] for c in candidates if c["familyname"]]
        matched, dist, _ = ocr_error_match(ln, fams)
        if matched and dist <= 2:
            c = next(x for x in candidates if x["familyname"] == matched)
            return c, 85
    
    # 7) wenn unverifiable dann in Output zur review speichern
    if any([fn, ln, role_raw]):
        if fn and not ln:
            reason = "forename_only"
        elif ln and not fn:
            reason = "familyname_only"
        elif not fn and not ln and role_raw:
            reason = "role_only"
        else:
            reason = "partial_info"

        unverified = {
            "forename": fn,
            "familyname": ln,
            "role": role_raw,
            "title": person.get("title", ""),
            "alternate_name": "",
            "nodegoat_id": "",
            "match_score": 0,
            "confidence": "unverified",
            "needs_review": True,
            "review_reason": reason
        }
        return unverified, 0

    # -- Neuer Fallback: Auch unvollständige Namen behalten, wenn plausibel
    if any([fn, ln, role_raw]):
        review_reason_parts = []
        if not fn:
            review_reason_parts.append("missing_forename")
        if not ln:
            review_reason_parts.append("missing_familyname")
        if not role_raw:
            review_reason_parts.append("missing_role")

        review_reason = "; ".join(review_reason_parts)

        keep_name = (fn and appears_in_groundtruth(fn)) or (ln and appears_in_groundtruth(ln))
        if keep_name or role_raw:
            print(f"[NEEDS_REVIEW] Aufnahme trotz fehlender ID: {fn} {ln} ({role_raw})")
            return {
                "forename": fn,
                "familyname": ln,
                "title": person.get("title", ""),
                "alternate_name": "",
                "nodegoat_id": "",
                "role": role_raw,
                "role_schema": map_role_to_schema_entry(role_raw),
                "match_score": None,
                "confidence": "partial-no-id",
                "needs_review": True,
                "review_reason": review_reason
            }, 0
        
    # Komplett leere oder ungültige Person – wirklich ignorieren
    return None, 0


# ----------------------------------------------------------------------------
# Extract Person Data mit Rolleninfos
# ----------------------------------------------------------------------------
def extract_person_data(row: Dict[str,Any]) -> Dict[str,str]:
    # bereits getrennt?
    if row.get("forename") and row.get("familyname"):
        result = {k:str(row.get(k,"")).strip() for k in [
            "forename","familyname","alternate_name","title",
            "nodegoat_id","home","birth_date","death_date",
            "organisation","role","role_schema","associated_organisation",
            "stripped_role"
        ]}
        # Ensure role_schema is populated correctly if role exists
        if result.get("role") and not result.get("role_schema"):
            from Module.Assigned_Roles_Module import normalize_and_match_role, map_role_to_schema_entry
            normalized_role = normalize_and_match_role(result["role"])
            if normalized_role:
                result["role"] = normalized_role
            result["role_schema"] = map_role_to_schema_entry(result["role"])
            print(f"[DEBUG] person_matcher: role_schema = {result['role_schema']!r}")
        return result

    raw = row.get("name","").strip()
    m = re.match(r"^(Herrn?|Frau|Fräulein|Dr\.?|Prof\.?)\s+(.+)$", raw, flags=re.IGNORECASE)
    title = m.group(1).capitalize() if m else ""
    if m: raw = m.group(2).strip()
    clean, roles = strip_roles_from_name(raw)

    # Use normalize_and_match_role for consistency
    from Module.Assigned_Roles_Module import normalize_and_match_role, map_role_to_schema_entry
    role = roles[0] if roles else ""
    normalized_role = normalize_and_match_role(role) if role else ""
    role = normalized_role if normalized_role else role
    role_schema = map_role_to_schema_entry(role) if role else ""

    parts = normalize_name(clean)
    return {
        "forename": parts["forename"],
        "familyname": parts["familyname"],
        "alternate_name": row.get("alternate_name","").strip(),
        "title": title or parts["title"],
        "nodegoat_id": row.get("nodegoat_id","").strip(),
        "home": row.get("home","").strip(),
        "birth_date": row.get("birth_date","").strip(),
        "death_date": row.get("death_date","").strip(),
        "organisation": row.get("organisation","").strip(),
        "stripped_role": roles,
        "role": role,
        "role_schema": role_schema,
        "associated_organisation": row.get("associated_organisation","").strip(),
    }
from typing import List, Dict, Tuple, Optional, Any, Union

# ----------------------------------------------------------------------------
# Split und Enrichment
# ----------------------------------------------------------------------------
def split_and_enrich_persons(
    raw_persons: List[Union[str, Dict[str, str]]],
    content_transcription: str,
    document_id: Optional[str] = None,
    candidates: Optional[List[Dict[str, str]]] = None
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    
    # 0) Normalisiere alle Eingaben zu Dicts mit 'name'
    normalized = []
    for p in raw_persons:
        if isinstance(p, dict) and "forename" in p:
            name_str = f"{p['forename']} {p['familyname']}".strip()
        elif isinstance(p, dict) and "name" in p:
            name_str = p["name"].strip()
        else:
            name_str = str(p).strip()

        # **Skip empty names**
        if not name_str:
            continue

        normalized.append({"name": name_str})
    raw_persons = normalized

    # 1) Duplikat‑Filter & Matching
    seen, matched, unmatched = set(), [], []
    cand_list = candidates or KNOWN_PERSONS

    for p in raw_persons:
        raw_token = p["name"]

        # 2) Extrahiere sauberes Person‑Dict
        person = extract_person_data({"name": raw_token})

        # 3) Key für Duplikat‑Filter
        key = person.get("nodegoat_id") or (
            f"{normalize_name_string(person['forename'])} "
            f"{normalize_name_string(person['familyname'])}"
        )
        if not key or key in seen:
            continue
        seen.add(key)

        # 4) Fuzzy‑Match mit Kontextübergabe
        person["content_transcription"] = content_transcription  # für Kontext-Logik in match_person
        match, score = match_person(person, candidates=cand_list)


        # 5) Ergebnis sammeln
        if match and score > 0:
            matched.append({
                "raw_token":   raw_token,
                "forename":    match["forename"],
                "familyname":  match["familyname"],
                "nodegoat_id": match["nodegoat_id"],
                "match_score": score,
                "confidence":  "fuzzy",
            })
        else:
            person["match_score"] = 0
            person["confidence"] = ""
            person["raw_token"] = raw_token
            unmatched.append(person)

        # —– : Spezial-Fälle aus unmatched in matched verschieben —–
    additional = []
    role_only_entries = []

    for person in unmatched:
        name = person["raw_token"].strip()
        m = INITIAL_SURNAME.match(name)

        # Fall 1: Initial + Nachname (z.B. "A. Müller")
        if m:
            person["match_score"] = 50
            person["confidence"] = "initial"
            additional.append(person)
            continue

        # Fall 2: Einzelne Rolle ohne Person (z.B. "Vorsitzender") -> in role_only speichern
        if name in SOLE_ROLE:
            # Statt als Person zu behandeln, als reine Rolle ausgeben
            role_entry = {
                "forename": "",
                "familyname": "",
                "alternate_name": "",
                "title": "",
                "role": name,  # Die Rolle ist der gesamte Token
                "role_schema": map_role_to_schema_entry(name),
                "associated_place": "",
                "associated_organisation": person.get("associated_organisation", ""),
                "nodegoat_id": "",
                "match_score": 40,
                "confidence": "role_only",
                "raw_token": name
            }
            role_only_entries.append(role_entry)
            continue

        # Fall 3: Name, Rolle (z.B. "Schmidt, Vorsitzender")
        parts = [p.strip() for p in name.split(",")]
        if len(parts) == 2 and SINGLE_NAME.match(parts[0]) and parts[1] in SOLE_ROLE:
            person["forename"] = ""
            person["familyname"] = parts[0]
            person["role"] = parts[1]
            person["role_schema"] = map_role_to_schema_entry(parts[1])
            person["match_score"] = 45
            person["confidence"] = "name-role"
            additional.append(person)
            continue

        # Fall 4: Einzelner Name (z.B. "Schmidt")
        if SINGLE_NAME.match(name):
            person["match_score"] = 30
            person["confidence"] = "single-name"
            additional.append(person)
            continue

    matched.extend(additional)
    # Verwende set() basierend auf Memory-ID für Vergleich
    matched_ids = {id(p) for p in additional}
    unmatched = [p for p in unmatched if id(p) not in matched_ids]

    # Reine Rollen dem unmatched-Array hinzufügen (mit spezieller Markierung)
    if role_only_entries:
        unmatched.extend(role_only_entries)

    return matched, unmatched


# ----------------------------------------------------------------------------
# Deduplication
# ----------------------------------------------------------------------------
def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: Optional[List[Dict[str, str]]] = None
) -> List[Dict[str, str]]:
    """
    Dedupliziert Personeneinträge basierend auf nodegoat_id oder normalisierten Namen.
    Priorität haben Personen mit nodegoat_id, gefolgt von der besten Übereinstimmung.
    """
    import uuid

    person_groups = {}
    candidates = known_candidates or KNOWN_PERSONS

    # Erster Durchlauf: Gruppiere nach ID oder Namen
    for p in persons:
        nodegoat_id = p.get("nodegoat_id", "").strip()
        forename = normalize_name_string(p.get("forename", ""))
        familyname = normalize_name_string(p.get("familyname", ""))

        # Primärer Schlüssel: bevorzugt ID, dann vollständiger Name, dann fallback auf Einzelnamen
        if nodegoat_id:
            key = nodegoat_id
        elif forename and familyname:
            key = f"{forename} {familyname}"
        elif familyname:
            key = familyname
        elif forename:
            key = forename
        else:
            # Erzeuge eindeutigen Dummy-Key für roll-only-Personen
            role_label = p.get("role", "").strip() or "unbekannte Rolle"
            key = f"role_only::{role_label}::{uuid.uuid4().hex[:8]}"
            print(f"[INFO] Rolle ohne Namen wird übernommen: {role_label}")

        if key not in person_groups:
            person_groups[key] = []
        person_groups[key].append(p)

    # Zweiter Durchlauf: Wähle aus jeder Gruppe den besten Eintrag zur Repräsentation
    unique = []

    for key, group in person_groups.items():
        # Falls Einträge mit nodegoat_id vorhanden sind, priorisiere diese
        entries_with_id = [p for p in group if p.get("nodegoat_id")]

        if entries_with_id:
            # Wähle den Eintrag mit höchstem match_score
            if entries_with_id:
                # Wähle Eintrag mit höchstem Match-Score
                best_entry = max(entries_with_id, key=lambda p: float(p.get("match_score", 0) or 0))
                merged = best_entry.copy()

                # Anzahl Erwähnungen aufsummieren
                merged["mentioned_count"] = sum(int(p.get("mentioned_count", 1) or 1) for p in group)

                # Rollen kombinieren
                all_roles = {p.get("role", "").strip() for p in group if p.get("role")}
                if all_roles:
                    merged["role"] = "; ".join(sorted(all_roles))
                    # Update role_schema based on the combined roles
                    from Module.Assigned_Roles_Module import map_role_to_schema_entry
                    merged["role_schema"] = map_role_to_schema_entry(sorted(all_roles)[0]) if sorted(all_roles) else ""

                # Felder ergänzen, wenn leer
                for field in ["title", "alternate_name", "associated_place", "associated_organisation"]:
                    if not merged.get(field):
                        for p in group:
                            if p.get(field):
                                merged[field] = p[field]
                                break

                # match_score & confidence ergänzen, falls noch leer
                if not merged.get("match_score"):
                    merged["match_score"] = max(float(p.get("match_score", 0) or 0) for p in group)
                if not merged.get("confidence"):
                    merged["confidence"] = next((p.get("confidence") for p in group if p.get("confidence")), "")

                unique.append(merged)
                

        else:
            best_entry = sorted(group, key=lambda p: p.get("match_score", 0), reverse=True)[0]
            match, score = match_person(best_entry, candidates)

            if match and score >= 90:
                enriched = match.copy()
                enriched["match_score"] = score
                enriched["confidence"] = "fuzzy"
                enriched["mentioned_count"] = sum(int(p.get("mentioned_count", 1) or 1) for p in group)

                enriched["associated_place"] = best_entry.get("associated_place", "")
                enriched["associated_organisation"] = best_entry.get("associated_organisation", "")

                all_roles = list({p.get("role", "").strip() for p in group if p.get("role")})
                if all_roles:
                    enriched["role"] = "; ".join(all_roles)

                unique.append(enriched)
                print(f"[DEBUG] Behalte Person (fuzzy): {enriched.get('forename', '')} {enriched.get('familyname', '')}, Score: {score}")
            else:
                best_entry["match_score"] = score
                best_entry["confidence"] = "none"
                best_entry["mentioned_count"] = sum(int(p.get("mentioned_count", 1) or 1) for p in group)

                all_roles = list({p.get("role", "").strip() for p in group if p.get("role")})
                if all_roles:
                    best_entry["role"] = "; ".join(all_roles)

                unique.append(best_entry)
                print(f"[DEBUG] Behalte Person (no match): {best_entry.get('forename', '')} {best_entry.get('familyname', '')}, Score: {score}")


    return unique

def assess_llm_entry_score(
    forename: str,
    familyname: str,
    role: str
) -> Tuple[int, str, bool, str]:
    """
    Bewertet eine LLM-generierte Personeneintragung hinsichtlich Vollständigkeit.

    Gibt zurück:
    - match_score (int): Score für die Matching-Güte (30 = unvollständig, 50 = brauchbar)
    - confidence (str): Herkunft und Zuverlässigkeit der Information ("llm", "llm-incomplete")
    - needs_review (bool): Muss der Eintrag manuell überprüft werden?
    - review_reason (str): Erklärung für die Prüfbedürftigkeit
    """
    is_incomplete = not familyname or not forename or not role

    review_reasons = []
    if not forename:
        review_reasons.append("missing_forename")
    if not familyname:
        review_reasons.append("missing_familyname")
    if not role:
        review_reasons.append("missing_role")

    score = 30 if is_incomplete else 50
    confidence = "llm-incomplete" if is_incomplete else "llm"
    needs_review = is_incomplete
    review_reason = "; ".join(review_reasons) if review_reasons else ""

    return score, confidence, needs_review, review_reason


# ----------------------------------------------------------------------------
# Detail‑Info zum besten Match
# ----------------------------------------------------------------------------
def get_best_match_info(
    person: Dict[str, str],
    candidates: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    cand_list = candidates or KNOWN_PERSONS
    match, score = match_person(person, cand_list)
    return {
        "matched_forename":   match.get("forename") if match else None,
        "matched_familyname": match.get("familyname") if match else None,
        "matched_title":      match.get("title") if match else None,
        "match_id":           match.get("nodegoat_id") if match else None,
        "score":              score
    }


def count_mentions_in_transcript(persons: List[Person], transcript: str) -> List[Person]:
    updated = []
    for p in persons:
        count = 0
        fn = (p.forename or "").strip()
        ln = (p.familyname or "").strip()

        # (1) Bevorzugt vollständiger Name
        if fn and ln:
            full_pattern = re.compile(rf"\b{re.escape(fn)}\s+{re.escape(ln)}\b", re.IGNORECASE)
            count = len(full_pattern.findall(transcript))

        # (2) Wenn nur Nachname vorhanden
        elif ln:
            last_pattern = re.compile(rf"\b{re.escape(ln)}\b", re.IGNORECASE)
            count = len(last_pattern.findall(transcript))

        # (3) Wenn nur Vorname vorhanden (z. B. Otto)
        elif fn:
            first_pattern = re.compile(rf"\b{re.escape(fn)}\b", re.IGNORECASE)
            count = len(first_pattern.findall(transcript))

        p.mentioned_count = count
        updated.append(p)

    return updated
def deduplicate_and_group_persons(persons: List[Union[Dict[str, Any], Person]]) -> List[Person]:
    from collections import defaultdict
    nodegoat_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    unmatched: List[Dict[str, Any]] = []

    def normalize(s: str) -> str:
        return s.strip().lower()

    def ensure_dict(p: Union[Person, Dict[str, Any]]) -> Dict[str, Any]:
        return p.to_dict() if isinstance(p, Person) else p

    final: List[Person] = []

    # 1) Mit nodegoat_id gruppieren
    for p in persons:
        p = ensure_dict(p)
        if p.get("nodegoat_id"):
            nodegoat_groups[p["nodegoat_id"].strip()].append(p)
        else:
            unmatched.append(p)

    for nodegoat_id, group in nodegoat_groups.items():
        best = max(group, key=lambda x: float(x.get("match_score", 0) or 0))
        combined_roles = "; ".join(sorted(set(p.get("role", "") for p in group if p.get("role"))))

        best["role"] = combined_roles
        best["mentioned_count"] = sum(int(p.get("mentioned_count", 1)) for p in group)
        best["recipient_score"] = max(int(p.get("recipient_score", 0) or 0) for p in group)
        final.append(Person.from_dict(best))
        print(f"[DEBUG] Grouped by nodegoat_id {nodegoat_id}: {best.get('forename')} {best.get('familyname')}, Score: {best.get('match_score')}")

    # 2) Manuelle Gruppierung nach Namen (nur wenn keine ID)
    for entry in unmatched:
        entry = ensure_dict(entry)
        fn = normalize(entry.get("forename", ""))
        ln = normalize(entry.get("familyname", ""))
        role = normalize(entry.get("role", ""))
        key = f"{fn}|{ln}|{role}"

        matched = False
        for target in final:
            tfn = normalize(getattr(target, "forename", ""))
            tln = normalize(getattr(target, "familyname", ""))
            tr  = normalize(getattr(target, "role", ""))

            conditions = [
                fn and ln and fn == tfn and ln == tln,
                fn and ln and fn == tln and ln == tfn,
                fn == tr or ln == tr or role == tfn or role == tln
            ]

            if any(conditions):
                print(f"[MERGE] Kombiniere {fn} {ln} ({role}) mit {tfn} {tln} ({tr})")
                target.mentioned_count += int(entry.get("mentioned_count", 1))
                if entry.get("role"):
                    combined = set(filter(None, (target.role or "").split("; ")))
                    combined.add(entry["role"])
                    target.role = "; ".join(sorted(combined))
                
                target.recipient_score = max(getattr(target, "recipient_score", 0), entry.get("recipient_score", 0))
                
                matched = True
                break

        if not matched:
            entry["mentioned_count"] = int(entry.get("mentioned_count", 1))
            # Ensure recipient_score is preserved in new entries
            entry["recipient_score"] = int(entry.get("recipient_score", 0) or 0)
            # Print recipient_score for debugging
            print(f"[DEBUG] Neuer Eintrag mit recipient_score={entry['recipient_score']} übernommen: {entry.get('forename')} {entry.get('familyname')} {entry.get('role')}")
            final.append(Person.from_dict(entry))
            
    recipient_score_lookup = {
        ensure_dict(p)["nodegoat_id"]: ensure_dict(p).get("recipient_score", 0)
        for p in persons
        if ensure_dict(p).get("recipient_score", 0) > 0 and ensure_dict(p).get("nodegoat_id")
    }

    # Weise recipient_score zurück an deduplizierte finale Personen
    for person in final:
        nid = person.nodegoat_id
        if nid in recipient_score_lookup:
            person.recipient_score = max(person.recipient_score or 0, recipient_score_lookup[nid])    

    print("\n[DEBUG] Finale erwähnte Personen nach Deduplikation:")
    for p in final:
        print(f" → {p.forename} {p.familyname}, Rolle: {p.role}, ID: {p.nodegoat_id}, Score: {p.match_score}, Count: {p.mentioned_count}")

    return final