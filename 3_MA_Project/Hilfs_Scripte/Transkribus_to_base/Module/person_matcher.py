import os
import re
import unicodedata
from typing import List, Dict, Tuple, Optional, Union, Any
import pandas as pd
from rapidfuzz import fuzz
from Module.Assigned_Roles_Module import (
    POSSIBLE_ROLES,
    ROLE_AFTER_NAME_RE,
    ROLE_BEFORE_NAME_RE,
    map_role_to_schema_entry,
)
from rapidfuzz.distance import Levenshtein

from Module.Assigned_Roles_Module import (
    POSSIBLE_ROLES,
    map_role_to_schema_entry,
)

#============================================================================
#   BLACKLIST & CONFIGURATION
#============================================================================
# Vereine/Rollen, die niemals als Personenname gelten dürfen
ROLE_TOKENS = { r.lower() for r in POSSIBLE_ROLES }

# Und zusätzlich Anrede‑Titel (kannst Du beliebig erweitern)
TITLE_TOKENS = {"herr", "herrn", "frau", "fräulein", "dr", "prof", "der", "pg", "pg."}

# ----- Gesamt‑Blacklist -------
NON_PERSON_TOKENS = ROLE_TOKENS.union(TITLE_TOKENS)
UNMATCHABLE_SINGLE_NAMES = {"otto", "döbele", "doebele"}



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
# ----------------------------------------------------------------------------
# Person Match
# ----------------------------------------------------------------------------
def match_person(
    person: Dict[str, str],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS
) -> Tuple[Optional[Dict[str, str]], int]:
    """
    1) Entfernt Rollentitel und Sonderzeichen aus dem Vornamen.
    2) Schneidet alles nach dem ersten Komma im Nachnamen ab.
    3) Verwirft reine Rollen‑Tokens (z.B. 'Mutter').
    4) Initial‑Fallback: z.B. 'C.' → 'Carl'.
    5) Klassisches Fuzzy‑Matching.
    6) Verschiedene Levenshtein‑Fallbacks.
    """
    # Roh‑Strings
    fn_raw = (person.get("forename") or "").strip()
    ln_raw = (person.get("familyname") or "").strip()

    # 1) Rollen‑Suffix/Prefix & Klammern/Doppelpunkte entfernen
    fn_stripped, roles = strip_roles_from_name(fn_raw)
    if roles and not ln_raw:
        print(f"[DEBUG] Dropping because stripped role: fn_raw='{fn_raw}', role='{roles[0]}'")
        return None, 0
    # entferne noch verbliebene Klammern/Doppelpunkte
    fn = re.sub(r"[():]", "", fn_stripped).strip()

    # 1a) Pre‑Filter: wenn irgendein Teilwort in der Blacklist, verwerfen
    tokens = [w.strip(",:;.").lower() for w in fn.split()]
    if any(t in NON_PERSON_TOKENS for t in tokens) and not ln_raw:
        print(f"[DEBUG] Dropping because token in blacklist: fn='{fn}', ln='{ln_raw}'")
        return None, 0

    # 2) Nachname: alles nach dem ersten Komma weg
    ln = ln_raw.split(",", 1)[0].strip() if ln_raw else ""

    # 3) Verwerfe reine Rollen‑Tokens
    ROLE_TOKENS = {"mutter", "prokurist", "vereinsführer", "errn", "herrn"}
    if not ln or fn.lower() in ROLE_TOKENS:
        print(f"[DEBUG] Dropping token as name: fn='{fn}', ln='{ln}'")
        return None, 0

    thr = get_matching_thresholds()
    

    # 4) Initial‑Fallback
    if is_initial(fn):
        init = fn[0].upper()
        fam_norm = normalize_name_string(ln)
        for c in candidates:
            if c["forename"] and c["forename"][0].upper() == init \
               and normalize_name_string(c["familyname"]) == fam_norm:
                print(f"[DEBUG] initial matched: {c['forename']} {c['familyname']}")
                person["forename"], person["familyname"] = c["forename"], c["familyname"]
                return c, 95

    # 5) Klassisches Fuzzy‑Matching
    best, best_score = None, 0
    norm_fn, norm_ln = normalize_name_string(fn), normalize_name_string(ln)
    for c in candidates:
        fn_c, ln_c = c["forename"], c["familyname"]
        fn_score = (
            100
            if is_initial(fn) and fn_c and fn[0].upper() == fn_c[0].upper()
            else fuzzy_match_name(fn, [fn_c, c.get("alternate_name","")], thr["forename"])[1]
        )
        ln_score = (
            100
            if is_initial(ln) and ln_c and ln[0].upper() == ln_c[0].upper()
            else fuzzy_match_name(ln, [ln_c], thr["familyname"])[1]
        )
        combo = 0.4 * fn_score + 0.6 * ln_score
        if combo > best_score:
            best_score, best = combo, c
        # Exakter Vor-/Nachnamen‑Swap
        if norm_fn == normalize_name_string(ln_c) and norm_ln == normalize_name_string(fn_c):
            return c, 100

    if best_score >= max(thr.values()):
        return best, int(best_score)

    # 6a) Levenshtein‑Fallback gleicher Vorname
    for c in candidates:
        if normalize_name_string(fn) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(ln.lower(), c["familyname"].lower()) <= 1:
                person["forename"], person["familyname"] = c["forename"], c["familyname"]
                return c, 90

    # 6b) Swap‑Fallback
    for c in candidates:
        if normalize_name_string(ln) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(fn.lower(), c["familyname"].lower()) <= 1:
                person["forename"], person["familyname"] = c["forename"], c["familyname"]
                return c, 90

    # 6c) Levenshtein‑Fallback auf Nachname
    fams = [c["familyname"] for c in candidates if c["familyname"]]
    matched, dist, _ = ocr_error_match(ln, fams)
    if matched and dist <= 2:
        c = next(x for x in candidates if x["familyname"] == matched)
        if normalize_name_string(fn) == normalize_name_string(c["forename"]):
            person["forename"], person["familyname"] = c["forename"], c["familyname"]
            return c, 90

    # kein Match gefunden
    return None, 0

# ----------------------------------------------------------------------------
# Extract Person Data mit Rolleninfos
# ----------------------------------------------------------------------------
def extract_person_data(row: Dict[str,Any]) -> Dict[str,str]:
    # bereits getrennt?
    if row.get("forename") and row.get("familyname"):
        return {k:str(row.get(k,"")).strip() for k in [
            "forename","familyname","alternate_name","title",
            "nodegoat_id","home","birth_date","death_date",
            "organisation","role","role_schema","associated_organisation",
            "stripped_role"
        ]}
    raw = row.get("name","").strip()
    m = re.match(r"^(Herrn?|Frau|Fräulein|Dr\.?|Prof\.?)\s+(.+)$", raw, flags=re.IGNORECASE)
    title = m.group(1).capitalize() if m else ""
    if m: raw = m.group(2).strip()
    clean, roles = strip_roles_from_name(raw)
    role = roles[0] if roles else ""
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

        # 4) Fuzzy‑Match
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
            unmatched.append({"raw_token": raw_token})

    return matched, unmatched


# ----------------------------------------------------------------------------
# Deduplication
# ----------------------------------------------------------------------------
def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: Optional[List[Dict[str, str]]] = None
) -> List[Dict[str, str]]:
    seen, unique = set(), []
    candidates = known_candidates or KNOWN_PERSONS
    for p in persons:
        key = f"{normalize_name_string(p['forename'])} {normalize_name_string(p['familyname'])}"
        if not key or key in seen:
            continue
        seen.add(key)
        match, score = match_person(p, candidates)
        if match and score >= 90:
            enriched = {**match, "match_score": score, "confidence": "fuzzy"}
            enriched.setdefault("role", p.get("role", ""))
            enriched.setdefault("role_schema", p.get("role_schema", ""))
            enriched.setdefault("associated_organisation", p.get("associated_organisation", ""))
            unique.append(enriched)
        else:
            p["match_score"], p["confidence"] = score, "none"
            unique.append(p)
    return unique


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
