from curses import raw
import os
import re
import unicodedata
import uuid
import string
from typing import List, Dict, Tuple, Optional, Any, Union
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
    extract_role_from_raw_name,)
from Module.letter_metadata_matcher import (
    _RECIPIENT_RE,
    INDIRECT_RECIPIENT_PATTERNS,
    GREETING_PATTERNS as CLOSING_PATTERNS,
    ROLE_PATTERNS, direct_patterns
)
# ============================================================================
#   BLACKLIST & CONFIGURATION
# ============================================================================

# Vereine/Rollen, die niemals als Personenname gelten dürfen
ROLE_TOKENS = {r.lower() for r in POSSIBLE_ROLES}

# Anrede‑Titel
MALE_TITLE_TOKENS = {"herr", "herrn"}
FEMALE_TITLE_TOKENS = {"frau", "fräulein", "frauchen", "ww.", "witwe"}
NEUTRAL_TITLE_TOKENS = {"dr", "prof", "pg", "pg."}

TITLE_TOKENS = MALE_TITLE_TOKENS | FEMALE_TITLE_TOKENS | NEUTRAL_TITLE_TOKENS


# Begriffe wie "Grüße", "Gruss", "Gruß" – typisch in Briefabschlüssen
BLACKLIST_TOKENS = {
    "grüße",
    "grüsse",
    "gruß",
    "gruss",
    "herzliche grüße",
    "mit freundlichen grüßen",
    "freundliche grüsse",
    "mit besten grüßen",
    "dank",
    "danke",
}
PRONOUN_TOKENS = {
    "der", "die", "das", "des", "dem", "den", "ein", "eine", "einer", "eines", "einem","mein", "dein", "sein", "ihr", "unser", "euer","mein", 
    "meine", "meinen", "meinem", "meiner", "meines",
    "dein", "deine", "deinen", "deinem", "deiner", "deines",
    "sein", "seine", "seinen", "seinem", "seiner", "seines",
    "ihr", "ihre", "ihren", "ihrem", "ihrer", "ihres",
    "unser", "unsere", "unseren", "unserem", "unserer", "unseres",
    "euer", "eure", "euren", "eurem", "eurer", "eures",
    "ihr",

}

# ----- Gesamt‑Blacklist -------
NON_PERSON_TOKENS= ROLE_TOKENS.union(
    TITLE_TOKENS,
    BLACKLIST_TOKENS,
    PRONOUN_TOKENS
)

# Einzelne Tokens, die nie als eigenständige Personenbezeichnung zählen sollen
UNMATCHABLE_SINGLE_NAMES = {"otto", "döbele", "doebele"}




# ============================================================================
# ============================================================================
# ============================================================================
# ============================================================================


def get_matching_thresholds() -> dict[str, int]:
    return {"forename": 80, "familyname": 85}


# CSV-Pfad für bekannte Personen
CSV_PATH_KNOWN_PERSONS = os.path.expanduser(
    "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-person.csv"
)

def clean_string(val):
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    if isinstance(val, str):
        v = val.strip()
        if v.lower() == "nan":
            return ""
        return v
    return str(val).strip()


def load_known_persons_from_csv(
    path: str = CSV_PATH_KNOWN_PERSONS,
) -> List[Dict[str, str]]:
    def safe_strip(val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, float) and pd.isna(val):
            return ""
        if isinstance(val, str):
            v = val.strip()
            if v.lower() == "nan":
                return ""
            return v
        return str(val).strip()

    try:
        df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(
            columns={
                "Vorname": "forename",
                "Nachname": "familyname",
                "Alternativer Vorname": "alternate_name",
                "nodegoat ID": "nodegoat_id",
                "[Wohnort] Location Reference - Object ID": "home",
                "[Geburt] Date Start": "birth_date",
                "[Tod] Date Start": "death_date",
                "[Mitgliedschaft] in Organisation": "organisation",
                "Gender": "gender",
            },
            inplace=True,
        )

        for col in [
            "forename",
            "familyname",
            "alternate_name",
            "nodegoat_id",
            "home",
            "birth_date",
            "death_date",
            "organisation",
            "gender",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()

        persons = []
        for _, r in df.iterrows():
            raw_gender = safe_strip(r.get("gender", "")).lower()
            gender = {
                "männlich": "male",
                "weiblich": "female",
                "divers": "other"
            }.get(raw_gender, "")

            if raw_gender and not gender:
                print(f"[WARN] Unbekannter Gender-Wert: '{raw_gender}' in Zeile: {r.to_dict()}")

            persons.append(
                {
                    "forename": safe_strip(r["forename"]),
                    "familyname": safe_strip(r["familyname"]),
                    "alternate_name": safe_strip(r.get("alternate_name", "")),
                    "nodegoat_id": safe_strip(r.get("nodegoat_id", "")),
                    "home": safe_strip(r.get("home", "")),
                    "birth_date": safe_strip(r.get("birth_date", "")),
                    "death_date": safe_strip(r.get("death_date", "")),
                    "organisation": safe_strip(r.get("organisation", "")),
                    "gender": gender,
                }
            )
        return persons

    except Exception as e:
        print(f"[ERROR] Fehler beim Laden der Personendaten aus {path}: {e}")
        return []


KNOWN_PERSONS = load_known_persons_from_csv()

# Groundtruth-Namenslisten für Review-Erkennung vorbereiten
GROUNDTRUTH_SURNAMES = {
    p["familyname"].lower() for p in KNOWN_PERSONS if p["familyname"]
}
GROUNDTRUTH_FORENAMES = {
    p["forename"].lower() for p in KNOWN_PERSONS if p["forename"]
}


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
EXPANDED_NICKNAME_MAP: Dict[str, str] = {
    nick: canon for canon, nicks in NICKNAME_MAP.items() for nick in nicks
}
OCR_ERRORS: Dict[str, List[str]] = {
    "ü": ["u", "ue"],
    "ä": ["a", "ae"],
    "ö": ["o", "oe"],
    "ß": ["ss"],
}


# ============================================================================
#   NORMALISIERUNG
# ============================================================================

def normalize_name_string(name: str) -> (str, str):
    """
    Trennt Titel-Token am Anfang, normalisiert den Rest.
    Rückgabe: (bereinigter Name, erkannter Titel)
    """
    s = name.strip()

    # Regex dynamisch aus TITLE_TOKENS bauen
    title_pattern = re.compile(
        r"^(" + "|".join(re.escape(t) for t in TITLE_TOKENS) + r")\.?\s*",
        re.I
    )

    title = ""
    m = title_pattern.match(s)
    if m:
        title = m.group(1).capitalize()
        s = s[m.end():].strip()

    # Klammern löschen, Inhalt behalten
    s = re.sub(r"[()]", "", s)

    # Unicode-NFKD und Diakritika entfernen
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # alles außer Wort- und Leerzeichen löschen
    s = re.sub(r"[^\w\s]", "", s)

    # Nickname-Mapping
    s = EXPANDED_NICKNAME_MAP.get(s.lower(), s.lower())

    return s, title


def is_initial(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]\.?", s.strip()))


def normalize_name(name: str) -> Dict[str, str]:
    """
    Zerlegt einen vollständigen Namen in title, forename und familyname.
    Erkennt Titel aus TITLE_TOKENS am Anfang und entfernt sie.
    """
    if not name:
        return {"title": "", "forename": "", "familyname": ""}

    s = name.strip()

    # Regex dynamisch aus TITLE_TOKENS bauen
    pattern = r"(?i)^(" + "|".join(re.escape(t) for t in TITLE_TOKENS) + r")\.?\s+"

    m = re.match(pattern, s)
    title = m.group(1).capitalize() if m else ""
    if m:
        s = s[m.end():].strip()

    # Klammern löschen, Inhalt behalten
    s = re.sub(r"[()]", "", s)

    # Unicode-NFKD und Diakritika entfernen
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # alles außer Wort- und Leerzeichen löschen
    parts = re.sub(r"[^\w\s]", "", s).split()

    if not parts:
        return {"title": title, "forename": "", "familyname": ""}

    if len(parts) == 1:
        return {
            "title": title,
            "forename": parts[0].capitalize(),
            "familyname": "",
        }

    return {
        "title": title,
        "forename": parts[0].capitalize(),
        "familyname": " ".join(parts[1:]).capitalize(),
    }

# ============================================================================
#   Levenshtein-Fallback
# ============================================================================
def ocr_error_match(
    name: str, candidates: List[str]
) -> Tuple[Optional[str], int, float]:
    """
    Versucht, name gegen Kandidaten rein per Levenshtein zu matchen.
    """
    name_lower = name.lower().strip()
    best_match = None
    best_dist = 999999  # erstellt eine sehr hohe distanz zu Werten

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
def correct_swapped_name(forename: str, familyname: str) -> Tuple[str, str]:
    """Korrigiert vertauschte Namensteile heuristisch."""
    if not forename or not familyname:
        return forename, familyname

    fn_lower, ln_lower = forename.lower(), familyname.lower()

    fn_is_surname = fn_lower in KNOWN_SURNAMES
    ln_is_forename = ln_lower in KNOWN_FORENAMES

    if not fn_is_surname:
        _, dist_fn, _ = ocr_error_match(fn_lower, list(KNOWN_SURNAMES))
        fn_is_surname = dist_fn <= 1

    if not ln_is_forename:
        _, dist_ln, _ = ocr_error_match(ln_lower, list(KNOWN_FORENAMES))
        ln_is_forename = dist_ln <= 2

    fn_is_forename = fn_lower in KNOWN_FORENAMES
    ln_is_surname = ln_lower in KNOWN_SURNAMES

    if fn_is_surname and ln_is_forename and not (fn_is_forename and ln_is_surname):
        return familyname, forename

    return forename, familyname





# ----------------------------------------------------------------------------
# Fuzzy Matching
# ----------------------------------------------------------------------------

def fuzzy_match_name(
    name: str, candidates: List[str], threshold: int
) -> Tuple[Optional[str], int]:
    best, score = None, 0
    norm = normalize_name_string(name)
    for c in candidates:
        sc = int(fuzz.ratio(normalize_name_string(c), norm))  # 👈 Cast auf int
        if sc > score:
            best, score = c, sc
    return (best, score) if score >= threshold else (None, 0)


from typing import Union, Optional, Dict, Tuple, List, Any


def match_person(
    person: dict[str, Union[str, int, bool, None]],
    candidates: List[Dict[str, str]] = KNOWN_PERSONS,
) -> Tuple[Optional[Dict[str, Union[str, int, bool, None]]], int]:

    if (
        person.get("role_schema")
        and not person.get("familyname")
        and not person.get("forename")
    ):
        return {
            "forename": "",
            "familyname": "",
            "role": str(person.get("role", "")),
            "title": str(person.get("title", "")),
            "alternate_name": "",
            "nodegoat_id": "",
            "match_score": 30,
            "confidence": "partial",
            "needs_review": True,
            "gender": person.get("gender", "") or infer_gender_for_person(person, KNOWN_PERSONS),
            "review_reason": f"Nur Rolle ohne vollständigen Namen erkannt: {person.get('role', '')}",
        }, 30

    # 1) Roh-Strings holen und trimmen
    fn_raw = str(person.get("forename") or "").strip()
    ln_raw = str(person.get("familyname") or "").strip()
    fn = fn_raw.strip(string.punctuation)
    ln = ln_raw.strip(string.punctuation)
    role_raw = str(person.get("role") or "").strip()

    # ────────────────────────────────────────
    # 2) Inhalts-Filter (Pronomen, Rollen, Blacklist)
    # ────────────────────────────────────────
    # Pronomen
    if fn.lower() in PRONOUN_TOKENS:
        return None, 0

    # Rollen-Token ohne Nachname
    if fn.lower() in ROLE_TOKENS and not ln:
        return None, 0

    # Generischer Non-Person-Filter über alle Tokens
    tokens = [t.lower() for t in fn.split()]
    if any(t in NON_PERSON_TOKENS for t in tokens) and not ln:
        return None, 0
    # -------------------------
    #  UNIQUE-NAME HEURISTIK
    # -------------------------
    # Wenn nur Vorname da ist und dieser in der Groundtruth genau einmal vorkommt,
    # dann ist es definitiv diese Person.
    if fn and not ln:
        matches = [
            c for c in candidates
            if normalize_name_string(c["forename"]) == normalize_name_string(fn)
        ]
        if len(matches) == 1:
            result = dict(matches[0])
            result.update({
                "confidence": "single_name_in_GT",
                "needs_review": False,
                "match_score": 90,           
            })
            return result, result["match_score"]

    if ln and not fn:
        matches = [
            c for c in candidates
            if normalize_name_string(c["familyname"]) == normalize_name_string(ln)
        ]
        if len(matches) == 1:
            result = dict(matches[0])
            result.update({
                "confidence": "single_name_in_GT",
                "needs_review": False,
                "match_score": 90,
            })
            return result, result["match_score"]


    tokens = [w.strip(",:;.").lower() for w in fn.split()]

    context = str(person.get("content_transcription") or "")
    fn_in_unmatchable = fn.lower() in UNMATCHABLE_SINGLE_NAMES
    ln_in_unmatchable = ln.lower() in UNMATCHABLE_SINGLE_NAMES

    if (fn_in_unmatchable and not ln) or (ln_in_unmatchable and not fn):
        full = f"{fn} {ln}".strip()
        inv = f"{ln} {fn}".strip()
        if re.search(
            rf"\b{re.escape(full)}\b", context, flags=re.IGNORECASE
        ) or re.search(rf"\b{re.escape(inv)}\b", context, flags=re.IGNORECASE):
            print(
                f"[DEBUG] Kontext rettet '{fn}'/'{ln}' durch Fund von '{full}' oder '{inv}' im Text"
            )
        return None, 0

    if any(t in NON_PERSON_TOKENS for t in tokens) and not ln:
        reason = f"Dropping because token in blacklist: fn='{fn}', ln='{ln}'"
        print(f"[DEBUG] {reason}")
        return {
            "forename": fn,
            "familyname": ln,
            "role": role_raw,
            "gender": person.get("gender", "") or infer_gender_for_person(person, KNOWN_PERSONS),
            "title": str(person.get("title", "")),
            "alternate_name": "",
            "nodegoat_id": "",
            "match_score": 0,
            "confidence": "blacklist",
            "needs_review": True,
            "review_reason": reason,
            "raw_token": str(person.get("raw_token") or f"{fn} {ln}".strip()),
        }, 0

    if not ln and fn.lower() in ROLE_TOKENS:
        reason = (
            f"Dropping token as name (role detected): fn='{fn}', ln='{ln}'"
        )
        print(f"[DEBUG] {reason}")
        return {
            "forename": fn,
            "familyname": ln,
            "role": role_raw,
            "gender": person.get("gender", "") or infer_gender_for_person(person, KNOWN_PERSONS),
            "title": str(person.get("title", "")),
            "alternate_name": "",
            "nodegoat_id": "",
            "match_score": 0,
            "confidence": "blacklist",
            "needs_review": True,
            "review_reason": reason,
            "raw_token": str(person.get("raw_token") or fn),
        }, 0

    thr = get_matching_thresholds()
    norm_fn, norm_ln = normalize_name_string(fn), normalize_name_string(ln)

    if is_initial(fn):
        init = fn[0].upper()
        fam_norm = normalize_name_string(ln)
        for c in candidates:
            if (
                c["forename"]
                and c["forename"][0].upper() == init
                and normalize_name_string(c["familyname"]) == fam_norm
            ):
                return dict(c), 95
    # 3-CHAR-Vorname + exakter Nachname
    if fn and len(fn) >= 3 and ln:
        norm_ln = normalize_name_string(ln)
        fn_lower = fn.lower()
        for c in candidates:
            # Normal: Vorname-Teil prüfen, Nachname exakt
            if (
                c["forename"]
                and c["forename"][:len(fn)].lower() == fn_lower[:len(fn)]
                and normalize_name_string(c["familyname"]) == norm_ln
            ):
                return dict(c), 89

            # NEU: Reverse-Prüfung
            # Prüfe: lieferte dein fn vielleicht den Nachnamen?
            norm_fn = normalize_name_string(fn)
            ln_lower = ln.lower()
            # Neue REVERSE-Prüfung – robust:
            if (
                c["forename"] and c["familyname"] and
                normalize_name_string(c["forename"])[:4] == norm_ln[:4] and  # dein ln gegen c[forename]
                normalize_name_string(c["familyname"])[:4] == norm_fn[:4] and  # dein fn gegen c[familyname]
                # NEU: nur tauschen, wenn es nicht eh schon normal ist
                not (
                    normalize_name_string(c["forename"]) == norm_fn and
                    normalize_name_string(c["familyname"]) == norm_ln
                )
            ):
                swapped = dict(c)
                swapped["forename"], swapped["familyname"] = swapped["familyname"], swapped["forename"]
                return swapped, 88

    best, best_score = None, 0
    for c in candidates:
        fn_score = (
            100
            if is_initial(fn)
            and c["forename"]
            and fn[0].upper() == c["forename"][0].upper()
            else fuzzy_match_name(
                fn,
                [c["forename"], c.get("alternate_name", "")],
                thr["forename"],
            )[1]
        )
        ln_score = (
            100
            if is_initial(ln)
            and c["familyname"]
            and ln[0].upper() == c["familyname"][0].upper()
            else fuzzy_match_name(ln, [c["familyname"]], thr["familyname"])[1]
        )
        combo = 0.4 * fn_score + 0.6 * ln_score
        if combo > best_score:
            best_score, best = combo, c
        if norm_fn == normalize_name_string(
            c["familyname"]
        ) and norm_ln == normalize_name_string(c["forename"]):
            return dict(c), 100

    if best_score >= max(thr.values()):
        result = dict(best)

        result["gender"] = (
            result.get("gender", "")  # bevorzugt, falls im dict schon gesetzt (oft identisch mit best.get("gender", ""))
            or best.get("gender", "")  # falls aus irgendeinem Grund im dict leer
            or person.get("gender", "")  # falls es im Input schon gesetzt war (z. B. durch vorherige Extraktion)
            or infer_gender_for_person(result, KNOWN_PERSONS)  # Notlösung: heuristisch ableiten
        )
        return result, int(best_score)



    for c in candidates:
        if normalize_name_string(fn) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(ln.lower(), c["familyname"].lower()) <= 1:
                return dict(c), 90

    for c in candidates:
        if normalize_name_string(ln) == normalize_name_string(c["forename"]):
            if Levenshtein.distance(fn.lower(), c["familyname"].lower()) <= 1:
                return dict(c), 90

    if not fn and ln:
        for c in candidates:
            ln_score = fuzzy_match_name(
                ln, [c["familyname"]], thr["familyname"]
            )[1]
            if ln_score >= thr["familyname"]:
                return dict(c), ln_score

    if not fn and not ln and role_raw:
        for c in candidates:
            if role_raw.lower() in (
                c.get("role", "").lower(),
                c.get("alternate_name", "").lower(),
            ):
                return dict(c), 80

    if fn.lower() in NON_PERSON_TOKENS and ln:
        for c in candidates:
            if normalize_name_string(ln) == normalize_name_string(
                c["familyname"]
            ):
                return dict(c), 85
    if ln.lower() in NON_PERSON_TOKENS and fn:
        for c in candidates:
            if normalize_name_string(fn) == normalize_name_string(
                c["familyname"]
            ):
                return dict(c), 85

    if not fn and ln:
        fams = [c["familyname"] for c in candidates if c["familyname"]]
        matched, dist, _ = ocr_error_match(ln, fams)
        if matched and dist <= 2:
            c = next(x for x in candidates if x["familyname"] == matched)
            return dict(c), 85
    if not person.get("gender") and person.get("title") in FEMALE_TITLE_TOKENS:
        person["gender"] = "female"
    elif not person.get("gender") and person.get("title") in MALE_TITLE_TOKENS:
        person["gender"] = "male"
    # SPEZIAL: Wenn Nachname bekannt & mehrere Matches & gender gegeben
    if ln and person.get("gender"):
        print (f"[DEBUG] WHUUUU WIR SIND IM SPEZIALFALL NACHNAME + GENDER:")
        same_ln = [
            c for c in candidates
            if normalize_name_string(c["familyname"]) == normalize_name_string(ln)
        ]
        # Finde nur die, die gender matchen
        same_ln_gender = [
            c for c in same_ln
            if str(c.get("gender", "")).lower() == str(person["gender"]).lower()
        ]
        if len(same_ln_gender) == 1:
            print(f"[DEBUG] Gender-gestütztes Disambiguieren: {ln} + {person['gender']}")
            result: Dict[str, Union[str, int, bool, None]] = dict(same_ln_gender[0])
            result.update({
                "confidence": "gender_ln_match",
                "needs_review": False,
                "match_score": 95
            })
            result["title"] = person.get("title") or result.get("title", "")
            result["gender"] = person.get("gender") or result.get("gender", "")
            return result, 95

    if any([fn, ln, role_raw]):
        review_reason_parts = []
        if not fn:
            review_reason_parts.append("missing_forename")
        if not ln:
            review_reason_parts.append("missing_familyname")
        if not role_raw:
            review_reason_parts.append("missing_role")

        review_reason = "; ".join(review_reason_parts)

        keep_name = (fn and appears_in_groundtruth(fn)) or (
            ln and appears_in_groundtruth(ln)
        )
        if keep_name or role_raw:
            print(
                f"[NEEDS_REVIEW] Aufnahme trotz fehlender ID: {fn} {ln} ({role_raw})"
            )
            return {
                "forename": fn,
                "familyname": ln,
                "title": str(person.get("title", "")),
                "gender": person.get("gender", "") or infer_gender_for_person(person, KNOWN_PERSONS),
                "alternate_name": "",
                "nodegoat_id": "",
                "role": role_raw,
                "role_schema": str(map_role_to_schema_entry(role_raw) or ""),
                "match_score": None,
                "confidence": "partial-no-id",
                "needs_review": True,
                "review_reason": review_reason,
            }, 0


    return None, 0


# ----------------------------------------------------------------------------
# Extract Person Data mit Rolleninfos
# ----------------------------------------------------------------------------
# Groundtruth-Listen
KNOWN_SURNAMES = {
    p["familyname"].lower() for p in KNOWN_PERSONS if p.get("familyname")
}
KNOWN_FORENAMES = {
    p["forename"].lower() for p in KNOWN_PERSONS if p.get("forename")
}
print(f"[DEBUG] KNOWN_FORENAMES count: {len(KNOWN_FORENAMES)}")


def extract_person_data(row: Dict[str, Any]) -> Dict[str, str]:
    from Module.person_matcher import extract_role_from_raw_name
    from Module.Assigned_Roles_Module import (
        normalize_and_match_role,
        map_role_to_schema_entry,
    )

    raw = row.get("name", "").strip()
    m = re.match(
        r"^(Herrn?|Frau|Fräulein|Dr\.?|Prof\.?)\s+(.+)$",
        raw,
        flags=re.IGNORECASE,
    )
    title = m.group(1).capitalize() if m else ""
    if m:
        raw = m.group(2).strip()

    clean, roles = extract_role_from_raw_name(raw)
    parts = clean.split()
    forename = parts[0] if len(parts) >= 1 else ""
    familyname = parts[-1] if len(parts) >= 2 else ""
    forename, familyname = correct_swapped_name(forename, familyname)

    role_raw = row.get("role", "").strip()
    if roles:
        role_raw = roles[0]

    normalized_role = normalize_and_match_role(role_raw) if role_raw else ""
    role_schema = (
        map_role_to_schema_entry(normalized_role) if normalized_role else ""
    )

    raw_gender = clean_string(row.get("gender", "")).lower()
    gender = {
        "männlich": "male",
        "weiblich": "female",
        "divers": "other"
    }.get(raw_gender, "")

    return {
        "forename": forename,
        "familyname": familyname,
        "alternate_name": clean_string(row.get("alternate_name", "")),
        "title": title,
        "nodegoat_id": clean_string(row.get("nodegoat_id", "")),
        "home": clean_string(row.get("home", "")),
        "birth_date": clean_string(row.get("birth_date", "")),
        "death_date": clean_string(row.get("death_date", "")),
        "organisation": clean_string(row.get("organisation", "")),
        "stripped_role": roles,
        "role": normalized_role,
        "role_schema": role_schema,
        "gender": gender,
        "associated_organisation": clean_string(row.get("associated_organisation", "")),
        "confidence": "",
        "match_score": 0,
        "needs_review": True if not row.get("nodegoat_id") else False,
        "review_reason": "low_score or no nodegoat_id",
    }


def get_review_reason_for_person(p: Dict[str, str]) -> str:
    reasons = []
    if not p.get("forename"):
        reasons.append("missing_forename")
    if not p.get("familyname"):
        reasons.append("missing_familyname")
    if not p.get("role"):
        reasons.append("missing_role")

    return "; ".join(reasons)

def infer_gender_for_person(
    person: Union[Person, Dict[str, Any]],
    known_persons: List[Dict[str, str]] = KNOWN_PERSONS
) -> str:
    """
    Ermittelt das Geschlecht einer Person auf Basis von Titel (aus Transkribus)
    oder Groundtruth-Matching (CSV). Gibt 'Männlich', 'Weiblich' oder '' zurück.
    """
    # 1. Sicherstellen, dass wir mit einem Dict arbeiten
    if isinstance(person, Person):
        print(f"[DEBUG] Konvertiere Person-Objekt zu Dict: {person}")
        person = person.to_dict()

    fn = person.get("forename", "").strip().lower()
    ln = person.get("familyname", "").strip().rstrip(".").lower()


    # 2. Titelbasiertes Matching (aus Transkribus)
    title = (person.get("title") or "").strip().lower()
    title_map = {
        #männliche Titel
        "herr": "male", "herrn": "male", "witwer": "male",
        "sänger": "male", "sangesbruder": "male", "sängerbruder": "male", 
        "bruder": "male", "kamerad": "male", "genosse": "male",
        #weibliche Titel
        "frau": "female", "fräulein": "female", "witwe": "female",
        "sängerin": "female", "kameradin": "female", "genossin": "female",
    }
    if title in title_map:
        gender = title_map[title]
        print(f"[MATCH] Titelbasiertes Gender-Match: {gender}")
        return gender

    # 3. Groundtruth-Matching über CSV (Feld: "Gender")
    for known in known_persons:
        fn_known = known.get("forename", "").strip().lower()
        ln_known = known.get("familyname", "").strip().lower()
        if fn_known == fn and ln_known == ln:
            gender_raw = known.get("gender", "").strip().lower()
            if gender_raw in {"männlich", "male"}:
                
                return "male"
            elif gender_raw in {"weiblich", "female"}:
                
                return "female"

            else:
                print(f"[WARN] CSV-Gender ungültig oder leer: '{gender_raw}'")

    return ""


# Neue Fallback-Regel: Einwort-Personen, die als Rolle erkannt werden
def detect_and_convert_role_only_entries(
    person: Dict[str, Any],
) -> Dict[str, Any]:
    word = person.get("forename", "").strip().lower()
    role_schema = map_role_to_schema_entry(word)

    if role_schema and not person.get("familyname"):
        return {
            "forename": "",
            "familyname": "",
            "role": role_schema,
            "role_schema": role_schema,
            "needs_review": True,
            "review_reason": f"Nur Rolle ohne vollständigen Namen erkannt: {word}",
        }
    print(f"[DEBUG] Dummy-Rolle erkannt und übernommen: {role_schema}")

    return person

import re

# benutze deine bereits definierten Patterns
from Module.letter_metadata_matcher import (
    _RECIPIENT_RE,
    INDIRECT_RECIPIENT_PATTERNS,
    GREETING_PATTERNS as CLOSING_PATTERNS,
    ROLE_PATTERNS,
)

def extract_metadata_names(text: str) -> list[str]:
    from Module.letter_metadata_matcher import _CLOSING_RE, INDIRECT_RECIPIENT_PATTERNS, direct_patterns
    names = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        # 1) Direkte Adressierung (An…, Herrn…, Frau…)
        m = _RECIPIENT_RE.match(line)
        if m:
            # „An Frau Maria Müller“ → „Maria Müller“
            cleaned = re.sub(r'^(?:An\s+|Herrn?\s+|Frau\s+|Liebe[rn]?\s+)', '',
                             line, flags=re.IGNORECASE).strip()
            names.append(cleaned)
            continue

        # 2) Indirekte „zu Händen von …“
        for pat in INDIRECT_RECIPIENT_PATTERNS:
            mi = re.search(pat, line, flags=re.IGNORECASE)
            if mi:
                # Gruppe 1=Vorname, 2=Nachname
                names.append(f"{mi.group(1)} {mi.group(2)}")
                break

        # 3) Direkte Anrede‐Patterns („Lieber Otto“, „Liebe Maria“)
        for pat in direct_patterns:
            md = re.search(pat, line)
            if md:
                names.append(md.group(1))
                break

        # 4) Closing‐Formeln („Mit freundlichen Grüßen“, „Hochachtungsvoll“)
        #    und davor ggf. den Namen
        if _CLOSING_RE.search(line):
            # suche nach „…,\n<Name>“ oder zeilenvorher nach Großwortfolge
            if i > 0:
                prev = lines[i-1].strip()
                # z.B. „Otto Müller,“
                m2 = re.match(r"^([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)*)[,\.]?$", prev)
                if m2:
                    names.append(m2.group(1))
            continue

    return names

def merge_title_tokens(raw_persons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = []
    skip_next = False
    for i, p in enumerate(raw_persons):
        if skip_next:
            skip_next = False
            continue
        if p.get("name", "").strip().lower() in TITLE_TOKENS:
            if i + 1 < len(raw_persons):
                next_p = raw_persons[i + 1]
                # immer Titel setzen, egal ob next_p bereits forename/familyname hat
                next_p["title"] = p["name"].strip().capitalize()
                merged.append(next_p)
                skip_next = True
            # sonst droppen
        else:
            merged.append(p)
    return merged



# ----------------------------------------------------------------------------
# Split und Enrichment
# ----------------------------------------------------------------------------
def split_and_enrich_persons(
    raw_persons: List[Union[str, Dict[str, str]]],
    content_transcription: str,
    document_id: Optional[str] = None,
    candidates: Optional[List[Dict[str, str]]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    from Module.Assigned_Roles_Module import POSSIBLE_ROLES


    # print(
    #     f"[DEBUG] Split_and_enrich in Person_matcher.py erhält folgende raw_persons: {raw_persons}"
    # )

    # print("===========>", raw_persons)
    # for p in raw_persons:
    #     for key, value in p.items():
    #         if (isinstance(key, str) and "Adolf" in key) or (isinstance(value, str) and "Adolf" in value):
    #             print("**********************>", key, value)

    # --- 1. Zeilen zusammenführen ---
    merged_raw_persons = []
    meta_names = extract_metadata_names(content_transcription)
    for nm in meta_names:
        merged_raw_persons.append({"name": nm})
    i = 0
    while i < len(raw_persons):
        current = raw_persons[i]

        # Bereits vollständiges Personenobjekt → direkt übernehmen
        if isinstance(current, dict) and (
            current.get("forename")
            and current.get("familyname")
            and current.get("nodegoat_id")
        ):
            # CLEANUP!
            current["forename"] = clean_string(current.get("forename"))
            current["familyname"] = clean_string(current.get("familyname"))
            current["alternate_name"] = clean_string(current.get("alternate_name"))
            # und ggf. weitere Felder wie "title", "role" etc., falls betroffen
            merged_raw_persons.append(current)
            i += 1
            continue
 

        current_name = (
            current.get("name", "")
            if isinstance(current, dict)
            else str(current)
        ).strip()

        if not current_name:
            i += 1
            continue

        if i + 1 < len(raw_persons) and isinstance(raw_persons[i + 1], dict):
            next_name = raw_persons[i + 1].get("name", "").strip()
            if not re.search(r"[,.;:]$", current_name) and next_name:
                combined = f"{current_name} {next_name}"
                if not any(r.lower() in combined.lower() for r in ROLE_TOKENS):
                    print(
                        f"[DEBUG] Merge von Mehrzeiliger Person: '{current_name}' + '{next_name}' → '{combined}'"
                    )
                    merged_raw_persons.append({"name": combined})
                    i += 2
                    continue

        merged_raw_persons.append({"name": current_name})
        i += 1

    print("[DEBUG] Merged raw_persons vor Filter:", merged_raw_persons)

    processed_raw_persons = []
    skip_next = False
    processed_raw_persons = []
    skip_next = False

    for idx, p in enumerate(merged_raw_persons):
        if skip_next:
            skip_next = False
            continue

        if isinstance(p, dict):
            # ✅ Wenn schon forename/familyname/nodegoat_id: unverändert übernehmen
            if p.get("forename") and p.get("familyname") and p.get("nodegoat_id"):
                processed_raw_persons.append(p)
                continue

            elif "name" in p:
                name_str = p["name"].strip()
                tokens = [t.strip(",.;:").lower() for t in name_str.split()]

                title_token = ""
                gender = ""

                if tokens:
                    first = tokens[0]
                    if first in FEMALE_TITLE_TOKENS:
                        title_token = first
                        gender = "female"
                    elif first in MALE_TITLE_TOKENS:
                        title_token = first
                        gender = "male"

                    rest_tokens = tokens[1:] if title_token else tokens
                    if (
                        len(rest_tokens) >= 2
                        and gender == "female"
                        and any(
                            c.get("forename", "").lower() == rest_tokens[0]
                            and c.get("gender") == "male"
                            for c in (candidates or KNOWN_PERSONS)
                        )
                    ):
                        # Ignoriere männlichen Vornamen
                        rest_tokens = rest_tokens[1:]
                    rest_name = " ".join(rest_tokens).strip().title()

                    # ✅ Immer als forename/familyname speichern
                    new_person = {}

                    new_person = {}

                    # Titel immer setzen, wenn vorhanden
                    if title_token:
                        new_person["title"] = title_token.capitalize()
                        new_person["gender"] = gender

                    # Rest-Name setzen (nur wenn sinnvoll)
                    if rest_name:
                        new_person["forename"] = ""
                        new_person["familyname"] = rest_name
                    else:
                        # Falls gar nichts übrig: setze den Titel als Vorname, Family leer
                        if title_token:
                            new_person["forename"] = ""
                            new_person["familyname"] = ""
                        else:
                            new_person["forename"] = ""
                            new_person["familyname"] = name_str
                    new_person["name"] = name_str



                    processed_raw_persons.append(new_person)

                else:
                    continue

            else:
                name_str = str(p).strip()
                tokens = [t.lower() for t in name_str.split()]
                if all(t in NON_PERSON_TOKENS for t in tokens):
                    continue
                if name_str:
                    processed_raw_persons.append({
                        "forename": "",
                        "familyname": name_str,
                        "title": "",
                        "gender": ""
                    })
        else:
            name_str = str(p).strip()
            tokens = [t.lower() for t in name_str.split()]
            if all(t in NON_PERSON_TOKENS for t in tokens):
                continue
            if name_str:
                processed_raw_persons.append({
                    "forename": "",
                    "familyname": name_str,
                    "title": "",
                    "gender": ""
                })

    # ✅ Final
    raw_persons = processed_raw_persons

    print(f"[DEBUG] raw persons nach Zeile 1116: {raw_persons}")

    print("[DEBUG] processed_raw_persons:", processed_raw_persons)

    # --- 2. Kontextbasierte Rollenzuweisung ---
    lines = content_transcription.splitlines()
    for p in raw_persons:
        token = (
            p.get("name")
            or f"{p.get('forename', '')} {p.get('familyname', '')}".strip()
        )
        for idx, line in enumerate(lines):
            if token.strip() in line.strip():
                role, org = infer_role_and_organisation(
                    idx, lines, token, "", ""
                )
                if role:
                    p["role"] = role
                    p["associated_organisation"] = org
                break

    # --- 3. Matching & Key-Bildung ---
    seen, matched, unmatched = set(), [], []
    cand_list = candidates or KNOWN_PERSONS

    for p in raw_persons:
        print(f"[DEBUG] Verarbeite Person: {p} in raw persons {raw_persons}")
        raw_token = (
            p.get("name")
            or f"{p.get('forename', '')} {p.get('familyname', '')}".strip()
        )
        clean_name, roles = extract_role_from_raw_name(raw_token)
        p["name"] = clean_name

        # ✅ NEU: Wenn die Person bereits vollständige Daten enthält, direkt übernehmen
        if p.get("forename") and p.get("familyname") and p.get("nodegoat_id"):
            print(
                f"[BYPASS] Person bereits gematcht (aus extract_person_from_custom): {p}"
            )
            print(f" Diese Person ist {p}")
            p["match_score"] = p.get("match_score", 100)
            p["confidence"] = p.get("confidence", "nodegoat")
            p["needs_review"] = False
            p["raw_token"] = raw_token
            matched.append(p)
            continue

        if roles and not p.get("role"):
            p["role"] = roles[0]

        person = extract_person_data({
        "name": raw_token,
        "title": p.get("title", ""),
        "gender": p.get("gender", "")
        })
        
        if (
            person.get("forename", "").strip()
            and person.get("familyname", "").strip()
            and person.get("nodegoat_id", "").strip()
        ):
            person["match_score"] = 100
            person["confidence"] = "nodegoat"
            person["needs_review"] = False
            person["raw_token"] = raw_token
            matched.append(person)
            print(
                f"[SAFE-APPEND] Direktübernahme: {person['forename']} {person['familyname']} (ID={person['nodegoat_id']})"
            )
            continue

        fn_norm = normalize_name_string(person.get("forename", ""))
        ln_norm = normalize_name_string(person.get("familyname", ""))
        key = person.get("nodegoat_id") or f"{fn_norm} {ln_norm}".strip()

        print(f"[DEBUG KEY] raw_token={raw_token!r}, key={key!r}")

        if not key:
            print(
                f"[DEBUG WARNING] Kein Key – Person wird trotzdem übernommen mit needs_review"
            )
        elif key in seen:
            print(f"[DEBUG DUPLICATE] Person mit key='{key}' übersprungen.")
            continue
        else:
            seen.add(key)

        person["content_transcription"] = content_transcription
        match, score = match_person(person, candidates=cand_list)

        if match and score > 0:
            matched.append(
                {
                    "raw_token": raw_token,
                    "forename": match["forename"],
                    "familyname": match["familyname"],
                    "nodegoat_id": match["nodegoat_id"],
                    "match_score": score,
                    "confidence": "fuzzy",
                }
            )
        else:
            # Standard Markierung
            person["match_score"] = 0
            person["confidence"] = ""
            person["review_reason"] = get_review_reason_for_person(person)
            person["raw_token"] = raw_token
            person["needs_review"] = True

            fn = person.get("forename", "").strip()
            ln = person.get("familyname", "").strip()

            # NEU: Wenn Vorname nur 3–4 Zeichen → tausche Vor- und Nachname
            if ((fn and len(fn) <= 4) or (ln and len(ln) <= 4)) and fn and ln:
                swapped = {
                    "forename": ln,
                    "familyname": fn,
                    "content_transcription": content_transcription,
                }
                swapped_match, swapped_score = match_person(swapped, candidates=cand_list)

                if swapped_match and swapped_score > 0:
                    print(f"[SWAP-MATCH] Tausch erfolgreich: {fn}/{ln} → {swapped_match}")
                    matched.append({
                        "raw_token": raw_token,
                        "forename": swapped_match["forename"],
                        "familyname": swapped_match["familyname"],
                        "nodegoat_id": swapped_match["nodegoat_id"],
                        "match_score": swapped_score,
                        "confidence": "swapped",
                    })
                    continue

            # Wenn kein besserer Match → normal aufnehmen
            unmatched.append(person)


    # --- 4. Sonderfälle ---
    additional = []
    role_only_entries = []

    for person in unmatched:
        name = person["raw_token"].strip()
        if is_initial(name):
            person["match_score"] = 50
            person["confidence"] = "initial"
            additional.append(person)
            continue

        if name in POSSIBLE_ROLES:
            role_entry = {
                "forename": "",
                "familyname": "",
                "alternate_name": "",
                "title": "",
                "role": name,
                "role_schema": map_role_to_schema_entry(name),
                "associated_place": "",
                "associated_organisation": person.get(
                    "associated_organisation", ""
                ),
                "nodegoat_id": "",
                "match_score": 40,
                "confidence": "role_only",
                "raw_token": name,
            }
            role_only_entries.append(role_entry)
            continue

        parts = [p.strip() for p in name.split(",")]
        if (
            len(parts) == 2
            and normalize_name(parts[0])
            and parts[1] in POSSIBLE_ROLES
        ):
            person["forename"] = ""
            person["familyname"] = parts[0]
            person["role"] = parts[1]
            person["role_schema"] = map_role_to_schema_entry(parts[1])
            person["match_score"] = 45
            person["confidence"] = "name-role"
            additional.append(person)
            continue

        if normalize_name(name):
            person["match_score"] = 30
            person["confidence"] = "single-name"
            additional.append(person)
            continue

    matched.extend(additional)
    matched_ids = {id(p) for p in additional}
    unmatched = [p for p in unmatched if id(p) not in matched_ids]

    if role_only_entries:
        unmatched.extend(role_only_entries)


    return matched, unmatched


def infer_role_and_organisation(
    index: int,
    lines: List[str],
    raw_token: str,
    forename: str = "",
    familyname: str = "",
) -> Tuple[str, str]:
    from Module.letter_metadata_matcher import _CLOSING_RE, _RECIPIENT_RE, GREETING_PATTERNS, RECIPIENT_PATTERNS, INDIRECT_RECIPIENT_PATTERNS
    """
    Analysiert die nachfolgende Zeile auf Rollen + Organisationen.
    Beispiel:
      R Weiss
      Ortsverbandsleiter des V.D.A.
    → Rolle: Ortsverbandsleiter, Organisation: V.D.A.
    """
    if index + 1 >= len(lines):
        return "", ""

    next_line = lines[index + 1].strip()

    # 1) Klassisches Muster: "<Rolle> des/der <Organisation>"
    m = re.match(
        r"^(?P<role>[\w\s\-ÄÖÜäöüß]+?)\s+(?:des|der|vom|von)\s+(?P<org>.+)$",
        next_line,
    )
    if m:
        return m.group("role").strip(), m.group("org").strip()

    # 2) Wenn der volle Name in der aktuellen Zeile vorkommt
    full_name = f"{forename} {familyname}".strip().lower()
    if full_name and full_name in lines[index].lower():
        parts = next_line.split()
        if parts and any(k in next_line.lower() for k in ("leiter", "führer")):
            return parts[0].strip(), " ".join(parts[1:]).strip()

    # 3) Beliebiges Rollen-Pattern
    m_role = ROLE_PATTERNS.search(next_line)
    if m_role:
        role = m_role.group(0).strip()
        # org nicht gefunden → leer lassen oder mit map_role_to_schema_entry normalisieren
        return role, ""

    return "", ""


# ----------------------------------------------------------------------------
# Deduplication
# ----------------------------------------------------------------------------
def deduplicate_persons(
    persons: List[Dict[str, str]],
    known_candidates: Optional[List[Dict[str, str]]] = None,
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
                best_entry = max(
                    entries_with_id,
                    key=lambda p: float(p.get("match_score", 0) or 0),
                )
                merged = best_entry.copy()

                # Anzahl Erwähnungen aufsummieren
                merged["mentioned_count"] = sum(
                    int(p.get("mentioned_count", 1) or 1) for p in group
                )

                # recipient_score übernehmen (höchsten Wert aus der Gruppe)
                merged["recipient_score"] = max(
                    float(p.get("recipient_score", 0) or 0) for p in group
                )

                # Rollen kombinieren
                all_roles = {
                    p.get("role", "").strip() for p in group if p.get("role")
                }
                if all_roles:
                    merged["role"] = "; ".join(sorted(all_roles))
                    # Update role_schema based on the combined roles
                    from Module.Assigned_Roles_Module import (
                        map_role_to_schema_entry,
                    )

                    merged["role_schema"] = (
                        map_role_to_schema_entry(sorted(all_roles)[0])
                        if sorted(all_roles)
                        else ""
                    )

                def flatten_organisation(org):
                    """Entfernt tiefe Verschachtelung aus Organisationseinträgen und gibt flache Struktur zurück."""
                    result = {}
                    visited = set()
                    current = org

                    while isinstance(current, dict):
                        if id(current) in visited:
                            break  # zyklische Struktur vermeiden
                        visited.add(id(current))
                        for key in ["name", "nodegoat_id"]:
                            if key in current and not isinstance(
                                current[key], dict
                            ):
                                result[key] = current[key]
                        # gehe tiefer, falls 'name' oder 'nodegoat_id' ein dict ist
                        if "name" in current and isinstance(
                            current["name"], dict
                        ):
                            current = current["name"]
                        elif "nodegoat_id" in current and isinstance(
                            current["nodegoat_id"], dict
                        ):
                            current = current["nodegoat_id"]
                        else:
                            break

                    return result if result else org

                # Felder ergänzen, wenn leer
                for field in [
                    "title",
                    "alternate_name",
                    "associated_place",
                    "associated_organisation",
                ]:
                    if not merged.get(field):
                        for p in group:
                            value = p.get(field)
                            if value:
                                if field == "associated_organisation":
                                    value = flatten_organisation(value)
                                merged[field] = value
                                break

                # match_score & confidence ergänzen, falls noch leer
                if not merged.get("match_score"):
                    merged["match_score"] = max(
                        float(p.get("match_score", 0) or 0) for p in group
                    )
                if not merged.get("confidence"):
                    merged["confidence"] = next(
                        (
                            p.get("confidence")
                            for p in group
                            if p.get("confidence")
                        ),
                        "",
                    )

                unique.append(merged)

        else:
            # Sicherheitsprüfung: Gruppe enthält doch nodegoat_id → Vorrang geben!
            recovered_with_id = [p for p in group if p.get("nodegoat_id")]
            if recovered_with_id:
                best_entry = max(
                    recovered_with_id,
                    key=lambda p: float(p.get("match_score", 0) or 0),
                )
                best_entry["mentioned_count"] = sum(
                    int(p.get("mentioned_count", 1) or 1) for p in group
                )
                unique.append(best_entry)
                print(
                    f"[FIXED] Person mit ID wurde fälschlich in fallback-Block einsortiert: {best_entry.get('forename', '')} {best_entry.get('familyname', '')}"
                )
                continue
            best_entry = sorted(
                group, key=lambda p: p.get("match_score", 0), reverse=True
            )[0]
            match, score = match_person(best_entry, candidates)

            if match and score >= 90:
                enriched = match.copy()
                enriched["match_score"] = score
                enriched["confidence"] = "fuzzy"
                enriched["mentioned_count"] = sum(
                    int(p.get("mentioned_count", 1) or 1) for p in group
                )

                enriched["associated_place"] = best_entry.get(
                    "associated_place", ""
                )
                enriched["associated_organisation"] = best_entry.get(
                    "associated_organisation", ""
                )

                all_roles = list(
                    {p.get("role", "").strip() for p in group if p.get("role")}
                )
                if all_roles:
                    enriched["role"] = "; ".join(all_roles)

                unique.append(enriched)
                print(
                    f"[DEBUG] Behalte Person (fuzzy): {enriched.get('forename', '')} {enriched.get('familyname', '')}, Score: {score}"
                )
            else:
                best_entry["match_score"] = score
                best_entry["confidence"] = "none"
                best_entry["mentioned_count"] = sum(
                    int(p.get("mentioned_count", 1) or 1) for p in group
                )

                all_roles = list(
                    {p.get("role", "").strip() for p in group if p.get("role")}
                )
                if all_roles:
                    best_entry["role"] = "; ".join(all_roles)

                unique.append(best_entry)
                print(
                    f"[DEBUG] Behalte Person (no match): {best_entry.get('forename', '')} {best_entry.get('familyname', '')}, Score: {score}"
                )

    return unique


def assess_llm_entry_score(
    forename: str, familyname: str, role: str
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
    person: Dict[str, str], candidates: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    cand_list = candidates or KNOWN_PERSONS
    match, score = match_person(person, cand_list)
    return {
        "matched_forename": match.get("forename") if match else None,
        "matched_familyname": match.get("familyname") if match else None,
        "matched_title": match.get("title") if match else None,
        "match_id": match.get("nodegoat_id") if match else None,
        "score": score,
    }


def deduplicate_and_group_persons(
    persons: List[Union[Dict[str, Any], Person]],
) -> List[Person]:
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
        combined_roles = "; ".join(
            sorted(set(p.get("role", "") for p in group if p.get("role")))
        )
        best["role"] = combined_roles
        best["mentioned_count"] = sum(int(p.get("mentioned_count", 1)) for p in group)
        best["recipient_score"] = max(int(p.get("recipient_score", 0) or 0) for p in group)
        final.append(Person.from_dict(best))

    # 2) Manuelle Gruppierung nach Namensähnlichkeit
    for entry in unmatched:
        entry = ensure_dict(entry)
        fn = normalize(entry.get("forename", ""))
        ln = normalize(entry.get("familyname", ""))
        matched = False

        for target in final:
            tfn = normalize(getattr(target, "forename", ""))
            tln = normalize(getattr(target, "familyname", ""))

            if entry.get("match_score", 0) == 0 and not entry.get("nodegoat_id"):
                entry["needs_review"] = True
                entry["review_reason"] = "No nodegoat_id, match_score 0"


            # Wenn entweder Vorname oder Nachname übereinstimmt
            if (fn and fn == tfn) or (ln and ln == tln):
                print(f"[MERGE] {fn or ln} → {tfn} {tln}")

                # Mention count mergen
                entry_count = int(entry.get("mentioned_count", 1))
                target.mentioned_count += entry_count

                # recipient_score mergen
                target.recipient_score = max(
                    getattr(target, "recipient_score", 0),
                    entry.get("recipient_score", 0),
                )

                # Rollen mergen
                if entry.get("role"):
                    existing_roles = set(filter(None, (target.role or "").split("; ")))
                    existing_roles.add(entry["role"])
                    target.role = "; ".join(sorted(existing_roles))

                # role_schema ergänzen, falls Ziel leer
                if entry.get("role_schema") and not getattr(target, "role_schema", ""):
                    target.role_schema = entry.get("role_schema")

                matched = True
                break

        if not matched:
            entry["mentioned_count"] = int(entry.get("mentioned_count", 1))
            entry["recipient_score"] = int(entry.get("recipient_score", 0) or 0)
            print(f"[NEU] Person ohne Merge: {entry.get('forename')} {entry.get('familyname')}")
            final.append(Person.from_dict(entry))

    # 3) recipient_score über nodegoat_id sichern
    recipient_score_lookup = {
        ensure_dict(p)["nodegoat_id"]: ensure_dict(p).get("recipient_score", 0)
        for p in persons
        if ensure_dict(p).get("recipient_score", 0) > 0 and ensure_dict(p).get("nodegoat_id")
    }

    for person in final:
        nid = person.nodegoat_id
        if nid in recipient_score_lookup:
            person.recipient_score = max(person.recipient_score or 0, recipient_score_lookup[nid])

    print("\n[DEBUG] Finale erwähnte Personen nach Deduplikation:")
    for p in final:
        print(f" → {p.forename} {p.familyname}, Rolle: {p.role}, ID: {p.nodegoat_id}, Score: {p.match_score}, Count: {p.mentioned_count}")

    return final


def count_mentions_in_transcript_contextual(
    persons: List[Person], transcript: str
) -> List[Person]:
    lines = [line.strip().lower() for line in transcript.splitlines()]
    num_lines = len(lines)

    for p in persons:
        fn = (p.forename or "").lower().strip()
        ln = (p.familyname or "").lower().strip()
        role = (p.role or "").lower().strip()

        count = 0
        matched_line_indices = set()

        for i, line in enumerate(lines):
            # Suche nach vollständigem Namen, Nachnamen oder Vornamen
            name_hit = (
                (fn and ln and f"{fn} {ln}" in line)
                or (ln and ln in line)
                or (fn and fn in line)
            )
            if not name_hit:
                continue

            # Suche ±1 Zeile nach Rolle
            role_found = False
            for j in [i - 1, i, i + 1]:
                if 0 <= j < num_lines:
                    if role and role in lines[j]:
                        role_found = True
                        break

            # Zähle nur einmal pro Block (nicht mehrfach dieselbe Name-Rolle-Kombi)
            if i not in matched_line_indices:
                count += 1
                matched_line_indices.update([i - 1, i, i + 1])

        p.mentioned_count = max(count, 1)

    return persons
