import re
import pandas as pd
import xml.etree.ElementTree as ET  
from typing import List, Dict, Any
from pathlib import Path
from Module.document_schemas import Person

# --- Dynamische Ermittlung des Projekt-Root (sucht up bis Data/Nodegoat_Export) ---
THIS_FILE = Path(__file__).resolve()
BASE_DIR = THIS_FILE.parent
while BASE_DIR != BASE_DIR.parent and not (BASE_DIR / "Data" / "Nodegoat_Export").exists():
    BASE_DIR = BASE_DIR.parent

# Pfade zu CSV-Dateien
CSV_ROLE_PATH = BASE_DIR / "Data" / "Nodegoat_Export" / "export-roles.csv"
CSV_PERSON_PATH = BASE_DIR / "Data" / "Nodegoat_Export" / "export-person.csv"

# === Rollen-CSV laden und Mapping aufbauen ===
_df = pd.read_csv(CSV_ROLE_PATH, sep=";", dtype=str).fillna("") 
ROLE_MAPPINGS_DE: Dict[str, str] = {}

for _, row in _df.iterrows():
    base = row.get("Rollenname", "").strip()
    if not base:
        continue

    variants = {base}

    # Falls maskuline Endung auf „er“, versuche Deklinationsformen
    if re.search(r"er$", base):
        variants |= {
            base + "n",
            base + "en",
            base + "ern",
        }

    alt = row.get("Alternativer Rollenname", "").strip()
    for alt_name in alt.split(','):
        if alt_name.strip():
            variants.add(alt_name.strip())

    for variant in variants:
        ROLE_MAPPINGS_DE[variant.lower()] = base

# Basis-Vokabular: alle Keys
POSSIBLE_ROLES: List[str] = list(ROLE_MAPPINGS_DE.keys())
# Bekanntes Rollen-Verzeichnis für externe Nutzung
KNOWN_ROLE_LIST: List[str] = POSSIBLE_ROLES
print(f"{len(ROLE_MAPPINGS_DE)} Rollenvarianten geladen.")

# === Regex-Patterns ===
ROLE_AFTER_NAME_RE = re.compile(
    rf"(?P<name>[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)\s*,?\s*"
    rf"(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?",
    re.IGNORECASE | re.UNICODE
)
ROLE_BEFORE_NAME_RE = re.compile(
    rf"(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})\s+(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*"
    rf"(?: (?P<organisation>[A-ZÄÖÜ][\w\s\-]+)\s+ )?"
    rf"(?P<name>[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)",
    re.IGNORECASE | re.UNICODE
)
STANDALONE_ROLE_RE = re.compile(
    rf"^\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})"
    rf"(?:\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*(?P<organisation>[A-ZÄÖÜ][\w\s\-]+))?\s*$",
    re.IGNORECASE | re.UNICODE | re.MULTILINE
)
NAME_RE = re.compile(
    r"^[A-ZÄÖÜ][a-zäöüß]+(?:[- ][A-Za-zäöüÄÖÜß]+)*$"
)

def normalize_and_match_role(text: str) -> str:
    text_clean = text.lower().strip()

    # 1. Direkter Treffer
    if text_clean in ROLE_MAPPINGS_DE:
        return ROLE_MAPPINGS_DE[text_clean]

    # 2. Versuche über häufige Flexionsendungen zur Grundform
    suffixes = ["en", "ern", "em", "e", "es", "n", "r", "s", "ns", "nt", "ner", "ners"]
    for suffix in suffixes:
        if text_clean.endswith(suffix) and len(text_clean) > len(suffix) + 2:
            base = text_clean[: -len(suffix)]
            candidate = base + "er"
            if candidate in ROLE_MAPPINGS_DE:
                return ROLE_MAPPINGS_DE[candidate]

    # 3. Startswith-Fallback
    for role in ROLE_MAPPINGS_DE:
        if role.startswith(text_clean):
            return ROLE_MAPPINGS_DE[role]

    return ""


# === Laden der Bekanntpersonen ===
def load_known_persons() -> List[Dict[str, Any]]:
    df = pd.read_csv(CSV_PERSON_PATH, sep=";", dtype=str, keep_default_na=False)
    df.rename(columns={"Vorname": "forename", "Name": "familyname", "Alternativname": "alternate_name", "Titel": "title"}, inplace=True)
    persons = df[["forename", "familyname", "alternate_name", "title"]].to_dict(orient="records")
    for p in persons:
        p.update({"role": "", "role_schema": "", "associated_organisation": "", "associated_place": "", "nodegoat_id": "", "match_score": 0, "confidence": ""})
    return persons

# === Mapping-Funktion ===
def map_role_to_schema_entry(role_string: str) -> str:
    return ROLE_MAPPINGS_DE.get(role_string.strip().lower(), "None")

# === Extraktion Inline-Rollen zu bekannten Personen ===

def assign_roles_to_known_persons(persons: List[Dict[str, Any]], full_text: str) -> List[Person]:
    # 1) Inline-Matches nach ROLE_AFTER_NAME_RE und ROLE_BEFORE_NAME_RE
    for regex in (ROLE_AFTER_NAME_RE, ROLE_BEFORE_NAME_RE):
        for match in regex.finditer(full_text):
            name      = match.group("name") or ""
            raw_role  = match.group("role")
            org       = (match.group("organisation") or "").strip()

            normalized_role = normalize_and_match_role(raw_role)
            if not normalized_role:
                continue
            normalized_role = normalize_role_form(normalized_role)

            parts = name.strip().split()
            if len(parts) < 2:
                continue
            fn_cand, ln_cand = " ".join(parts[:-1]), parts[-1]

            for p in persons:
                if (p.get("familyname") == ln_cand and fn_cand in p.get("forename", "")):
                    p["role"]               = normalized_role
                    p["role_schema"]        = map_role_to_schema_entry(normalized_role)
                    p["associated_organisation"] = org
                    print(f"[DEBUG] role_schema = {p['role_schema']!r}")

    # 2) Finales Normalisieren aller gefundenen Rollen
    for p in persons:
        if p.get("role"):
            norm = normalize_and_match_role(p["role"])
            if norm:
                p["role"]        = norm
                p["role_schema"] = map_role_to_schema_entry(norm)
                print(f"[DEBUG] final role_schema = {p['role_schema']!r}")
    for p in persons:
        fn = p.get("forename","").strip()
        ln = p.get("familyname","").strip()
        name_token = fn or ln
        if not p.get("role") and name_token.lower() in ROLE_MAPPINGS_DE:
            normalized = normalize_and_match_role(name_token)
            if normalized:
                p["role"]        = normalized
                p["role_schema"] = map_role_to_schema_entry(normalized)
                p["forename"]    = ""
                p["familyname"]  = ""
                print(f"[DEBUG] Name-als-Rolle gefixt: '{name_token}' → role='{normalized}'")


    # 3) Name-als-Rolle-Fallback auf Dictionary-Ebene
    cleaned_dicts: List[Dict[str, Any]] = []
    for p in persons:
        fn = p.get("forename", "").strip()
        ln = p.get("familyname", "").strip()
        name_token = fn or ln  # entweder Vorname oder Nachname
        # Wenn keine Rolle gesetzt ist, aber der Name-Token in ROLE_MAPPINGS_DE auftaucht:
        if not p.get("role") and name_token.lower() in ROLE_MAPPINGS_DE:
            normalized = normalize_and_match_role(name_token)
            p["role"]        = normalized
            p["role_schema"] = map_role_to_schema_entry(normalized)
            print(f"[DEBUG] Name-als-Rolle gefixt: '{name_token}' → role='{normalized}'")
            # und die Namensfelder löschen
            p["forename"]    = ""
            p["familyname"]  = ""
        cleaned_dicts.append(p)

    # 4) Umwandlung in Person-Objekte
    result: List[Person] = []
    for p in cleaned_dicts:
        try:
            person = Person.from_dict(p)
            print(f"[DEBUG] person.role_schema = {person.role_schema!r}")
            result.append(person)
        except Exception as e:
            print(f"[WARN] Ungültiges Personen-Dict in Rollen-Modul: {p} – {e}")

    return result

def extract_role_in_token(token: str) -> List[Dict[str, Any]]:
    """
    Erkennt Konstruktionen wie 'Ehrenvorsitzender Burger' oder 'Schriftführer Huber' und trennt sie.
    Gibt eine Liste von Personendictionaries zurück, wenn Rolle und Name erkennbar sind.
    """
    results = []
    token = token.strip()
    parts = token.split()

    # zu kurz oder kein Leerzeichen → überspringen
    if len(parts) < 2:
        return results

    # Kandidaten: alle n-1 Tokens als möglicher Rollenbegriff
    for i in range(1, len(parts)):
        role_candidate = " ".join(parts[:i])
        name_candidate = " ".join(parts[i:])

        normalized_role = normalize_and_match_role(role_candidate)
        if not normalized_role:
            continue

        # Name normalisieren
        fn, ln = "", ""
        name_parts = name_candidate.split()
        if len(name_parts) == 2:
            fn, ln = name_parts
        else:
            ln = name_parts[-1] if name_parts else ""

        if ln:
            # Ensure role is consistently normalized
            final_role = normalize_and_match_role(normalized_role) or normalized_role

            role_schema = map_role_to_schema_entry(final_role)
            print(f"[DEBUG] extract_role_in_token: role_schema = {role_schema!r}")
            results.append({
                "forename": fn,
                "familyname": ln,
                "alternate_name": "",
                "title": "",
                "role": final_role,
                "role_schema": role_schema,
                "associated_place": "",
                "associated_organisation": "",
                "nodegoat_id": "",
                "match_score": 99,
                "confidence": "llm-matched",
                "raw_token": token
            })

    return results



# === Extraktion reiner Rollenzeilen ===
def extract_standalone_roles(persons: List[Dict[str, Any]], full_text: str) -> List[Dict[str, Any]]:
    new_entries: List[Dict[str, Any]] = []
    lines = full_text.splitlines()
    for idx, line in enumerate(lines):
        segment = line.split(',', 1)[-1].strip()
        m = STANDALONE_ROLE_RE.match(segment)
        # Zusätzliche Prüfung: ganze Zeile wie "Der Vereinsführer"
        simple_match = re.match(r"^\s*(Der|Die)?\s*(?P<role>[A-ZÄÖÜa-zäöüß\-]+)(e|er|in)?\s*$", segment)
        if simple_match:
            raw_role = simple_match.group("role")
            normalized_role = normalize_and_match_role(raw_role)

            # Unterscheide zwischen bekannten und unbekannten Rollen
            if normalized_role:
                # Bekannte Rolle aus der Liste
                new_entries.append({
                    "forename": "",
                    "familyname": "",
                    "alternate_name": "",
                    "title": "",
                    "role": normalized_role,
                    "role_schema": map_role_to_schema_entry(normalized_role),
                    "associated_place": "",
                    "associated_organisation": "",
                    "nodegoat_id": "",
                    "match_score": 5,
                    "confidence": "role_only"
                })
            elif len(raw_role) > 3:  # Mindestlänge für potenzielle Rollen
                # Unbekannte potenzielle Rolle - für Unmatched-Output
                new_entries.append({
                    "forename": "",
                    "familyname": "",
                    "alternate_name": "",
                    "title": "",
                    "role": raw_role,
                    "role_schema": "unknown",
                    "associated_place": "",
                    "associated_organisation": "",
                    "nodegoat_id": "",
                    "match_score": 2,
                    "confidence": "unknown_role"
                })
            continue
        if not m:
            continue
        role = m.group("role")
        org  = (m.group("organisation") or "").strip()

        # Vorherige Zeile zur Namensableitung
        if idx > 0:
            prev = lines[idx - 1].strip()
            name_line = re.sub(r'.*(Herrn?|Frau)\s+', '', prev, flags=re.IGNORECASE).strip()
            parts = name_line.split()
            if len(parts) >= 2:
                fn_cand, ln_cand = " ".join(parts[:-1]), parts[-1]
            else:
                fn_cand, ln_cand = "", ""
        else:
            fn_cand, ln_cand = "", ""

        # Rolle normalisieren
        normalized_role = normalize_and_match_role(role)
        if not normalized_role:
            continue

        # Bereits zugeordnet?
        exists = any(
            p.get("forename") == fn_cand and
            p.get("familyname") == ln_cand and
            p.get("role") == normalized_role
            for p in persons
        )
        if exists:
            continue

        # Fall 1: Gültiger Name
        if NAME_RE.match(fn_cand) and NAME_RE.match(ln_cand):
            match_score = 60
        elif ln_cand and NAME_RE.match(ln_cand):
            match_score = 40
        # Fall 2: Keine erkennbare Person → reine Rolle
        else:
            fn_cand, ln_cand = "", ""
            match_score = 5

        new_entries.append({
            "forename": fn_cand,
            "familyname": ln_cand,
            "alternate_name": "",
            "title": "",
            "role": normalized_role,
            "role_schema": map_role_to_schema_entry(normalized_role),
            "associated_place": "",
            "associated_organisation": org,
            "nodegoat_id": "",
            "match_score": match_score,
            "confidence": "role_only"
        })

    return new_entries



def normalize_role_form(role_str: str) -> str:
    """
    Normalisiert deklinierte Rollenformen wie 'vorsitzenden' oder 'vorsitzende' zu 'vorsitzender'.
    """
    role_str = role_str.lower()

    # Nur wenn Wort auf typische Endungen endet
    for suffix in ["en", "n", "e", "ern"]:
        if role_str.endswith(suffix) and len(role_str) > len(suffix) + 2:
            candidate = role_str[: -len(suffix)]
            possible_base = candidate + "er"
            if possible_base in ROLE_MAPPINGS_DE:
                return possible_base
    return role_str


# === Fallback: nur Rollen ohne Personen ===
def extract_mentioned_roles(full_text: str) -> List[Dict[str, Any]]:
    seen = set()
    entries: List[Dict[str, Any]] = []
    for m in STANDALONE_ROLE_RE.finditer(full_text):
        role = m.group("role")
        org  = (m.group("organisation") or "").strip()
        key = (role.lower(), org.lower())
        if key in seen:
            continue
        seen.add(key)
        entries.append({
            "forename": "",
            "familyname": "",
            "alternate_name": "",
            "title": "",
            "role": role,
            "role_schema": map_role_to_schema_entry(role),
            "associated_place": "",
            "associated_organisation": org,
            "nodegoat_id": "",
            "match_score": 0,
            "confidence": "mentioned_role"
        })
    return entries

# === Haupt-Prozess ===
def process_text(full_text: str) -> Dict[str, Any]:
    """
    Extrahiert Rollen-Personen und – als Fallback – Author/Recipient
    aus dem Text und liefert ein Dict, das sowohl persons_with_roles
    als auch author/recipient enthalten kann.
    """
    # 1) Rollen-Personen extrahieren
    persons = load_known_persons()
    persons = assign_roles_to_known_persons(persons, full_text)
    standalone = extract_standalone_roles(persons, full_text)
    persons.extend(standalone)
    matched = [p for p in persons if p.get("role")]

    result: Dict[str, Any] = {}
    if matched:
        result["persons_with_roles"] = matched
    else:
        # wenn keine Rollen-Personen gefunden, wenigstens reine Rollen erwähnen
        result["mentioned_persons"] = extract_mentioned_roles(full_text)

    # 2) Fallback für Author & Recipient aus dem Brieftext
    from .letter_metadata_matcher import (
        extract_authors_raw, extract_recipients_raw, letter_match_and_enrich
    )

    # Author extrahieren & anreichern
    raw_author = extract_authors_raw(full_text)
    enriched_author = letter_match_and_enrich(raw_author, full_text)
    if enriched_author.get("forename"):
        result["author"] = enriched_author

    # Recipient extrahieren & anreichern
    raw_rec = extract_recipients_raw(full_text)
    enriched_rec = letter_match_and_enrich(raw_rec, full_text)
    if enriched_rec.get("forename"):
        result["recipient"] = enriched_rec

    return result

# Beispielaufruf
if __name__ == "__main__":
    full_text = """... dein Transkript ..."""
    print(process_text(full_text))
