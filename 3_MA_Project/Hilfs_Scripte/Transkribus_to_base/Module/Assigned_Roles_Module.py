import re
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Tuple
from pathlib import Path
from Module.document_schemas import Person
from Module.organization_matcher import (
    KNOWN_ORGS,
    extract_organization,
    match_organization,
)

# --- Dynamische Ermittlung des Projekt-Root (sucht up bis Data/Nodegoat_Export) ---
THIS_FILE = Path(__file__).resolve()
BASE_DIR = THIS_FILE.parent
while (
    BASE_DIR != BASE_DIR.parent
    and not (BASE_DIR / "Data" / "Nodegoat_Export").exists()
):
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
            base + "ers",
            base + "s",
            base + "es",
        }

    alt = row.get("Alternativer Rollenname", "").strip()
    for alt_name in alt.split(","):
        if alt_name.strip():
            variants.add(alt_name.strip())

    for variant in variants:
        ROLE_MAPPINGS_DE[variant.lower()] = base

# Basis-Vokabular: alle Keys
POSSIBLE_ROLES: List[str] = list(ROLE_MAPPINGS_DE.keys())
# Bekanntes Rollen-Verzeichnis für externe Nutzung
KNOWN_ROLE_LIST: List[str] = POSSIBLE_ROLES

# === Regex-Patterns ===
ROLE_AFTER_NAME_RE = re.compile(
    rf"(?P<name>[A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?)\s*,?\s*"
    rf"(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?",
    re.IGNORECASE | re.UNICODE,
)
ROLE_BEFORE_NAME_RE = re.compile(
    rf"""
    (?P<role>
        (?:
            {"|".join(map(re.escape, POSSIBLE_ROLES))}
        )
        (?:s|es|en|em|ern|e|er|n)?  # Genitiv- und Pluralformen zulassen
    )
    \s+
    (?:
        des|der|dem|den|vom|zum|zur|im|in|am|an|beim
    )?
    \s*
    (?:
        (?P<organisation>
            (?:[A-ZÄÖÜ][\w\-]+(?:\s+(?:der|des|dem|den|vom)?\s*)?){{1,4}}
        )
        \s+
    )?
    (?P<name>
        [A-ZÄÖÜ][\w\-]+
        (?:\s+[A-ZÄÖÜ][\w\-]+)*
    )
    """,
    re.IGNORECASE | re.UNICODE | re.VERBOSE,
)

STANDALONE_ROLE_RE = re.compile(
    rf"^\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})"
    rf"(?:\s*(?:des|der|dem|den|vom|zum|zur|im|in|am|an|beim)?\s*(?P<organisation>[A-ZÄÖÜ][\w\s\-]+))?\s*$",
    re.IGNORECASE | re.UNICODE | re.MULTILINE,
)
NAME_RE = re.compile(r"^[A-ZÄÖÜ][a-zäöüß]+(?:[- ][A-Za-zäöüÄÖÜß]+)*$")


# ----------------------------------------------------------------------------
# Helfer zur Vereinheitlichung von Organisationseinträgen
# ----------------------------------------------------------------------------
def resolve_nested_value(d: dict, key: str) -> str:
    val = d.get(key, "")
    while isinstance(val, dict):
        val = val.get(key, "")
    return val if isinstance(val, str) else ""


def is_flat_organisation(org: dict) -> bool:
    """
    Prüft, ob der Organisationseintrag bereits flach vorliegt (alle relevanten Felder sind Strings).
    """
    if not isinstance(org, dict):
        return False
    if not isinstance(org.get("name"), str):
        return False
    if not isinstance(org.get("nodegoat_id", ""), str):
        return False
    if not isinstance(org.get("wikidata_id", ""), str):
        return False
    if "place" in org and isinstance(org["place"], dict):
        if not isinstance(org["place"].get("name", ""), str):
            return False
        if not isinstance(org["place"].get("nodegoat_id", ""), str):
            return False
    return True


def flatten_organisation_entry(org: dict) -> dict:
    if not isinstance(org, dict):
        print(
            f"[WARN] Ungültiger org-Typ für flatten_organisation_entry: {org!r}"
        )
        return {}

    # Wenn bereits flach → nichts tun
    if (
        isinstance(org.get("name"), str)
        and isinstance(org.get("nodegoat_id", ""), str)
        and isinstance(org.get("wikidata_id", ""), str)
        and (
            "place" not in org
            or (
                isinstance(org["place"], dict)
                and isinstance(org["place"].get("name", ""), str)
                and isinstance(org["place"].get("nodegoat_id", ""), str)
            )
        )
    ):
        return org

    # Flache Struktur erzeugen
    flat = {
        "name": resolve_nested_value(org, "name"),
        "nodegoat_id": resolve_nested_value(org, "nodegoat_id"),
        "wikidata_id": resolve_nested_value(org, "wikidata_id"),
    }

    if "place" in org and isinstance(org["place"], dict):
        flat["place"] = {
            "name": resolve_nested_value(org["place"], "name"),
            "nodegoat_id": resolve_nested_value(org["place"], "nodegoat_id"),
        }

    print(f"[DEBUG] Flattened associated_organisation: {flat}")
    return flat


# Felder, die Du in associated_organisation haben willst
ORG_KEYS = ("name", "nodegoat_id")
PLACE_KEYS = ("name", "nodegoat_id")


def normalize_and_match_role(text: str) -> str:
    text_clean = text.lower().strip()

    # 1. Direkter Treffer
    if text_clean in ROLE_MAPPINGS_DE:
        return ROLE_MAPPINGS_DE[text_clean]

    # 2. Genitiv-Erkennung (z. B. „Führers“ → „Führer“)
    if text_clean.endswith("s") and len(text_clean) > 4:
        base = text_clean[:-1]
        if base in ROLE_MAPPINGS_DE:
            print(f"[DEBUG] Genitiv erkannt: {text_clean} → {base}")
            return ROLE_MAPPINGS_DE[base]

    # 3. Maskuline Flexionsendungen zurückführen (z. B. „vorsitzenden“ → „vorsitzender“)
    suffixes = ["en", "ern", "em", "e", "n", "er", "es", "nt", "ner", "ners"]
    for suffix in suffixes:
        if text_clean.endswith(suffix) and len(text_clean) > len(suffix) + 2:
            base = text_clean[: -len(suffix)] + "er"
            if base in ROLE_MAPPINGS_DE:
                print(
                    f"[DEBUG] Maskuline Flexion erkannt: {text_clean} → {base}"
                )
                return ROLE_MAPPINGS_DE[base]

    # 4. Feminine Rollenformen (z. B. „Führerin“ → „Führer“)
    feminine_suffixes = ["in", "innen", "e"]
    for suffix in feminine_suffixes:
        if text_clean.endswith(suffix) and len(text_clean) > len(suffix) + 2:
            base = text_clean[: -len(suffix)] + "er"
            if base in ROLE_MAPPINGS_DE:
                print(f"[DEBUG] Feminine Rolle erkannt: {text_clean} → {base}")
                return ROLE_MAPPINGS_DE[base]

    # 5. Fallback: startswith
    for role in ROLE_MAPPINGS_DE:
        if role.startswith(text_clean):
            print(f"[DEBUG] Fallback startswith: {text_clean} → {role}")
            return ROLE_MAPPINGS_DE[role]

    # 6. Kein Treffer
    return ""


# === Laden der Bekanntpersonen ===
def load_known_persons() -> List[Dict[str, Any]]:
    df = pd.read_csv(
        CSV_PERSON_PATH, sep=";", dtype=str, keep_default_na=False
    )
    df.rename(
        columns={
            "Vorname": "forename",
            "Name": "familyname",
            "Alternativname": "alternate_name",
            "Titel": "title",
        },
        inplace=True,
    )
    persons = df[
        ["forename", "familyname", "alternate_name", "title"]
    ].to_dict(orient="records")
    for p in persons:
        p.update(
            {
                "role": "",
                "role_schema": "",
                "associated_organisation": {},
                "associated_place": "",
                "nodegoat_id": "",
                "match_score": 0,
                "confidence": "",
            }
        )
    print(
        f"[DEBUG] load_known_persons in Assign_ROles_module hat diese bekannte Personen: {persons}"
    )
    return persons


# === Mapping-Funktion ===
def map_role_to_schema_entry(role_string: str) -> str:
    return ROLE_MAPPINGS_DE.get(role_string.strip().lower(), "")


# === Extraktion Inline-Rollen zu bekannten Personen ===

def find_line_index_for_person(p: Dict[str, Any], lines: List[str]) -> int:
    """
    Findet die Zeile, in der eine Person (nach Namen) vorkommt.
    Nimmt den ersten Match im Text.
    """
    search_name = f"{p.get('forename', '')} {p.get('familyname', '')}".strip()
    if not search_name:
        return -1  # Kein Name -> keine Suche

    for idx, line in enumerate(lines):
        if search_name in line:
            return idx
    return -1

def search_roles_nearby(idx: int, lines: List[str]) -> List[str]:
    candidate_roles = []
    for offset in [-1, 1]:
        n = idx + offset
        if 0 <= n < len(lines):
            line = lines[n].lower()
            m = ROLE_BEFORE_NAME_RE.search(line)
            if m:
                candidate_roles.append(m.group(0))
    return candidate_roles





def assign_roles_to_known_persons(
    persons: List[Dict[str, Any]], full_text: str
) -> List[Person]:
    result: List[Person] = []
    def is_safely_matched(p: Dict[str, Any]) -> bool:
        return bool(p.get("nodegoat_id")) or p.get("match_score", 0) >= 90

    # Frühprüfung – aber nur für schwache Matches
    for p in persons:
        fn = p.get("forename", "").strip().lower()
        ln = p.get("familyname", "").strip().lower()

        if not is_safely_matched(p):
            if fn in ROLE_MAPPINGS_DE and not p.get("role"):
                print(
                    f"[DEBUG] '{fn}' wird als Rolle interpretiert, aber Person war kein sicherer Match."
                )
                role = normalize_and_match_role(fn)
                p["role"] = role
                p["role_schema"] = map_role_to_schema_entry(role)
                p["confidence"] = "single-name"
                p["needs_review"] = True
                p["review_reason"] = (
                    "role_without_person (mixed Role and Name)"
                )
                p["match_score"] = 30
                p["mentioned_count"] = 1
                continue

            if ln in ROLE_MAPPINGS_DE and not p.get("role"):
                print(
                    f"[FIX] '{ln}' ist keine Person, sondern Rolle – wird verschoben."
                )
                role = normalize_and_match_role(ln)
                p["role"] = role
                p["role_schema"] = map_role_to_schema_entry(role)
                p["confidence"] = "single-name"
                p["needs_review"] = True
                p["review_reason"] = (
                    "role_without_person (mixed Role and Name)"
                )
                p["match_score"] = 30
                p["mentioned_count"] = 1
                continue
            else:
                print(
                    f"[DEBUG-SAFE] Person '{p.get('forename', '')} {p.get('familyname', '')}' wird geschützt (ID oder Score ≥90)."
                )

    # 1) Inline-Matches nach ROLE_AFTER_NAME_RE und ROLE_BEFORE_NAME_RE
    for regex in (ROLE_AFTER_NAME_RE, ROLE_BEFORE_NAME_RE):
        for match in regex.finditer(full_text):
            name = match.group("name") or ""
            raw_role = match.group("role")
            org = (match.group("organisation") or "").strip()

            normalized_role = normalize_and_match_role(raw_role)
            if not normalized_role:
                continue
            normalized_role = normalize_role_form(normalized_role)

            parts = name.strip().split()
            if len(parts) < 2:
                continue
            fn_cand, ln_cand = " ".join(parts[:-1]), parts[-1]

            for p in persons:
                if p.get("familyname") == ln_cand and fn_cand in p.get(
                    "forename", ""
                ):
                    p["role"] = normalized_role
                    p["role_schema"] = map_role_to_schema_entry(
                        normalized_role
                    )

                    if org:
                        org_candidate = " ".join(org.split()[:3])

                        org_raw = re.sub(
                            r"\s+", " ", org_candidate.replace("\n", " ")
                        ).strip()

                        org_clean = extract_organization(org_raw)

                        if org_clean:
                            # 1. Erst normaler Fuzzy-Match
                            best_match, score = match_organization(
                                {"name": org_clean}, KNOWN_ORGS, threshold=80
                            )

                            # 2. Falls das fehlschlägt → manuelle Substring-Suche als Fallback
                            if not best_match:
                                for org_entry in KNOWN_ORGS:
                                    if (
                                        org_entry["name"].lower()
                                        in org_clean.lower()
                                    ):
                                        best_match = org_entry
                                        score = 81
                                        break

                            best_match_counter = 0
                            if best_match:
                                best_match_counter += 1
                                print(
                                    f"[DEBUG]  best_match #{best_match_counter}: {best_match}"
                                )
                                # Stelle sicher, dass alle Felder Strings sind
                                assoc_org = {
                                    "name": str(best_match.get("name", "")),
                                    "nodegoat_id": str(
                                        best_match.get("nodegoat_id", "")
                                    ),
                                    "wikidata_id": str(
                                        best_match.get("wikidata_id", "")
                                    ),
                                }

                                place_info = best_match.get("place", {})
                                if isinstance(place_info, dict):
                                    print(f"instance is {isinstance}")
                                    assoc_org["place"] = {
                                        "name": place_info.get("name", ""),
                                        "nodegoat_id": place_info.get(
                                            "nodegoat_id", ""
                                        ),
                                    }
                                    print(f"assoc_org is {assoc_org}")

                                p["associated_organisation"] = {
                                    "name": str(best_match.get("name", "")),
                                    "nodegoat_id": str(
                                        best_match.get("nodegoat_id", "")
                                    ),
                                    "wikidata_id": str(
                                        best_match.get("wikidata_id", "")
                                    ),
                                }

                        else:
                            print(
                                f"[DEBUG] extract_organization() lieferte None für '{org_raw}'"
                            )

                    print(f"[DEBUG] role_schema = {p['role_schema']!r}")

    # 2) Finales Normalisieren aller gefundenen Rollen
    for p in persons:
        if p.get("role"):
            norm = normalize_and_match_role(p["role"])
            if norm:
                p["role"] = norm
                p["role_schema"] = map_role_to_schema_entry(norm)
                print(f"[DEBUG] final role_schema = {p['role_schema']!r}")
    for p in persons:
        fn = p.get("forename", "").strip()
        ln = p.get("familyname", "").strip()
        name_token = fn or ln
        if not p.get("role") and name_token.lower() in ROLE_MAPPINGS_DE:
            normalized = normalize_and_match_role(name_token)
            if normalized:
                p["role"] = normalized
                p["role_schema"] = map_role_to_schema_entry(normalized)
                p["forename"] = ""
                p["familyname"] = ""
                print(
                    f"[DEBUG] Name-als-Rolle gefixt: '{name_token}' → role='{normalized}'"
                )

    # 3) Name-als-Rolle-Fallback auf Dictionary-Ebene
    cleaned_dicts: List[Dict[str, Any]] = []
    for p in persons:
        fn = p.get("forename", "").strip()
        ln = p.get("familyname", "").strip()
        name_token = fn or ln  # entweder Vorname oder Nachname
        # Wenn keine Rolle gesetzt ist, aber der Name-Token in ROLE_MAPPINGS_DE auftaucht:
        if not p.get("role") and name_token.lower() in ROLE_MAPPINGS_DE:
            normalized = normalize_and_match_role(name_token)
            p["role"] = normalized
            p["role_schema"] = map_role_to_schema_entry(normalized)
            print(
                f"[DEBUG] Name-als-Rolle gefixt: '{name_token}' → role='{normalized}'"
            )
            # und die Namensfelder löschen
            p["forename"] = ""
            p["familyname"] = ""
        cleaned_dicts.append(p)

    # 5) Füge explizit Dummy-Personen für Rollen-Tokens ein, wenn keine passende Person da ist
    for p in persons:
        fn = p.get("forename", "").strip().lower()
        ln = p.get("familyname", "").strip().lower()

        if fn in ROLE_MAPPINGS_DE and not p.get("role"):
            role = normalize_and_match_role(fn)
            role_schema = map_role_to_schema_entry(role)
            dummy = {
                "forename": "",
                "familyname": "",
                "alternate_name": "",
                "title": "",
                "role": role,
                "role_schema": role_schema,
                "associated_place": "",
                "associated_organisation": "",
                "nodegoat_id": "",
                "match_score": 30,
                "recipient_score": 0,
                "mentioned_count": 1,
                "confidence": "single-name",
                "needs_review": True,
                "review_reason": "role_without_person",
            }
            print(f"[ADDED] Dummy-Person mit Rolle '{role}' angelegt.")
            try:
                result.append(Person.from_dict(dummy))
            except Exception as e:
                print(f"[WARN] Ungültiger Dummy: {dummy} – {e}")

        elif ln in ROLE_MAPPINGS_DE and not p.get("role"):
            role = normalize_and_match_role(ln)
            role_schema = map_role_to_schema_entry(role)
            dummy = {
                "forename": "",
                "familyname": "",
                "alternate_name": "",
                "title": "",
                "role": role,
                "role_schema": role_schema,
                "associated_place": "",
                "associated_organisation": "",
                "nodegoat_id": "",
                "match_score": 30,
                "recipient_score": 0,
                "mentioned_count": 1,
                "confidence": "single-name",
                "needs_review": True,
                "review_reason": "role_without_person",
            }
            print(f"[ADDED] Dummy-Person mit Rolle '{role}' angelegt.")
            try:
                result.append(Person.from_dict(dummy))
            except Exception as e:
                print(f"[WARN] Ungültiger Dummy: {dummy} – {e}")

    lines = full_text.split("\n")
    for p in cleaned_dicts:
        if not p.get("role_schema"):
            idx = find_line_index_for_person(p, lines)
            if idx == -1:
                continue  # Kein Treffer im Text gefunden

            nearby_roles = search_roles_nearby(idx, lines)
            if nearby_roles:
                normalized = normalize_and_match_role(nearby_roles[0])
                p["role"] = normalized
                p["role_schema"] = map_role_to_schema_entry(normalized)
                print(
                    f"[CTX] Kontextrolle gesetzt für {p.get('forename')} {p.get('familyname')}: '{nearby_roles[0]}' → '{normalized}'"
                )


    
    for p in cleaned_dicts:
        try:
            org = p.get("associated_organisation", {})
            if isinstance(org, dict):
                if p.get("role") and not p.get("role_schema"):
                    p["role_schema"] = map_role_to_schema_entry(p["role"])
                    print(f"[FIX] role_schema gefüllt: {p['role']} → {p['role_schema']}")
                if not isinstance(org.get("name"), str):
                    p["associated_organisation"] = flatten_organisation_entry(
                        org
                    )

            person = Person.from_dict(p)
            print(f"[DEBUG] person.role_schema = {person.role_schema!r}")
            result.append(person)
        except Exception as e:
            print(
                f"[WARN] Ungültiges Personen-Dict in Rollen-Modul: {p} – {e}"
            )

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
            final_role = (
                normalize_and_match_role(normalized_role) or normalized_role
            )

            role_schema = map_role_to_schema_entry(final_role)
            print(
                f"[DEBUG] extract_role_in_token: role_schema = {role_schema!r}"
            )
            results.append(
                {
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
                    "raw_token": token,
                }
            )

    return results


# === Extraktion reiner Rollenzeilen ===
def extract_standalone_roles(
    persons: List[Dict[str, Any]], full_text: str
) -> List[Dict[str, Any]]:
    new_entries: List[Dict[str, Any]] = []
    lines = full_text.splitlines()
    for idx, line in enumerate(lines):
        segment = line.split(",", 1)[-1].strip()
        m = STANDALONE_ROLE_RE.match(segment)
        # Zusätzliche Prüfung: ganze Zeile wie "Der Vereinsführer"
        simple_match = re.match(
            r"^\s*(Der|Die)?\s*(?P<role>[A-ZÄÖÜa-zäöüß\-]+)(e|er|in)?\s*$",
            segment,
        )
        if simple_match:
            raw_role = simple_match.group("role")
            normalized_role = normalize_and_match_role(raw_role)

            # Unterscheide zwischen bekannten und unbekannten Rollen
            if normalized_role:
                # Bekannte Rolle aus der Liste
                new_entries.append(
                    {
                        "forename": "",
                        "familyname": "",
                        "alternate_name": "",
                        "title": "",
                        "role": normalized_role,
                        "role_schema": map_role_to_schema_entry(
                            normalized_role
                        ),
                        "associated_place": "",
                        "associated_organisation": "",
                        "nodegoat_id": "",
                        "match_score": 5,
                        "confidence": "role_only",
                    }
                )
            elif len(raw_role) > 3:  # Mindestlänge für potenzielle Rollen
                # Unbekannte potenzielle Rolle - für Unmatched-Output
                new_entries.append(
                    {
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
                        "confidence": "unknown_role",
                    }
                )
            continue
        if not m:
            continue
        role = m.group("role")
        org = (m.group("organisation") or "").strip()

        # Vorherige Zeile zur Namensableitung
        if idx > 0:
            prev = lines[idx - 1].strip()
            name_line = re.sub(
                r".*(Herrn?|Frau)\s+", "", prev, flags=re.IGNORECASE
            ).strip()
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
            p.get("forename") == fn_cand
            and p.get("familyname") == ln_cand
            and p.get("role") == normalized_role
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

        new_entries.append(
            {
                "forename": fn_cand,
                "familyname": ln_cand,
                "alternate_name": "",
                "title": "",
                "role": normalized_role,
                "role_schema": map_role_to_schema_entry(normalized_role),
                "associated_place": "",
                "associated_organisation": (
                    match_organization({"name": org}, KNOWN_ORGS)[0]["data"]
                    if org
                    else {}
                ),
                "nodegoat_id": "",
                "match_score": match_score,
                "confidence": "role_only",
            }
        )

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
        org = (m.group("organisation") or "").strip()
        key = (role.lower(), org.lower())
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            {
                "forename": "",
                "familyname": "",
                "alternate_name": "",
                "title": "",
                "role": role,
                "role_schema": map_role_to_schema_entry(role),
                "associated_place": "",
                "associated_organisation": (
                    match_organization({"name": org}, KNOWN_ORGS)[0]["data"]
                    if org
                    else {}
                ),
                "nodegoat_id": "",
                "match_score": 0,
                "confidence": "mentioned_role",
            }
        )
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
        extract_authors_raw,
        extract_recipients_raw,
        letter_match_and_enrich,
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


def extract_role_from_raw_name(raw_name: str) -> Tuple[str, List[str]]:
    """
    Nutzt ROLE_BEFORE_NAME_RE + Fallbacks, um Rollennamen aus einem kombinierten Namensstring zu extrahieren.
    Gibt zurück: (bereinigter Name, Liste erkannter Rollen).
    """
    name = raw_name.strip().rstrip(".,:;")
    roles_found: List[str] = []

    # 1) Regex-Extraktion (inkl. Genitivformen etc.)
    match = ROLE_BEFORE_NAME_RE.match(name)
    if match:
        role_raw = match.group("role")
        cleaned_name = match.group("name").strip()
        org = match.group("organisation") or ""
        role_base = normalize_and_match_role(role_raw)
        if role_base:
            roles_found.append(role_base)
        print(
            f"[DEBUG] extract_role_from_raw_name: regex match → role='{role_raw}', org='{org}', name='{cleaned_name}'"
        )
        return cleaned_name, roles_found

    # 2) Fallback: ", Rolle" am Ende
    lower = name.lower()
    for key in sorted(POSSIBLE_ROLES, key=len, reverse=True):
        pat = rf"(?:,\s*)?{re.escape(key)}$"
        if re.search(pat, lower):
            canon = map_role_to_schema_entry(key)
            roles_found.append(canon)
            lower = re.sub(pat, "", lower).strip(" ,")
            cleaned = lower.title()
            print(
                f"[DEBUG] extract_role_from_raw_name: fallback suffix → role='{key}', name='{cleaned}'"
            )
            return cleaned, roles_found

    # 3) Fallback: "Rolle ..." am Anfang
    for key in sorted(POSSIBLE_ROLES, key=len, reverse=True):
        pat = rf"^{re.escape(key)}\s+"
        if re.search(pat, lower):
            canon = map_role_to_schema_entry(key)
            roles_found.append(canon)
            lower = re.sub(pat, "", lower).strip()
            cleaned = lower.title()
            print(
                f"[DEBUG] extract_role_from_raw_name: fallback prefix → role='{key}', name='{cleaned}'"
            )
            return cleaned, roles_found

    # 4) Wenn innerhalb von 2 Wörtern eine bekannte Person, Rolle dieser Person zuweisen
    if roles_found:
        tokens = name.split()
        for idx, tok in enumerate(tokens):
            if tok in person_names:
                # prüfe Abstand im Token-List
                # wir wissen: Rolle und Name stammen aus demselben raw_name
                if abs(idx - tokens.index(tok)) <= 2:
                    cleaned_name = tok
                    print(
                        f"[DEBUG] extract_role_from_raw_name: nearby person match → assign role '{roles_found[0]}' to '{tok}'"
                    )
                    break

    # 4) Keine Rolle erkannt
    return name, roles_found
