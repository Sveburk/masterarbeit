import re
from typing import Dict, Any, Optional, List
from .document_schemas import BaseDocument

# --- Dokumenttypen für Author-/Recipient-Erkennung ---
ALLOWED_ADDRESSING_TYPES = ["Brief", "Postkarte"]

# --- Grußformeln / Closing Patterns ---
GREETING_PATTERNS = [
    r"Mit\s+freundlichen\s+Grüßen\b",
    r"Heil\s*Hitler\b!?,",
    r"mit\s+treudeutschen\s+Grüßen",
    r"treudeutschen\s+Grüßen",
    r"mit\s+deutschem\s+Sängergruß(?:en)?",
    r"mit\s+deutschen\s+Sängergrüßen",
    r"mit\s+badischem\s+Sängergruß(?:en)?",
    r"mit\s+badischen\s+Sängergrüßen"
]
_CLOSING_RE = re.compile("|".join(GREETING_PATTERNS), re.IGNORECASE)

# --- Rollen/Funktions-Patterns für Author-Erkennung ---
ROLE_PATTERNS = re.compile(
    r"\b(?:führer|vereinsführer|vorsitzender|schriftführer|kassierer|chorleiter)\b",
    re.IGNORECASE
)


def extract_author_raw(text: str) -> Dict[str, str]:
    """
    Extrahiert den (Roh-)Autor anhand der letzten Grußformel und Name-Zeile danach.
    """
    matches = list(_CLOSING_RE.finditer(text))
    if not matches:
        return {"forename": "", "familyname": "", "role": "", "closing": ""}
    closing_m = matches[-1]
    closing = closing_m.group().strip()

    rest_lines = text[closing_m.end():].splitlines()
    name_pattern = re.compile(r"^([A-ZÄÖÜ]\.|[A-ZÄÖÜ][a-zß]+)(?:\s+[A-ZÄÖÜ][a-zß]+)+")
    name_line = None

    for line in rest_lines:
        stripped = line.strip().rstrip('.,;:')
        if not stripped or _CLOSING_RE.fullmatch(stripped):
            continue
        if ROLE_PATTERNS.search(stripped) and ',' not in stripped:
            continue
        if not name_pattern.match(stripped):
            continue
        name_line = stripped
        break

    if not name_line:
        return {"forename": "", "familyname": "", "role": "", "closing": closing}

    # a) Name + Rolle
    m = re.match(
        r"^([A-ZÄÖÜ]\.\s*[A-ZÄÖÜ][a-zß]+|[A-ZÄÖÜ][a-zß]+(?:\s+[A-ZÄÖÜ][a-zß]+)*)\s*,\s*([A-Za-zäöüÄÖÜß ]+)$",
        name_line
    )
    if m:
        name_part, role_part = m.group(1), m.group(2).strip().capitalize()
        mi = re.match(r"^([A-ZÄÖÜ])\.\s*([A-ZÄÖÜ][a-zß]+)$", name_part)
        if mi:
            fn, ln = mi.group(1), mi.group(2)
        else:
            parts = name_part.split()
            fn, ln = parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""
        return {"forename": fn, "familyname": ln, "role": role_part, "closing": closing}

    # b) Initiale + Nachname
    m = re.match(r"^([A-ZÄÖÜ])\.\s*([A-ZÄÖÜ][a-zß]+)$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": m.group(2), "role": "", "closing": closing}

    # c) Vor- + Nachname
    m = re.match(r"^([A-ZÄÖÜ][a-zß]+)\s+([A-ZÄÖÜ][a-zß]+)$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": m.group(2), "role": "", "closing": closing}

    # d) Einzelner Name
    m = re.match(r"^([A-ZÄÖÜ][a-zß]{2,})$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": "", "role": "", "closing": closing}

    return {"forename": "", "familyname": "", "role": "", "closing": closing}


def extract_recipient_raw(text: str) -> Dict[str, str]:
    """
    Extrahiert den (Roh-)Empfänger anhand des Headers (bis zur ersten Leerzeile).
    """
    header = text.split("\n\n", 1)[0]
    for line in header.splitlines():
        m = re.search(r"\b(Frau|Herr)n?\s+([A-ZÄÖÜ][a-zß]+)(?:\s+([A-ZÄÖÜ][a-zß]+))?", line)
        if m:
            anrede = m.group(1)
            names = [g for g in m.groups()[1:] if g]
            fn = names[0] if len(names) > 1 else ""
            ln = names[-1] if names else ""
            return {"anrede": anrede, "forename": fn, "familyname": ln, "role": "", "closing": ""}
    return {"anrede": "", "forename": "", "familyname": "", "role": "", "closing": ""}


def letter_match_and_enrich(raw: Dict[str, str], text: str) -> Dict[str, Any]:
    # Lazy-import zur Vermeidung von Zyklen
    from .person_matcher import match_person, KNOWN_PERSONS, get_matching_thresholds
    from .Assigned_Roles_Module import assign_roles_to_known_persons

    person_query = {"forename": raw.get("forename", ""), "familyname": raw.get("familyname", "")}
    match, score = match_person(person_query, KNOWN_PERSONS)
    thresholds = get_matching_thresholds()

    # nur übernehmen, wenn Score oberhalb der minimalen Schwelle liegt
    if match and score >= thresholds.get("familyname", 0):
        enriched = {**raw, **match, "match_score": score, "confidence": "fuzzy"}
        enriched_list = assign_roles_to_known_persons([enriched], text)
        return enriched_list[0]

    # kein zufriedenstellender Match: rohes Ergebnis zurückgeben
    return {
        **raw,
        "nodegoat_id": "",
        "associated_place": "",
        "associated_organisation": "",
        "match_score": 0,
        "confidence": "none"
    }


def match_author(text: str, document_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Ermittelt den Autor, sofern der Dokumenttyp Brief oder Postkarte ist.
    """
    if document_type not in ALLOWED_ADDRESSING_TYPES:
        return {
            "forename": "", "familyname": "", "role": "", "closing": "",
            "nodegoat_id": "", "associated_place": "", "associated_organisation": "",
            "match_score": 0, "confidence": "none"
        }
    raw = extract_author_raw(text)
    return letter_match_and_enrich(raw, text)


def match_recipient(text: str, document_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Ermittelt den Empfänger, sofern der Dokumenttyp Brief oder Postkarte ist.
    """
    if document_type not in ALLOWED_ADDRESSING_TYPES:
        return {
            "anrede": "", "forename": "", "familyname": "", "role": "", "closing": "",
            "nodegoat_id": "", "associated_place": "", "associated_organisation": "",
            "match_score": 0, "confidence": "none"
        }
    raw = extract_recipient_raw(text)
    return letter_match_and_enrich(raw, text)
