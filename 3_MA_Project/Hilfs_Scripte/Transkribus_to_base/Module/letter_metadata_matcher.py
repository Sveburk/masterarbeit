import re
from typing import Dict, Any, Optional, List, Tuple
from .document_schemas import BaseDocument
from .document_schemas import Person


# --- Dokumenttypen für authors-/recipients-Erkennung ---
ALLOWED_ADDRESSING_TYPES = ["Brief", "Postkarte"]

# --- Grußformeln / Closing Patterns ---
GREETING_PATTERNS = [
     r"der\s+Vereinsführer\b" ,         #prüfen, ob das so gut läuft
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

# --- Rollen/Funktions-Patterns für authors-Erkennung ---
ROLE_PATTERNS = re.compile(
    r"\b(?:führer|vereinsführer|vorsitzender|schriftführer|kassierer|chorleiter)\b",
    re.IGNORECASE
)


def extract_authors_raw(text: str) -> Dict[str, str]:
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
        # wenn Rolle UND Name (ohne Komma) → direkt als Autor mit Rolle nehmen
        m_role_name = re.match(
            rf"^(?:der|die)\s*(?P<role>{ROLE_PATTERNS.pattern})\s+(?P<lastname>[A-ZÄÖÜ][a-zäöüß]+)$",
            stripped,
            re.IGNORECASE
        )
        if m_role_name:
            fn = ""  # keine Vornameninformation
            ln = m_role_name.group("lastname")
            role = m_role_name.group("role").capitalize()
            return {"forename": fn, "familyname": ln, "role": role, "closing": closing}

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


def extract_recipients_raw(text: str) -> Dict[str, str]:
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


def match_authors(text: str, document_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Ermittelt den Autor, sofern der Dokumenttyp Brief oder Postkarte ist.
    """
    # if document_type not in ALLOWED_ADDRESSING_TYPES:
    #     return {
    #         "forename": "", "familyname": "", "role": "", "closing": "",
    #         "nodegoat_id": "", "associated_place": "", "associated_organisation": "",
    #         "match_score": 0, "confidence": "none"
    #     }
    raw = extract_authors_raw(text)
    return letter_match_and_enrich(raw, text)


def match_recipients(text: str, document_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Ermittelt den Empfänger, sofern der Dokumenttyp Brief oder Postkarte ist.
    """
    # if document_type not in ALLOWED_ADDRESSING_TYPES:
    #     return {
    #         "anrede": "", "forename": "", "familyname": "", "role": "", "closing": "",
    #         "nodegoat_id": "", "associated_place": "", "associated_organisation": "",
    #         "match_score": 0, "confidence": "none"
    #     }
    raw = extract_recipients_raw(text)
    return letter_match_and_enrich(raw, text)

import json
from pathlib import Path
from .person_matcher import match_person, KNOWN_PERSONS

def resolve_llm_custom_authors_recipients(base_doc: BaseDocument,
                                        xml_text: str,
                                        log_path: Optional[Path] = None
                                        ) -> Tuple[Optional[Person], Optional[Person]]:
    """
    Analysiert custom-Tags im XML nach authors/recipients-Einträgen und vergleicht sie mit vorher erkannten Personen.
    Wenn dort authors/recipients leer ist, übernimmt es den LLM-Vorschlag mit match_score="llm-matched".
    Bei Konflikten erfolgt ein print-Warning + optionales Log.
    """

    import xml.etree.ElementTree as ET
    import re
    import json

    log = []

    # Helper: Durchsuche alle TextLines mit passenden custom-Tags
    def extract_tagged_persons(xml_root: ET.Element, tag: str) -> List[Dict[str, str]]:
        ns = {'ns': 'http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15'}
        persons = []
        for textline in xml_root.findall(".//ns:TextLine", ns):
            custom_attr = textline.attrib.get("custom", "")
            if tag in custom_attr:
                unicode_el = textline.find(".//ns:Unicode", ns)
                if unicode_el is not None:
                    text = unicode_el.text or ""
                    # Extrahiere alle passenden Tags mit Offset/Length
                    pattern = (
                        rf"{tag}\s*\{{{{offset:(\d+); length:(\d+)"
                        r"(?:; role:([^;}}]*))?"
                        r"\s*\}}}}"
                    )
                    for match in re.finditer(pattern, custom_attr):
                        offset, length, role = match.groups()
                        span = text[int(offset):int(offset)+int(length)].strip()
                        if span:
                            print(f"[DEBUG] LLM-{tag} erkannt: '{span}' aus Zeile '{text}'")
                            name_parts = span.split()
                            if len(name_parts) == 1:
                                persons.append({
                                    "forename": name_parts[0],
                                    "familyname": "",
                                    "title": "",
                                    "role": (role or "").strip(),
                                    "match_score": "llm-matched",
                                    "confidence": "low"
                                })
                            else:
                                persons.append({
                                    "forename": name_parts[0],
                                    "familyname": name_parts[-1],
                                    "title": "",
                                    "role": (role or "").strip(),
                                    "match_score": "llm-matched",
                                    "confidence": "llm"
                                })
        return persons

    # Helper: Matching gegen bekannte Personen oder aus mentioned_persons
    def match_and_resolve(entries: List[Dict[str, str]], field: str):
        nonlocal log
        for entry in entries:
            # Erst gegen mentioned_persons prüfen
            matched = None
            for p in base_doc.mentioned_persons:
                if p.forename == entry.get("forename") and p.familyname == entry.get("familyname"):
                    matched = p
                    break

            if matched:
                person = matched
                person.role = entry.get("role", "")
                person.match_score = 100
                person.confidence = "llm"
            else:
                from Module.person_matcher import match_person, KNOWN_PERSONS
                match_result, _ = match_person(entry, KNOWN_PERSONS)
                if match_result:
                    person = Person(**{
                        **match_result,
                        "role": entry.get("role", ""),
                        "match_score": 100,
                        "confidence": "llm"
                    })
                else:
                    person = Person(
                        forename=entry.get("forename", ""),
                        familyname=entry.get("familyname", ""),
                        title="",
                        role=entry.get("role", ""),
                        associated_place="",
                        associated_organisation="",
                        nodegoat_id="",
                        match_score="llm-matched",
                        confidence="llm"
                    )

            # Eintrag übernehmen oder Konflikt melden
            current = getattr(base_doc, field)
            if current and (current.forename != person.forename or current.familyname != person.familyname):
                warn = f"⚠️ Widerspruch bei {field}: '{current.forename} {current.familyname}' ↔ '{person.forename} {person.familyname}'"
                print(warn)
                log.append({
                    "type": field,
                    "existing": current.to_dict(),
                    "llm_suggestion": person.to_dict(),
                    "warning": warn
                })
            else:
                setattr(base_doc, field, person)
                print(f"[DEBUG] Autor aus custom-Tag gesetzt: {person.forename} {person.familyname}")


    # Starte XML-Verarbeitung
    root = ET.fromstring(xml_text)
    authors_entries = extract_tagged_persons(root, "authors")
    recipients_entries = extract_tagged_persons(root, "recipients")

    match_and_resolve(authors_entries, "authors")
    match_and_resolve(recipients_entries, "recipients")

    if log_path and log:
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)

    # Return the entire lists instead of single objects
    return base_doc.authors, base_doc.recipients
