import re
from typing import Dict, Any, Optional, List, Tuple
from .document_schemas import BaseDocument
from .document_schemas import Person
from .Assigned_Roles_Module import KNOWN_ROLE_LIST
import json
from pathlib import Path
from .person_matcher import match_person, KNOWN_PERSONS
from .person_matcher import match_person, KNOWN_PERSONS, get_matching_thresholds
from .Assigned_Roles_Module import assign_roles_to_known_persons, map_role_to_schema_entry, normalize_and_match_role

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
    r"mit\s+badischen\s+Sängergrüßen",
    # Weitere häufige Grußformeln
    r"mit\s+kameradschaftlichen\s+Grüßen",
    r"mit\s+besten\s+Grüßen",
    r"(?:ich\s+)?verbleibe\s+mit",
    r"Herzliche\s+Grüße",
    r"(?:Hochachtungsvoll|Hochachtend)",
    r"Ihr\s+ergebener"
]
_CLOSING_RE = re.compile("|".join(GREETING_PATTERNS), re.IGNORECASE)

# --- Rollen/Funktions-Patterns für authors-Erkennung ---
ROLE_PATTERNS = re.compile(
    rf"\b(?:{'|'.join(map(re.escape, KNOWN_ROLE_LIST))})\b",
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
    name_pattern = re.compile(r"^[A-ZÄÖÜ](\.?\s+[A-ZÄÖÜ][a-zß]+)$|^[A-ZÄÖÜ][a-zß]+\s+[A-ZÄÖÜ][a-zß]+$")
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
        # Wenn vorherige Zeile Rolle enthält → verknüpfe Rolle mit Name
        if len(rest_lines) >= 2:
            role_line = rest_lines[0].strip().rstrip('.,;:')
            name_line = rest_lines[1].strip().rstrip('.,;:')
            role_match = re.match(
                rf"^(?:der|die)\s*(?P<role>{ROLE_PATTERNS.pattern})\s*:?$",
                role_line,
                re.IGNORECASE
            )
            name_match = re.match(r"^([A-ZÄÖÜ])\.?\s*([A-ZÄÖÜ][a-zß]+)$", name_line)
            if role_match and name_match:
                return {
                    "forename": name_match.group(1),
                    "familyname": name_match.group(2),
                    "role": role_match.group("role").capitalize(),
                    "closing": closing
                }
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
    # b2) Initiale ohne Punkt + Nachname
    m = re.match(r"^([A-ZÄÖÜ])\s+([A-ZÄÖÜ][a-zß]+)$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": m.group(2), "role": "", "closing": closing}


    # c) Vor- + Nachname
    m = re.match(r"^([A-ZÄÖÜ][a-zß]+)\s+([A-ZÄÖÜ][a-zß]+)$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": m.group(2), "role": "", "closing": closing}

    # d) Einzelner Name
    if name_line.lower() in (r.lower() for r in KNOWN_ROLE_LIST):
        return {"forename": "", "familyname": "", "role": name_line.capitalize(), "closing": closing}
    m = re.match(r"^([A-ZÄÖÜ][a-zß]{2,})$", name_line)
    if m:
        return {"forename": m.group(1), "familyname": "", "role": "", "closing": closing}

    return {"forename": "", "familyname": "", "role": "", "closing": closing}


def extract_recipients_raw(text: str) -> Dict[str, str]:
    """
    Extrahiert den (Roh-)Empfänger anhand des Headers (bis zur ersten Leerzeile).
    """
    header = text.split("\n\n", 1)[0]
    lines = header.splitlines()
    for i, line in enumerate(lines):
        m = re.search(r"\b(Frau|Herr)n?\s+([A-ZÄÖÜ][a-zß]+)(?:\s+([A-ZÄÖÜ][a-zß]+))?", line)
        if m:
            anrede = m.group(1)
            names = [g for g in m.groups()[1:] if g]
            fn = names[0] if len(names) > 1 else ""
            ln = names[-1] if names else ""
            # Wenn nächste Zeile vorhanden, als Rolle verwenden
            role = ""
            if i+1 < len(lines):
                nxt = lines[i+1].strip()
                if nxt and not nxt.startswith(("Herr","Frau")):
                    role = nxt.rstrip('.,;:')
            return {"anrede": anrede, "forename": fn, "familyname": ln, "role": role, "closing": ""}
    return {"anrede": "", "forename": "", "familyname": "", "role": "", "closing": ""}



def letter_match_and_enrich(raw: Dict[str, str], text: str, mentioned_persons: List[Person] = None) -> Dict[str, Any]:
    from .person_matcher import match_person, KNOWN_PERSONS, get_matching_thresholds
    from .Assigned_Roles_Module import assign_roles_to_known_persons, map_role_to_schema_entry, normalize_and_match_role

    if mentioned_persons is None:
        mentioned_persons = []

    # Process role if it exists in raw data using CSV lookup
    role_raw = raw.get("role", "").strip()
    role_schema = ""
    if role_raw:
        # Use normalize_and_match_role to get canonical role from CSV
        normalized_role = normalize_and_match_role(role_raw)
        if normalized_role:
            role_schema = map_role_to_schema_entry(normalized_role)
            raw["role"] = normalized_role
            raw["role_schema"] = role_schema
            print(f"[DEBUG] Rolle normalisiert: '{role_raw}' → '{normalized_role}' (Schema: {role_schema})")

    # 1) Schon in mentioned_persons?
    for p in mentioned_persons:
        if p.forename == raw.get("forename") and p.familyname == raw.get("familyname"):
            print(f"[DEBUG] Autor/Empfänger schon in mentioned_persons: {p.forename} {p.familyname}")
            enriched = p.to_dict()  # jetzt sicher ein dict
            enriched["confidence"]  = "from_mentions"
            enriched["match_score"] = 100
            for k, v in raw.items():
                if not enriched.get(k) and v:  # Nur setzen wenn v nicht leer
                    enriched[k] = v
            return enriched

    # 2) Fuzzy-Match
    person_query = {"forename": raw.get("forename",""), "familyname": raw.get("familyname","")}
    match, score = match_person(person_query, KNOWN_PERSONS)
    thresholds = get_matching_thresholds()

    if match and score >= thresholds.get("familyname", 0):
        enriched = {**raw, **match, "match_score": score, "confidence": "fuzzy"}
        
        # Preserve roles from raw data when enriching
        if role_raw:
            enriched["role"] = raw["role"]
            enriched["role_schema"] = raw.get("role_schema", "")
        
        enriched_list = assign_roles_to_known_persons([enriched], text)
        enriched_candidate = enriched_list[0]

        # Wenn assign_roles_to_known_persons ein Person-Objekt zurückgibt, ins dict wandeln:
        if isinstance(enriched_candidate, Person):
            enriched = enriched_candidate.to_dict()
        else:
            enriched = enriched_candidate

        # Roh-Felder ergänzen, falls noch fehlen
        for k, v in raw.items():
            if v and not enriched.get(k):  # Nur setzen wenn v nicht leer und noch nicht gesetzt
                enriched[k] = v

        # Ensure consistent return format with all required keys
        for key in ["anrede", "forename", "familyname", "role", "closing", "nodegoat_id", 
                   "associated_place", "associated_organisation", "match_score", "confidence"]:
            enriched.setdefault(key, "")
            
        return enriched

    # 3) Null-Fallback
    result = {
        **raw,
        "nodegoat_id":             "",
        "associated_place":        "",
        "associated_organisation": "",
        "match_score":             0,
        "confidence":              "none"
    }
    
    # Ensure consistent return format
    for key in ["anrede", "forename", "familyname", "role", "closing"]:
        result.setdefault(key, "")
        
    return result






def match_authors(text: str, document_type: Optional[str] = None, mentioned_persons: List[Person] = []) -> Dict[str, Any]:
    raw = extract_authors_raw(text)
    return letter_match_and_enrich(raw, text, mentioned_persons)

def match_recipients(text: str, document_type: Optional[str] = None, mentioned_persons: List[Person] = []) -> Dict[str, Any]:
    raw = extract_recipients_raw(text)
    return letter_match_and_enrich(raw, text, mentioned_persons)



def assign_roles_from_context(text_lines: List[str], base_doc: BaseDocument):
    """
    Kombiniert beide Methoden:
    – Rolle und Name in einer Zeile („Schriftführer: F. Jung“)
    – Rolle in einer Zeile, Name in der nächsten („Schriftführer:“, „F. Jung“)
    """

    # Import known role list from Assigned_Roles_Module
    from .Assigned_Roles_Module import KNOWN_ROLE_LIST, normalize_and_match_role

    # Verbesserte Patterns mit ground-truth Rollenliste statt permissive Regex
    ROLES_PATTERN = "|".join(map(re.escape, KNOWN_ROLE_LIST))

    INLINE_PATTERN = re.compile(
        rf"(?:Der|Die)?\s*({ROLES_PATTERN})[,:]?\s+([A-ZÄÖÜ]\.?\s+[A-ZÄÖÜ][a-zäöüß]+|[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)",
        re.IGNORECASE
    )
    ROLE_ONLY_PATTERN = re.compile(
        rf"(?:Der|Die)?\s*({ROLES_PATTERN})[,:]?\s*$", re.IGNORECASE
    )
    NAME_PATTERN = re.compile(
        r"([A-ZÄÖÜ]\.?)?\s*([A-ZÄÖÜ][a-zäöüß]+)"
    )

    for idx, line in enumerate(text_lines):
        line = line.strip()

        # Fall 1: Rolle + Name in einer Zeile
        inline_match = INLINE_PATTERN.match(line)
        if inline_match:
            role_raw = inline_match.group(1).strip()
            name_raw = inline_match.group(2).strip()
            # Rolle normalisieren mit ground-truth Liste
            normalized_role = normalize_and_match_role(role_raw)
            role = normalized_role if normalized_role else role_raw.capitalize()

            name_parts = name_raw.split()
            if len(name_parts) == 2:
                forename, familyname = name_parts
            else:
                continue

            _assign_role(base_doc, forename, familyname, role)
            continue

        # Fall 2: Rolle in dieser Zeile, Name in nächster
        if idx + 1 < len(text_lines):
            next_line = text_lines[idx + 1].strip()
            role_match = ROLE_ONLY_PATTERN.match(line)
            name_match = NAME_PATTERN.match(next_line)
            if role_match and name_match:
                role_raw = role_match.group(1).strip()
                forename = name_match.group(1).replace(".", "") if name_match.group(1) else ""
                familyname = name_match.group(2)
                # Rolle normalisieren mit ground-truth Liste
                normalized_role = normalize_and_match_role(role_raw)
                role = normalized_role if normalized_role else role_raw.capitalize()

                _assign_role(base_doc, forename, familyname, role)

def _assign_role(base_doc: BaseDocument, forename: str, familyname: str, role: str):
    for person in base_doc.mentioned_persons:
        match_found = False
        if forename and is_initial(forename):
            if person.forename and person.forename.startswith(forename) and person.familyname == familyname:
                match_found = True
        elif person.forename == forename and person.familyname == familyname:
            match_found = True
        elif not forename and person.familyname == familyname:
            match_found = True

        if match_found:
            if not person.role:
                person.role = role
                # Set role_schema when setting role
                from Module.Assigned_Roles_Module import map_role_to_schema_entry
                person.role_schema = map_role_to_schema_entry(role)
                print(f"[DEBUG] Rolle erkannt: {person.forename} {person.familyname} → {role} (Schema: {person.role_schema})")
            break



def resolve_llm_custom_authors_recipients(base_doc: BaseDocument,
                                        xml_text: str,
                                        log_path: Optional[Path] = None
                                        ) -> Tuple[List[Person], List[Person]]:
    """
    Analysiert custom-Tags im XML nach authors/recipients-Einträgen und vergleicht sie mit vorher erkannten Personen.
    Wenn dort authors/recipients leer ist, übernimmt es den LLM-Vorschlag mit match_score="llm-matched".
    Bei Konflikten erfolgt ein print-Warning + optionales Log.

    Returns:
        Tuple[List[Person], List[Person]]: Listen der erkannten Autoren und Empfänger
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
                # Set role_schema when setting role
                if person.role:
                    from Module.Assigned_Roles_Module import map_role_to_schema_entry
                    person.role_schema = map_role_to_schema_entry(person.role)
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
                    role = entry.get("role", "")
                    # Get the role_schema for the role
                    role_schema = ""
                    if role:
                        from Module.Assigned_Roles_Module import map_role_to_schema_entry
                        role_schema = map_role_to_schema_entry(role)

                    person = Person(
                        forename=entry.get("forename", ""),
                        familyname=entry.get("familyname", ""),
                        title="",
                        role=role,
                        role_schema=role_schema,
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
                existing = getattr(base_doc, field)
                if not any(p.nodegoat_id == person.nodegoat_id for p in existing):
                    existing.append(person)
                    print(f"[DEBUG] Autor/Empfänger zu Liste {field} hinzugefügt: {person.forename} {person.familyname}")
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
     
    # Kontextbasierte Rollenprüfung
    assign_roles_from_context(xml_text.splitlines(), base_doc)
    # Ensure authors and recipients are in mentioned_persons
    ensure_author_recipient_in_mentions(base_doc)

    # Always return lists (even if empty) for consistent return type
    author_list = base_doc.authors if base_doc.authors else []
    recipient_list = base_doc.recipients if base_doc.recipients else []

    return author_list, recipient_list

def extract_authors_recipients_from_mentions(
    mentioned: List[Person],
    author_ids: List[str],
    recipient_ids: List[str]
) -> Tuple[List[Person], List[Person]]:
    """
    Gibt vollständige Kopien der Autoren- und Empfängerpersonen basierend auf den nodegoat_ids zurück.
    Duplikate in mentioned_persons sind erlaubt.
    """
    import copy

    authors = [copy.deepcopy(p) for p in mentioned if p.nodegoat_id in author_ids]
    recipients = [copy.deepcopy(p) for p in mentioned if p.nodegoat_id in recipient_ids]
    return authors, recipients
def ensure_author_recipient_in_mentions(
    base_doc: BaseDocument,
    transcript_text: str
) -> None:
    """
    Stellt sicher, dass alle Autoren und Empfänger in mentioned_persons sind,
    und enrich­t die Rollen in-place auf genau diesen Instanzen.
    """
    # --- 1) Alte Logik: sicherstellen, dass authors/recipients in mentioned_persons sind ---
    seen = {(p.forename, p.familyname) for p in base_doc.mentioned_persons}
    def append_if_missing(p: Person):
        key = (p.forename, p.familyname)
        for mp in base_doc.mentioned_persons:
            if p.nodegoat_id and mp.nodegoat_id == p.nodegoat_id or (mp.forename, mp.familyname) == key:
                # falls match und mp noch keine Rolle hat, aber Autor/Empfänger schon:
                if not mp.role and p.role:
                    mp.role = p.role
                    mp.role_schema = getattr(p, "role_schema", "")
                return
        # sonst neu hinzufügen
        copy = Person.from_dict(p.to_dict())
        copy.confidence = copy.confidence or "author/recipient_only"
        copy.match_score = copy.match_score or 100
        base_doc.mentioned_persons.append(copy)

    for a in base_doc.authors:
        append_if_missing(a)
    for r in base_doc.recipients:
        append_if_missing(r)

    # --- 2) Rollen auf alle mentioned_persons nachziehen ---
    # Wir arbeiten nur auf mentioned_persons, authors/recipients referenzieren dieselben Instanzen.
    person_dicts = [p.to_dict() for p in base_doc.mentioned_persons]
    enriched = assign_roles_to_known_persons(person_dicts, transcript_text)

    for obj, data in zip(base_doc.mentioned_persons, enriched):
        # `data` kann Dict oder Person sein
        role = data.get("role") if isinstance(data, dict) else data.role
        schema = data.get("role_schema") if isinstance(data, dict) else getattr(data, "role_schema", "")
        if role:
            obj.role = role
            obj.role_schema = schema
