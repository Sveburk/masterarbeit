import re
from typing import Dict, Any, Optional, List, Tuple
from .document_schemas import BaseDocument
from .document_schemas import Person
from .Assigned_Roles_Module import normalize_and_match_role, map_role_to_schema_entry, ROLE_MAPPINGS_DE, KNOWN_ROLE_LIST

import json
from pathlib import Path
from .person_matcher import match_person, KNOWN_PERSONS
from .person_matcher import match_person, KNOWN_PERSONS, get_matching_thresholds
from .Assigned_Roles_Module import assign_roles_to_known_persons, map_role_to_schema_entry, normalize_and_match_role

# --- Dokumenttypen für authors-/recipients-Erkennung ---
ALLOWED_ADDRESSING_TYPES = ["Brief", "Postkarte"]

# --- Grußformeln / Closing Patterns ---
GREETING_PATTERNS = [
    r"der\s+Vereinsführer\b",
    r"Mit\s+freundlichen\s+Grüßen\b",
    r"Heil\s*Hitler\b!?,",
    r"mit\s+treudeutschen\s+Grüßen",
    r"treudeutschen\s+Grüßen",
    r"mit\s+deutschem\s+Sängergruß(?:en)?",
    r"mit\s+deutschen\s+Sängergrüßen",
    r"mit\s+badischem\s+Sängergruß(?:en)?",
    r"mit\s+badischen\s+Sängergrüßen",
    r"mit\s+kameradschaftlichen\s+Grüßen",
    r"mit\s+besten\s+Grüßen",
    r"(?:ich\s+)?verbleibe\s+mit",
    r"Herzliche\s+Grüße",
    r"(?:Hochachtungsvoll|Hochachtend)",
    r"Ihr\s+ergebener",
    r"Deine\s+.*?vergessende\b"          # “Deine Dich nie vergessende”
]
_CLOSING_RE = re.compile("|".join(GREETING_PATTERNS), re.IGNORECASE)


# --- Recipient Patterns (jetzt jeweils bis zum Zeilenende) ---
RECIPIENT_PATTERNS = [
    r"An\b",                                    # “An” allein
    r"An\s+Herrn\b",                            # “An Herrn Fritz Jung”
    r"An\s+Frau\b",                             # “An Frau Maria Müller”
    r"Herrn\b",                                 # “Herrn Fritz Jung”
    r"Frau\b",                                  # “Frau Maria Müller”
    r"Lieber\b",                                # “Lieber Otto!”
    r"Liebe[n]?\b",                             # “Liebe Maria!”
    r"mein\s+lieber\b",                         # “mein lieber Otto”
    r"meine\s+lieben\b",                        # “meine Lieben”
    # → Dynamisch extrahierbare Rollenansprache
    r"An\s+den\s+[A-ZÄÖÜa-zäöüß ]+",
    r"An\s+die\s+[A-ZÄÖÜa-zäöüß ]+",
    r"An\s+das\s+[A-ZÄÖÜa-zäöüß ]+",
]
INDIRECT_RECIPIENT_PATTERNS = [
    r"zu\s+Händen\s+(?:des\s+)?Herrn\s+([A-ZÄÖÜ][a-zäöüß]+)\s+([A-ZÄÖÜ][a-zäöüß]+)",
    r"z\.H\.d\s+Herrn\s+([A-ZÄÖÜ][a-zäöüß]+)\s+([A-ZÄÖÜ][a-zäöüß]+)",
    r"zu\s+Händen\s+Frau\s+([A-ZÄÖÜ][a-zäöüß]+)\s+([A-ZÄÖÜ][a-zäöüß]+)",
    r"z\.H\.d\s+Frau\s+([A-ZÄÖÜ][a-zäöüß]+)\s+([A-ZÄÖÜ][a-zäöüß]+)"
]






# --- Compile mit MULTILINE und IGNORECASE ---
_RECIPIENT_RE = re.compile(
    r"(?mi)^[ \t]*(?:" + "|".join(RECIPIENT_PATTERNS) + r").*"
)


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
    Extrahiert den (Roh-)Empfänger aus dem Briefkopf:
     - Inline: “Herrn Fritz Jung” / “An Herrn Fritz Jung” / “Frau Maria Meier”
     - Zweizeilig: “Herrn” in Zeile N, Name in Zeile N+1
     - Rollenanrede: “An den Bürgermeister”
    """
    parts = re.split(r"\n\s*\n", text, maxsplit=1)
    header_block = parts[0]
    lines = header_block.splitlines()

    # 1) Versuch: Inline-Pattern auf einer Zeile
    inline_re = re.compile(
        r"""^(?:An\s+)?           # optional “An ”
            (Herrn?|Frau)\s+      # Anrede
            ([A-ZÄÖÜ][\wäöüß]+)   # Vorname
            (?:\s+([A-ZÄÖÜ][\wäöüß]+))?  # optional Nachname
        """, re.IGNORECASE | re.VERBOSE
    )

    for idx, line in enumerate(lines[:5]):
        line_clean = line.strip()
        m = inline_re.search(line_clean)
        if m:
            anrede = m.group(1)
            fn     = m.group(2)
            ln     = m.group(3) or ""
            role   = ""

            # 🔍 Rollenerkennung auch aus derselben Zeile
            role_match = re.search(r"An\s+(?:den|die|das)\s+([A-ZÄÖÜa-zäöüß ]+)", line_clean)
            if role_match:
                raw_role = role_match.group(1).strip()
                role = normalize_and_match_role(raw_role) or raw_role

            # Falls keine Rolle erkannt → prüfe nächste Zeile
            elif idx + 1 < len(lines):
                nxt = lines[idx + 1].strip()
                if nxt and not inline_re.match(nxt):
                    role = nxt.rstrip('.,;:')

            return {
                "anrede": anrede,
                "forename": fn,
                "familyname": ln,
                "role": role,
                "closing": ""
            }

    
    # 2) Zweizeiliger Fall: “Herrn” auf eigener Zeile
    honorific_re = re.compile(r"^(Herrn?|Frau)\s*$", re.IGNORECASE)
    for i, line in enumerate(lines[:5]):
        if honorific_re.match(line.strip()):
            # nächster non-empty line ist Name
            for nxt in lines[i+1:i+4]:
                name = nxt.strip().rstrip('.,;:')
                parts = name.split()
                if len(parts) >= 1 and parts[0][0].isupper():
                    fn = parts[0]
                    ln = parts[1] if len(parts) > 1 else ""
                    return {"anrede": line.strip(), "forename": fn, "familyname": ln, "role": "", "closing": ""}

    # 3) Fallback: nichts gefunden
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
            print(f"[DEBUG-RECIPIENT] matched in mentioned_persons: {p.forename} {p.familyname}")
            enriched = p.to_dict()
            enriched["confidence"]  = "LLM_Tag_match_in_mentioned_persons"
            enriched["match_score"] = 100

            for k, v in raw.items():
                if not enriched.get(k) and v:
                    enriched[k] = v

            # 🛠️ Fallback-Zuweisung role_schema, falls Rolle vorhanden, aber kein Schema
            if enriched.get("role") and not enriched.get("role_schema"):
                enriched["role_schema"] = map_role_to_schema_entry(enriched["role"])
                print(f"[DEBUG-RECIPIENT] role_schema ergänzt aus Rolle: {enriched['role']} → {enriched['role_schema']}")

            return enriched

    # 2) # --- Fuzzy-Match ---
    person_query = {"forename": raw.get("forename",""), "familyname": raw.get("familyname","")}
    match, score = match_person(person_query, KNOWN_PERSONS)
    thresholds = get_matching_thresholds()

    if match and score >= thresholds.get("familyname", 0):
        enriched = {**raw, **match, "match_score": score, "confidence": "fuzzy"}
        if role_raw:
            enriched["role"] = raw["role"]
            enriched["role_schema"] = raw.get("role_schema", "")
        enriched_list = assign_roles_to_known_persons([enriched], text)
        enriched_candidate = enriched_list[0]
        if isinstance(enriched_candidate, Person):
            enriched = enriched_candidate.to_dict()
        else:
            enriched = enriched_candidate
        for k, v in raw.items():
            if v and not enriched.get(k):
                enriched[k] = v
        for key in ["anrede", "forename", "familyname", "role", "closing", "nodegoat_id", 
                   "associated_place", "associated_organisation", "match_score", "confidence"]:
            enriched.setdefault(key, "")
        return enriched

    # --- Fallback: keine Person erkannt, aber evtl. Rolle extrahierbar ---
    result = {
        **raw,
        "nodegoat_id":             "",
        "associated_place":        "",
        "associated_organisation": "",
        "match_score":             0,
        "confidence":              "unverified",
        "needs_review":            True,
        "review_reason":           "solitary_role_only"
    }

    for key in ["anrede", "forename", "familyname", "role", "closing"]:
        result.setdefault(key, "")

    # 🔍 Fallback-Rollenextraktion erneut versuchen
    enriched_role = assign_roles_to_known_persons([result], text)[0]
    if isinstance(enriched_role, Person):
        enriched_role = enriched_role.to_dict()

    if enriched_role.get("role"):
        result["role"] = enriched_role["role"]
        result["role_schema"] = enriched_role.get("role_schema", "")
        print(f"[DEBUG] Rolle (nachträglich) erkannt im Fallback: {result['role']} ({result['role_schema']})")

    if result.get("role"):
        return result

    return None





def match_authors(text: str, document_type: Optional[str] = None, mentioned_persons: List[Person] = []) -> Dict[str, Any]:
    raw = extract_authors_raw(text)
    return letter_match_and_enrich(raw, text, mentioned_persons)

def extract_multiple_recipients_raw(text: str) -> List[Dict[str, Any]]:
    """
    Extrahiert mehrere mögliche Empfänger:
    - Direkte Anredeformen (z.B. „Lieber Otto“)
    - Indirekte Anredeformen (z.B. „zu Händen des Herrn Alfons Zimmermann“)
    Gibt eine Liste von Roh-Empfängern zurück.
    """
    lines = text.splitlines()
    recipients = []

    # --- Direkte Anredeformen (recipient_score: 100) ---
    direct_patterns = [
        r"\bLieber\s+([A-ZÄÖÜ][a-zäöüß]+)",
        r"\bLiebe[rn]?\s+([A-ZÄÖÜ][a-zäöüß]+)",
        r"\bmein\s+lieber\s+([A-ZÄÖÜ][a-zäöüß]+)",
    ]
    for pat in direct_patterns:
        for line in lines:
            m = re.search(pat, line)
            if m:
                recipients.append({
                    "forename": m.group(1),
                    "familyname": "",
                    "role": "",
                    "recipient_score": 100,
                    "confidence": "String_direct_from_text"
                })

    # --- Indirekte Anredeformen (recipient_score: 70) ---
    for pat in INDIRECT_RECIPIENT_PATTERNS:
        for line in lines:
            m = re.search(pat, line)
            if m:
                fn = m.group(1)
                ln = m.group(2) if m.lastindex and m.lastindex >= 2 else ""
                recipients.append({
                    "forename": fn,
                    "familyname": ln,
                    "role": "",
                    "recipient_score": 70,
                    "confidence": "indirect_2line"
                })

    return recipients



def match_recipients(text: str, mentioned_persons: Optional[List[Person]] = None) -> List[Dict[str, Any]]:
    """
    Vereinheitlichte Empfängererkennung:
    – kombiniert klassische Briefkopferkennung und Anredeformen
    – enriched alle erkannten Personen
    – gibt deduplizierte Liste von Empfängern zurück (max. 4)
    """
    if mentioned_persons is None:
        mentioned_persons = []

    # Klassischer Kopf-Empfänger (z. B. "Herrn Fritz Jung")
    primary = extract_recipients_raw(text)
    enriched_primary = letter_match_and_enrich(primary, text, mentioned_persons)

    # 🔐 Nur wenn etwas zurückkam
    if enriched_primary:
        enriched_primary["recipient_score"] = 90
        enriched_primary["confidence"] = enriched_primary.get("confidence", "header")
        all_recipients = [enriched_primary]
    else:
        all_recipients = []

   # Weitere mögliche Empfänger
    additional_raw = extract_multiple_recipients_raw(text)
    enriched_list = []
    for raw in additional_raw:
        enriched = letter_match_and_enrich(raw, text, mentioned_persons)
        if enriched:
            enriched["recipient_score"] = raw.get("recipient_score", 50)
            enriched["confidence"] = raw.get("confidence", "indirect")
            enriched_list.append(enriched)

    all_recipients.extend(enriched_list)

    # Deduplizieren (nach forename + familyname)
    seen = set()
    final = []

    for rec in all_recipients:
        key = (rec.get("forename", "").strip(), rec.get("familyname", "").strip())

        if key in seen or not any(key):
            print(f"[DEBUG-DROP] Recipient gedroppt: {rec}")  # 👈 Hier ist der Debug-Print korrekt
            continue

        seen.add(key)
        final.append(rec)
        if len(final) >= 4:
            break

    return final

def enrich_final_recipients(base_doc: BaseDocument):
    from Module.person_matcher import match_person, KNOWN_PERSONS

    for rec in base_doc.recipients:
        is_incomplete = (
            not rec.nodegoat_id
            and rec.match_score in [None, 0]
            and (
                rec.familyname == ""
                or rec.forename == ""
                or (rec.role and not rec.role_schema)
            )
        )
        if is_incomplete:
            match, score = match_person(rec.to_dict(), KNOWN_PERSONS)
            if match:
                print(f"[DEBUG-FINAL-MATCH] Recipient {rec.forename} {rec.familyname} → Match mit {match.get('forename')} {match.get('familyname')} (Score: {score})")
                rec.nodegoat_id = match.get("nodegoat_id", "")
                rec.role = rec.role or match.get("role", "")
                rec.match_score = score
                rec.confidence = "late-match"
        
        # Übertrage recipient_score in mentioned_persons
        for mp in base_doc.mentioned_persons:
            if (
                mp.forename == rec.forename
                and mp.familyname == rec.familyname
                and (getattr(mp, "recipient_score", 0) in [0, None])
            ):
                mp.recipient_score = getattr(rec, "recipient_score", 0)

    # Zusätzlicher Debug-Output
    print("[DEBUG] recipient_scores in mentioned_persons:", [
        f"{p.forename} {p.familyname}: {getattr(p, 'recipient_score', '-')}"
        for p in base_doc.mentioned_persons
    ])




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
    # Diese Funktion hat praktischen Nutzen und kann für zukünftige Erweiterungen behalten werden
    def extract_tagged_persons(xml_root: ET.Element, tag: str) -> List[Dict[str, str]]:
        """
        Extrahiert Personeneinträge aus XML-Custom-Tags.

        Diese Funktion ist nützlich, weil sie gezielt nach author/recipient-Tags in Transkribus-XML sucht
        und die entsprechenden Personen mit Offset/Length und optionaler Rolle extrahiert.

        Args:
            xml_root: Root-Element des XML-Baums
            tag: Tag-Name (z.B. "author", "recipient")

        Returns:
            Liste von Personen-Dictionaries
        """
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
    # Diese Funktion wird aktiv genutzt und ist wichtig für LLM-Integration
    def match_and_resolve(entries: List[Dict[str, str]], field: str):
        """
        Ordnet extrahierte Personen aus LLM-Tags zu und löst Konflikte auf.

        Diese Funktion ist wichtig, weil sie:
        1. Existierende Personen aus mentioned_persons wiederverwendet
        2. Fuzzy-Matching gegen KNOWN_PERSONS durchführt
        3. role_schema korrekt setzt
        4. Konflikte mit existierenden Autoren/Empfängern protokolliert
        5. Ergebnisse in die base_doc-Listen einträgt

        Args:
            entries: Liste von Personen-Dictionaries
            field: Feldname im BaseDocument (z.B. "authors", "recipients")
        """
        nonlocal log
        for entry in entries:
            # Zuerst prüfen wir gegen mentioned_persons
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
                    # Nur überschreiben, wenn role_schema leer oder von role_only
                    if not person.role_schema or person.confidence == "role_only":
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

            # Skip persons with empty names
            if not (person.forename or person.familyname):
                print(f"[DEBUG] Skipping nameless person in match_and_resolve: role='{person.role}'")
                continue

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
                    setattr(base_doc, field, existing)

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
    Verhindert auch das Hinzufügen von Personen ohne Namen.
    """
    def is_same_person(a: Person, b: Person) -> bool:
        # Präziser Abgleich: entweder gleiche ID oder Vor- und Nachname, aber NICHT nur Vorname
        if a.nodegoat_id and b.nodegoat_id:
            return a.nodegoat_id == b.nodegoat_id
        return (
            a.forename == b.forename and
            a.familyname == b.familyname and
            (a.familyname or b.familyname)  # mind. ein Nachname muss vorhanden sein
        )

    def has_valid_name(p: Person) -> bool:
        """Check if person has at least either a forename or familyname."""
        return bool(p.forename.strip() or p.familyname.strip())

    def append_if_missing(p: Person):
        # Skip persons without any name
        if not has_valid_name(p):
            print(f"[DEBUG] Skipping nameless person in append_if_missing: role='{p.role}', match_score={p.match_score}")
            return

        # Check if this person already exists in mentioned_persons
        for mp in base_doc.mentioned_persons:
            if is_same_person(p, mp):
                # Update role if the existing person has no role but this one does
                if not mp.role and p.role:
                    mp.role = p.role
                    mp.role_schema = getattr(p, "role_schema", "")
                return

        # Add the person to mentioned_persons
        copy = Person.from_dict(p.to_dict())
        copy.recipient_score = getattr(p, "recipient_score", None)
        copy.confidence = copy.confidence or "author/recipient_only"
        copy.match_score = copy.match_score or 100
        base_doc.mentioned_persons.append(copy)
        print(f"[DEBUG] Added person to mentioned_persons: '{copy.forename} {copy.familyname}', role='{copy.role}'")

    # Process authors
    for a in base_doc.authors:
        if has_valid_name(a):
            append_if_missing(a)
        else:
            print(f"[DEBUG] Skipping nameless author: role='{a.role}', match_score={a.match_score}")

    # Process recipients
    for r in base_doc.recipients:
        if has_valid_name(r):
            append_if_missing(r)
        else:
            print(f"[DEBUG] Skipping nameless recipient: role='{r.role}', match_score={r.match_score}")

    # --- 2) Rollen auf alle mentioned_persons nachziehen ---
    # Filter out any persons without valid names before role enrichment
    base_doc.mentioned_persons = [p for p in base_doc.mentioned_persons if has_valid_name(p)]

    # Continue with role enrichment
    person_dicts = [p.to_dict() for p in base_doc.mentioned_persons]
    enriched = assign_roles_to_known_persons(person_dicts, transcript_text)

    for obj, data in zip(base_doc.mentioned_persons, enriched):
        role = data.get("role") if isinstance(data, dict) else data.role
        schema = data.get("role_schema") if isinstance(data, dict) else getattr(data, "role_schema", "")

        # Check if we should update the role_schema
        role_only_source = False
        if isinstance(data, dict):
            role_only_source = data.get("confidence", "") == "role_only"
        else:
            role_only_source = getattr(data, "confidence", "") == "role_only"

        if role:
            # Always update role value
            obj.role = role

            # Only update role_schema if one of these conditions is met:
            # 1. Current schema is empty
            # 2. Data comes from a role-only context (more authoritative)
            if not getattr(obj, "role_schema", "") or role_only_source:
                obj.role_schema = schema
                print(f"[DEBUG] Updated role_schema for {obj.forename} {obj.familyname}: '{obj.role_schema}'")
            else:
                print(f"[DEBUG] Kept existing role_schema for {obj.forename} {obj.familyname}: '{getattr(obj, 'role_schema', '')}'")

    # Final check - ensure we haven't accidentally added any nameless persons
    base_doc.mentioned_persons = [p for p in base_doc.mentioned_persons if has_valid_name(p)]
    print(f"[DEBUG] Final mentioned_persons count after filtering nameless persons: {len(base_doc.mentioned_persons)}")

def postprocess_roles(base_doc: BaseDocument):
    """
    – Verschiebt reine Rollen-Tokens aus forename/familyname in role (mit Normalisierung).
    – Tauscht Name↔Rolle, wenn role eigentlich ein Personenname und
      forename/familyname eine Rolle enthält.
    – Setzt role_schema für alle Rollen und leert Name-Felder, wenn nur Rolle bekannt.
    – Filtert erwähnte Personen nach Blacklist.
    """
    # Import blacklist tokens from person_matcher
    from .person_matcher import NON_PERSON_TOKENS, UNMATCHABLE_SINGLE_NAMES

    # Debug: Start
    print(f"[DEBUG postprocess_roles] Start: {len(base_doc.mentioned_persons)} mentioned_persons")

    filtered: List[Person] = []

    # 1–4: Regeln mit normalize_and_match_role
    for idx, p in enumerate(base_doc.mentioned_persons):
        fn = p.forename.strip()
        ln = p.familyname.strip()
        rl = p.role.strip()
        print(f"[DEBUG] Person[{idx}]: forename='{fn}', familyname='{ln}', role='{rl}'")

        # Blacklist check - skip persons with only blacklisted terms and low match score
        if ((fn.lower() in NON_PERSON_TOKENS and not ln) or
            (ln.lower() in NON_PERSON_TOKENS and not fn)) and getattr(p, "match_score", 0) < 40:
            print(f"[DEBUG] Person[{idx}] skipped due to blacklist: '{fn}'/'{ln}' with match_score {getattr(p, 'match_score', 0)}")
            continue

        # Skip unmatchable single names with low match score
        if ((fn.lower() in UNMATCHABLE_SINGLE_NAMES and not ln) or
            (ln.lower() in UNMATCHABLE_SINGLE_NAMES and not fn)) and getattr(p, "match_score", 0) < 40:
            print(f"[DEBUG] Person[{idx}] skipped due to unmatchable name: '{fn}'/'{ln}' with match_score {getattr(p, 'match_score', 0)}")
            continue

        # Rohes Lowercase
        raw_fn_lower = fn.lower()
        raw_ln_lower = ln.lower()
        print(f"[DEBUG] raw_fn_lower={raw_fn_lower!r}, raw_ln_lower={raw_ln_lower!r}")

        # Normierungen
        norm_fn = normalize_and_match_role(raw_fn_lower) if fn else None
        norm_ln = normalize_and_match_role(raw_ln_lower) if ln else None
        print(f"[DEBUG] norm_fn={norm_fn!r}, norm_ln={norm_ln!r}")

        # Fallback: Singularform für deutsche Plural "-en"
        if fn and not norm_fn and raw_fn_lower.endswith('en'):
            singular = raw_fn_lower[:-2]
            norm_fn = normalize_and_match_role(singular)
            print(f"[DEBUG] Fallback singular forename='{singular}', norm_fn={norm_fn!r}")
        if ln and not norm_ln and raw_ln_lower.endswith('en'):
            singular_ln = raw_ln_lower[:-2]
            norm_ln = normalize_and_match_role(singular_ln)
            print(f"[DEBUG] Fallback singular familyname='{singular_ln}', norm_ln={norm_ln!r}")

        # 1) reine Rolle im Vornamen
        if not rl and norm_fn:
            p.role      = norm_fn
            p.forename  = ""
            print(f"[DEBUG] Regel 1 angewendet: moved fn to role -> role='{p.role}'")

        # 2) reine Rolle im Nachnamen
        elif not rl and norm_ln:
            p.role        = norm_ln
            p.familyname  = ""
            print(f"[DEBUG] Regel 2 angewendet: moved ln to role -> role='{p.role}'")

        # 3) role enthält Namen, forename enthält Rolle → tauschen
        elif rl and norm_fn:
            old_role = p.role
            p.forename, p.role = rl, norm_fn
            print(f"[DEBUG] Regel 3 angewendet: swapped role and fn -> forename='{p.forename}', role='{p.role}' (old='{old_role}')")

        # 4) role enthält Namen, familyname enthält Rolle → tauschen
        elif rl and norm_ln:
            old_role = p.role
            p.familyname, p.role = rl, norm_ln
            print(f"[DEBUG] Regel 4 angewendet: swapped role and ln -> familyname='{p.familyname}', role='{p.role}' (old='{old_role}')")

        else:
            print("[DEBUG] Keine Regel angewendet.")

        # One final check after applying rules
        # Skip persons with only blacklisted terms after role processing
        if ((not p.forename or p.forename.lower() in NON_PERSON_TOKENS) and
            (not p.familyname or p.familyname.lower() in NON_PERSON_TOKENS) and
            getattr(p, "match_score", 0) < 40):
            print(f"[DEBUG] Person skipped after rule application: '{p.forename}'/'{p.familyname}' with match_score {getattr(p, 'match_score', 0)}")
            continue

        filtered.append(p)

    print(f"[DEBUG postprocess_roles] Nach erster Schleife: {len(filtered)} filtered")

    # 5) role_schema setzen und reine-Rollen-Einträge final bereinigen
    for idx, p in enumerate(filtered):
        has_name = bool(p.forename or p.familyname)
        has_id   = bool(p.nodegoat_id)
        print(f"[DEBUG] Finalizing[{idx}]: forename='{p.forename}', familyname='{p.familyname}', role='{p.role}', has_name={has_name}, has_id={has_id}")

        if p.role and not has_name and not has_id:
            normalized = normalize_and_match_role(p.role.lower()) or p.role
            p.role = normalized
            p.role_schema = map_role_to_schema_entry(normalized)
            p.forename = ""
            p.familyname = ""
            print(f"[DEBUG] Regel 5 angewendet: role_schema='{p.role_schema}', Namen geleert")
        else:
            if p.role and not getattr(p, "role_schema", None):
                p.role_schema = map_role_to_schema_entry(p.role)
                print(f"[DEBUG] role_schema gesetzt -> role_schema='{p.role_schema}'")

    base_doc.mentioned_persons = filtered
    print("[DEBUG postprocess_roles] Ende")