import re
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
from .document_schemas import BaseDocument, Person
from .Assigned_Roles_Module import normalize_and_match_role, map_role_to_schema_entry, ROLE_MAPPINGS_DE, KNOWN_ROLE_LIST

import json
from pathlib import Path
from .person_matcher import match_person, KNOWN_PERSONS, get_matching_thresholds, assess_llm_entry_score
from .Assigned_Roles_Module import(assign_roles_to_known_persons, map_role_to_schema_entry, normalize_and_match_role)
from Module.place_matcher import PlaceMatcher, consolidate_places
from .date_matcher import extract_custom_date


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


def extract_recipients_raw(text: str) -> List[Dict[str, Any]]:
        """
        Sammelt alle potenziellen Empfänger aus dem Briefkopf (mehrere Zeilen analysieren),
        vergibt recipient_score (z. B. 90 für vollständig, 70 für Zweizeiler usw.),
        und gibt eine Liste zurück.
        """
        parts = re.split(r"\n\s*\n", text, maxsplit=1)
        header_block = parts[0]
        lines = header_block.splitlines()

        recipients = []

        # 1) Einzeiler: z. B. "An Herrn Otto Bollinger"
        inline_re = re.compile(
            r"""^(?:An\s+)?(Herrn?|Frau)\s+([A-ZÄÖÜ][\wäöüß]+)(?:\s+([A-ZÄÖÜ][\wäöüß]+(?:\s+[A-ZÄÖÜ][\wäöüß]+)*))?""",
            re.IGNORECASE | re.VERBOSE,
        )
        for line in lines[:5]:
            m = inline_re.search(line.strip())
            if m:
                recipients.append({
                    "anrede": m.group(1),
                    "forename": m.group(2),
                    "familyname": m.group(3) or "",
                    "role": "",
                    "recipient_score": 90,
                    "confidence": "header-inline"
                })

        # 2) Dreizeiler: An\nHerrn\nOtto Bollinger
        for i, line in enumerate(lines[:5]):
            if line.strip().lower() == "an" and i + 2 < len(lines):
                honorific = lines[i + 1].strip()
                name_line = lines[i + 2].strip()
                if re.match(r"^(Herrn?|Frau)$", honorific, re.IGNORECASE):
                    parts = name_line.split()
                    if parts:
                        recipients.append({
                            "anrede": honorific,
                            "forename": parts[0],
                            "familyname": parts[1] if len(parts) > 1 else "",
                            "role": "",
                            "recipient_score": 80,
                            "confidence": "header-3line"
                        })

        # 3) Zweizeiler: "Herrn" in einer Zeile, Name darunter
        honorific_re = re.compile(r"^(Herrn?|Frau)\s*$", re.IGNORECASE)
        for i, line in enumerate(lines[:5]):
            if honorific_re.match(line.strip()) and i + 1 < len(lines):
                name_line = lines[i + 1].strip().rstrip(".,;:")
                parts = name_line.split()
                if parts:
                    recipients.append({
                        "anrede": line.strip(),
                        "forename": parts[0],
                        "familyname": parts[1] if len(parts) > 1 else "",
                        "role": "",
                        "recipient_score": 70,
                        "confidence": "header-2line"
                    })

        return recipients

def letter_match_and_enrich(raw: Dict[str, str], text: str, mentioned_persons: List[Person] = None) -> Optional[Dict[str, Any]]:
    
    if raw.get("forename") and not raw.get("familyname"):
        for p in mentioned_persons:
            if (
                p.forename == raw["forename"]
                and p.familyname  # Nur wenn Nachname vorhanden
            ):
                print(f"[DEBUG] Verwende vollständigere Person statt unvollständigem Recipient: {p.forename} {p.familyname}")
                enriched = p.to_dict()
                for k, v in raw.items():
                    if not enriched.get(k) and v:
                        enriched[k] = v
                return enriched

    if mentioned_persons is None:
        mentioned_persons = []

    # Process role if it exists in raw data using CSV lookup
    role_raw = raw.get("role", "").strip()
    role_schema = ""
    if role_raw:
        normalized_role = normalize_and_match_role(role_raw)
        if normalized_role:
            role_schema = map_role_to_schema_entry(normalized_role)
            raw["role"] = normalized_role
            raw["role_schema"] = role_schema
            print(f"[DEBUG] Rolle normalisiert: '{role_raw}' → '{normalized_role}' (Schema: {role_schema})")

    # --- 1) Schon in mentioned_persons vorhanden? ---
    for p in mentioned_persons:
        if p.forename == raw.get("forename") and p.familyname == raw.get("familyname"):
            print(f"[DEBUG-RECIPIENT] matched in mentioned_persons: {p.forename} {p.familyname}")
            enriched = p.to_dict()
            enriched["confidence"] = "LLM_Tag_match_in_mentioned_persons"
            enriched["match_score"] = 100

            for k, v in raw.items():
                if not enriched.get(k) and v:
                    enriched[k] = v

            if enriched.get("role") and not enriched.get("role_schema"):
                enriched["role_schema"] = map_role_to_schema_entry(enriched["role"])
                print(f"[DEBUG-RECIPIENT] role_schema ergänzt aus Rolle: {enriched['role']} → {enriched['role_schema']}")
            return enriched

    # --- 2) Fuzzy-Match gegen known_persons ---
    person_query = {"forename": raw.get("forename", ""), "familyname": raw.get("familyname", "")}
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

    # --- 3) Kein Match: Fallback mit Scoring ---
    score, confidence, needs_review, review_reason = assess_llm_entry_score(
        raw.get("forename", ""),
        raw.get("familyname", ""),
        raw.get("role", "")
    )

    result = {
        **raw,
        "nodegoat_id": "",
        "associated_place": "",
        "associated_organisation": "",
        "match_score": score,
        "confidence": confidence,
        "needs_review": needs_review,
        "review_reason": review_reason,
        "role_schema": role_schema
    }

    # Ergänze leere Schlüssel zur Sicherheit
    for key in ["anrede", "forename", "familyname", "role", "closing"]:
        result.setdefault(key, "")

    # Versuche nochmal Rollenextraktion
    enriched_results = assign_roles_to_known_persons([result], text)
    if not enriched_results:
        return result  # keine Anreicherung möglich – gib Original zurück
    enriched_role = enriched_results[0]
    if isinstance(enriched_role, Person):
        enriched_role = enriched_role.to_dict()

    if enriched_role.get("role"):
        result["role"] = enriched_role["role"]
        result["role_schema"] = enriched_role.get("role_schema", "")
        print(f"[DEBUG] Rolle (nachträglich) erkannt im Fallback: {result['role']} ({result['role_schema']})")

    # Nur zurückgeben, wenn wenigstens ein Namensbestandteil vorhanden ist
    if result.get("forename") or result.get("familyname"):
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

    # --- 1) Mehrere potenzielle Kopf-Empfänger extrahieren ---
    raw_head_recipients = extract_recipients_raw(text)
    enriched_head = []
    for raw in raw_head_recipients:
        enriched = letter_match_and_enrich(raw, text, mentioned_persons)
        if enriched:
            enriched["recipient_score"] = raw.get("recipient_score", 50)
            enriched["confidence"] = raw.get("confidence", "header")
            enriched_head.append(enriched)

    # ⬇️ Wähle den vollständigsten mit Vor- und Nachname
    def score_completeness(p: Dict[str, Any]) -> int:
        return (1 if p.get("forename") else 0) + (2 if p.get("familyname") else 0)

    enriched_head.sort(key=score_completeness, reverse=True)
    all_recipients = [enriched_head[0]] if enriched_head else []
    # --- 1) Klassischer Kopf-Empfänger (z. B. "An Herrn Otto Bolliger") ---
    raw_head_recipients = extract_recipients_raw(text)
    for raw in raw_head_recipients:
        enriched = letter_match_and_enrich(raw, text, mentioned_persons)
        if enriched:
            enriched["recipient_score"] = raw.get("recipient_score", 90)
            enriched["confidence"] = raw.get("confidence", "header")
            all_recipients.append(enriched)

    # --- 2) Weitere mögliche Empfänger (z. B. Anredeformen) ---
    additional_raw = extract_multiple_recipients_raw(text)
    enriched_list = []
    for raw in additional_raw:
        enriched = letter_match_and_enrich(raw, text, mentioned_persons)
        if enriched:
            enriched["recipient_score"] = raw.get("recipient_score", 50)
            enriched["confidence"] = raw.get("confidence", "indirect")
            enriched_list.append(enriched)

    all_recipients.extend(enriched_list)

    # --- 3) Deduplizieren (nach forename + familyname) ---
    seen = set()
    grouped = defaultdict(list)
    for rec in all_recipients:
        key = rec.get("forename", "").strip().lower()
        if not key:
            continue
        grouped[key].append(rec)

    final = []
    for forename, candidates in grouped.items():
        # Wähle den mit den meisten Namensbestandteilen
        # Sort by name completeness first, then by recipient_score
        candidates.sort(key=lambda x: (
            (1 if x.get("forename") else 0) + (2 if x.get("familyname") else 0),
            x.get("recipient_score", 0) or 0
        ), reverse=True)
        
        best = candidates[0]
        print(f"[DEBUG-FINAL] Selected best recipient: {best.get('forename')} {best.get('familyname')} with score={best.get('recipient_score', 0)}")
        final.append(best)
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

def deduplicate_recipients(recipients: List[Person]) -> List[Person]:
    deduped = []
    seen = {}

    for rec in recipients:
        key = (rec.forename.strip(), rec.familyname.strip(), rec.nodegoat_id)

        if key in seen:
            existing = seen[key]

            # Kombiniere recipient_score
            existing_score = getattr(existing, "recipient_score", 0) or 0
            new_score = getattr(rec, "recipient_score", 0) or 0
            
            print(f"[DEBUG-DEDUP] Combining recipient_scores: existing={existing_score}, new={new_score}")
            
            if new_score > existing_score:
                existing.recipient_score = new_score
                existing.confidence = rec.confidence or existing.confidence
                print(f"[DEBUG-DEDUP] Updated recipient_score to {existing.recipient_score}")
            continue
        else:
            seen[key] = rec
            deduped.append(rec)
            print(f"[DEBUG-DEDUP] Added new recipient with score={getattr(rec, 'recipient_score', 0)}: {rec.forename} {rec.familyname}")

    return deduped





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
                person.match_score = 100 if matched.nodegoat_id else 20
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

                    score, confidence, needs_review, review_reason = assess_llm_entry_score(
                        entry.get("forename", ""),
                        entry.get("familyname", ""),
                        entry.get("role", "")
                    )

                    person = Person(
                        forename=entry.get("forename", ""),
                        familyname=entry.get("familyname", ""),
                        title="",
                        role=entry.get("role", ""),
                        role_schema=role_schema,
                        associated_place="",
                        associated_organisation="",
                        nodegoat_id="",
                        match_score=score,
                        confidence=confidence,
                        needs_review=needs_review,
                        review_reason=review_reason
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
            (ln.lower() in NON_PERSON_TOKENS and not fn)):
            if not (p.match_score >= 50 or p.recipient_score > 0 or p.nodegoat_id):
                print(f"[DEBUG] Person[{idx}] skipped due to blacklist: '{fn}'/'{ln}' with match_score={p.match_score}, recipient_score={p.recipient_score}")
                continue

        # Skip unmatchable single names with low match score
        if ((fn.lower() in UNMATCHABLE_SINGLE_NAMES and not ln) or
            (ln.lower() in UNMATCHABLE_SINGLE_NAMES and not fn)) and getattr(p, "match_score", 0) < 40:
            print(f"[DEBUG] Person[{idx}] skipped due to unmatchable name: '{fn}'/'{ln}' with match_score {getattr(p, 'match_score', 0)}")
            continue

        # Rohes Lowercase
        raw_fn_lower = fn.lower()
        raw_ln_lower = ln.lower()


        # Normierungen
        norm_fn = normalize_and_match_role(raw_fn_lower) if fn else None
        norm_ln = normalize_and_match_role(raw_ln_lower) if ln else None


        # Fallback: Singularform für deutsche Plural "-en"
        if fn and not norm_fn and raw_fn_lower.endswith('en'):
            singular = raw_fn_lower[:-2]
            norm_fn = normalize_and_match_role(singular)

        if ln and not norm_ln and raw_ln_lower.endswith('en'):
            singular_ln = raw_ln_lower[:-2]
            norm_ln = normalize_and_match_role(singular_ln)


        # 1) reine Rolle im Vornamen
        if not rl and norm_fn:
            p.role      = norm_fn
            p.forename  = ""


        # 2) reine Rolle im Nachnamen
        elif not rl and norm_ln:
            p.role        = norm_ln
            p.familyname  = ""


        # 3) role enthält Namen, forename enthält Rolle → tauschen
        elif rl and norm_fn:
            old_role = p.role
            p.forename, p.role = rl, norm_fn


        # 4) role enthält Namen, familyname enthält Rolle → tauschen
        elif rl and norm_ln:
            old_role = p.role
            p.familyname, p.role = rl, norm_ln

        # Nach Regelanwendung: Dropping nur, wenn keine Rolle übrig und keine ID, kein Score
        if ((not p.forename or p.forename.lower() in NON_PERSON_TOKENS) and
            (not p.familyname or p.familyname.lower() in NON_PERSON_TOKENS) and
            not p.role and not p.role_schema and
            p.match_score < 40 and p.recipient_score == 0 and not p.nodegoat_id):
            print(f"[DEBUG] Person skipped after rule application: '{p.forename}'/'{p.familyname}' ohne Rolle, ohne ID")
            continue

        filtered.append(p)



    # 5) role_schema setzen und reine-Rollen-Einträge final bereinigen
    for idx, p in enumerate(filtered):
        has_name = bool(p.forename or p.familyname)
        has_id   = bool(p.nodegoat_id)
        

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


# =================================================
# Orte und Daten
# =================================================
#Liste ändern nach Org_matcher:
ORG_KEYWORDS = {"Männerchor", "Verein", "Schule", "Chor", "Club"}


# =================================================
# Orte und Daten
# =================================================

def extract_places_and_date(
    xml_root: ET.Element
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Liest aus dem PAGE-XML alle custom-Tags
    für creation_place, recipient_place und creation_date
    und gibt zurück: (raw_creation_place, raw_recipient_place, creation_date).
    Falls keine Custom-Tags gefunden werden, gibt es Fallback via Header-Regex.
    """
    ns = {"pc": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}
    raw_creation = raw_recipient = creation_date = None

    # 1) Custom-Tags
    for tl in xml_root.findall(".//pc:TextLine", namespaces=ns):
        custom = tl.attrib.get("custom", "")
        text = (tl.find(".//pc:Unicode", namespaces=ns).text or "").strip()

        if "creation_place" in custom:
            m = re.search(r"creation_place\s*\{\s*offset\s*:\s*(\d+);\s*length\s*:\s*(\d+)", custom)
            raw_creation = (
                text[int(m.group(1)):int(m.group(1))+int(m.group(2))].strip().rstrip(".,")
                if m else text.split()[0].rstrip(".,")
            )

        if "recipient_place" in custom:
            m = re.search(r"recipient_place\s*\{\s*offset\s*:\s*(\d+);\s*length\s*:\s*(\d+)", custom)
            raw_recipient = (
                text[int(m.group(1)):int(m.group(1))+int(m.group(2))].strip().rstrip(".,")
                if m else text.split()[0].rstrip(".,")
            )

        if "creation_date" in custom or "date" in custom:
            m = re.search(r"(?:creation_date|date)\s*\{[^}]*when\s*:\s*([^;]+)", custom)
            if m:
                creation_date = m.group(1).strip()

        if raw_creation and raw_recipient and creation_date:
            break

    # 2) Fallback über Header (falls nötig)
    if not (raw_creation and creation_date) or not raw_recipient:
        header_lines = []
        for tl in xml_root.findall(".//pc:TextLine", namespaces=ns):
            txt = (tl.find(".//pc:Unicode", namespaces=ns).text or "").strip()
            if txt:
                header_lines.append(txt)
            if len(header_lines) >= 5:
                break
        header = "\n".join(header_lines)

        if not (raw_creation and creation_date):
            m = re.search(
                r"^(?P<place>[A-ZÄÖÜ][\w\s\-/]+?),\s*(?:den\s*)?(?P<date>\d{1,2}\.\d{1,2}\.\d{2,4})",
                header, flags=re.MULTILINE
            )
            if m:
                raw_creation  = raw_creation  or m.group("place").strip()
                creation_date = creation_date or m.group("date").strip()

        if not raw_recipient:
            m2 = re.search(
                r"\bAn\s+(?:Herrn?|Frau|Frl\.?)\s+[A-ZÄÖÜ][\w\.\s-]+?\s+in\s+(?P<place>[A-ZÄÖÜ][\w\s\-/]+)",
                header
            )
            if m2:
                raw_recipient = m2.group("place").strip()

    return raw_creation, raw_recipient, creation_date


def assign_sender_and_recipient_place(    xml_root: ET.Element,
    matcher: PlaceMatcher,
    mentioned_places: List[Any],
    allow_multiple: bool = True,
    score_threshold: float = 80.0
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    raw_creation, _, creation_date = extract_places_and_date(xml_root)

    ns = {"pc": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}
    lines = [
        (tl.attrib.get("id", ""), (tl.find(".//pc:Unicode", namespaces=ns).text or "").strip())
        for tl in xml_root.findall(".//pc:TextLine", namespaces=ns)
    ]

    all_line_texts = " ".join(text for _, text in lines)

    def find_combined_place(line_texts: List[str], mentioned_places: List[Any]) -> Optional[Dict[str, Any]]:
        """
        Sucht in benachbarten Zeilen nach Ortsnamen, die in Kombination in den Alt-Namen eines Groundtruth-Ortes vorkommen.
        Gibt den vollständigeren Ortseintrag zurück, falls erkannt, und vergibt einen Bonus.
        """
        text_by_line = {i: line for i, line in enumerate(line_texts)}

        for i, line in text_by_line.items():
            for j in [i-1, i+1]:  # Vor- und Nachzeile prüfen
                if j not in text_by_line:
                    continue

                combined_line = f"{line} {text_by_line[j]}"

                for place in mentioned_places:
                    alt_names = getattr(place, "alt_names", []) or getattr(place, "alternate_place_name", "")
                    if isinstance(alt_names, str):
                        alt_names = [alt.strip() for alt in alt_names.split(";")]

                    for alt in alt_names:
                        if "-" not in alt:
                            continue

                        # Teile aufsplitten: z. B. "Laufenburg-Rhina"
                        alt_parts = re.split(r"[-–—\s]", alt)
                        if len(alt_parts) < 2:
                            continue

                        # Check: Sind alle Bestandteile in der kombinierten Zeile vorhanden (unscharf)
                        if all(any(re.search(rf"\b{re.escape(part)}\b", combined_line, flags=re.IGNORECASE) for part in alt_parts) for part in alt_parts):
                            print(f"[DEBUG] Kombinierter Ort erkannt in Kontext: {alt} in Zeilen {i}/{j}")
                            return {
                                "name": place.name,
                                "nodegoat_id": place.nodegoat_id,
                                "score": 100.0 + 5.0,  # Bonus für kombinierte Erkennung
                                "alternate_place_name": ";".join(alt_names)
                            }

        return None

    def enrich_place_candidate(candidate_raw: str) -> Optional[Dict[str, Any]]:
        for place in mentioned_places:
            alt = getattr(place, "alt_names", []) or getattr(place, "alternative_names", [])
            if candidate_raw == place.name or candidate_raw in alt:
                return {"name": place.name, "nodegoat_id": place.nodegoat_id, "score": 100.0}

        fuzzy = matcher.match_place(candidate_raw)
        if not fuzzy:
            return None
        best = max(fuzzy, key=lambda m: float(m.get("match_score", 0)))
        data = dict(best["data"])
        score = float(best.get("match_score", 0))

        # Penalty
        penalty = 0.0
        if any(org in all_line_texts for org in ORG_KEYWORDS):
            if candidate_raw in all_line_texts:
                penalty = 5.0
        final_score = max(0.0, score - penalty)

        return {
            "name": data["name"],
            "nodegoat_id": data.get("nodegoat_id", ""),
            "score": final_score
        }

    # Alle Ortskandidaten aus dem gesamten XML
    candidates = []

    # 1) Normaler Kandidatenscan
    for _, line in lines:
        for place in mentioned_places:
            if place.name in line or any(alt in line for alt in getattr(place, "alt_names", [])):
                result = enrich_place_candidate(place.name)
                if result and result["score"] >= score_threshold:
                    candidates.append(result)

    # 2) Ergänze kombinierte Orte (z. B. "Laufenburg-Rhina")
    combined = find_combined_place([text for _, text in lines], mentioned_places)
    if combined and combined["score"] >= score_threshold:
        # Überschreibe bestehenden, wenn name bereits als Teil vorhanden ist
        already = [c for c in candidates if c["name"] in combined["name"] or combined["name"] in c["name"]]
        if already:
            # ersetze ggf. einfachsten durch vollständigen
            for old in already:
                candidates.remove(old)
        candidates.append(combined)

    # Duplikate entfernen (nach Name)
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c["name"] not in seen:
            unique_candidates.append(c)
            seen.add(c["name"])

    # ✏️ Creation_place nur EIN Eintrag
    creation_match = enrich_place_candidate(raw_creation) if raw_creation else None
    if creation_match and creation_match["score"] < matcher.threshold:
        creation_match = None

    return creation_match, unique_candidates if allow_multiple else unique_candidates[:1], creation_date


def finalize_recipient_places(
    recipient_places: List[Dict[str, any]]
) -> Tuple[Optional[Dict[str, any]], List[Dict[str, any]]]:
    """
    Gibt den besten Ort (nach Score) als primären recipient_place zurück,
    alle weiteren mit needs_review = True als alternative Orte.
    """
    if not recipient_places:
        return None, []

    sorted_places = sorted(recipient_places, key=lambda x: (-x["score"], x["name"]))
    main = sorted_places[0]
    alts = sorted_places[1:]

    for alt in alts:
        alt["needs_review"] = True

    return main, alts