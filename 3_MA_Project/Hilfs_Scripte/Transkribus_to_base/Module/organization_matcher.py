import os
import re
import pandas as pd
from rapidfuzz import fuzz
from typing import List, Dict, Optional, Any

# ----------------------------------------------------------------------------
# Organization Extraction Helper
# ----------------------------------------------------------------------------
_WIKIDATA_RE = re.compile(r'wikiData:(Q\d+)', re.IGNORECASE)

# matches and strips enclosing parentheses/brackets
_ENCLOSING_BRACKETS = re.compile(r'^[\(\[\{]\s*(.*?)\s*[\)\]\}]$')
# removes any stray parentheses, brackets, or colons inside
_CLEAN_INSIDE = re.compile(r'[()\[\]\{\}:]')
# blacklist of tokens to drop
NON_ORG_TOKENS = {"verein", "partei", "amt", "lokal", "hotel", "süd", "krone"}


def extract_organization(org_raw: str) -> Optional[str]:
    """
    1) Entfernt umgebende Klammern/Brackets.
    2) Löscht innenliegende Klammern, Brackets und Doppelpunkte.
    3) Trimmt führende/trailinge Satzzeichen.
    4) Verwirft Blacklist-Tokens.
    """
    text = org_raw.strip()
    # 1) strip enclosing
    m = _ENCLOSING_BRACKETS.match(text)
    if m:
        text = m.group(1).strip()
    # 2) clean inside
    text = _CLEAN_INSIDE.sub("", text).strip()
    # 3) strip punctuation
    text = text.strip(".,;")
    # 4) drop blacklist or empty
    if not text or text.lower() in NON_ORG_TOKENS:
        return None
    return text


# ----------------------------------------------------------------------------
# Load & Normalize
# ----------------------------------------------------------------------------

def load_organizations_from_csv(csv_path: str) -> List[Dict[str, str]]:
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str).fillna("")
    organizations: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        name = row.get("Name", "").strip()
        alt_raw = row.get("Alternativer Organisationsname", "").strip()
        alt_names = [n.strip() for n in re.split(r"[;,]", alt_raw) if n.strip()]
        all_names = [name] + alt_names
        organizations.append({
            "name": name,
            "alternate_names": alt_names,
            "nodegoat_id": row.get("nodegoat ID", "").strip(),
            "type": row.get("Typus", "").strip(),
            "feldpostnummer": row.get("[hat Feldpostnummer] Feldpostnummer", "").strip(),
            "all_names": all_names
        })
    return organizations


def normalize_org_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9äöüÄÖÜß ]", "", name).lower().strip()

def extract_wikidata_id(custom_str: str) -> Optional[str]:
    m = _WIKIDATA_RE.search(custom_str)
    return m.group(1) if m else None

# ----------------------------------------------------------------------------
# Fuzzy-Matching
# ----------------------------------------------------------------------------

def match_organization(
    input_org: Dict[str, str],
    candidates: List[Dict[str, str]],
    threshold: int = 85
) -> (Optional[Dict[str, str]], int):
    norm_input = normalize_org_name(input_org.get("name", ""))
    best_match, best_score = None, 0
    for org in candidates:
        for cand in org.get("all_names", []):
            score = fuzz.ratio(norm_input, normalize_org_name(cand))
            if score > best_score and score >= threshold:
                best_match, best_score = org, score
    return best_match, best_score


def match_organization_from_text(
    transcript_text: str,
    org_list: List[Dict[str, str]],
    threshold: int = 85
) -> List[Dict[str, str]]:
    """
    Durchsucht den gesamten Text auf bekannte Organisationsnamen (oder fuzzy Varianten)
    und gibt eine Liste von org_list-Einträgen zurück, die mindestens threshold erreichen.
    """
    found = []
    norm_text = normalize_org_name(transcript_text.lower())

    for org in org_list:
        name = org.get("name", "")
        norm_name = normalize_org_name(name.lower())
        # exakter Substring-Check
        if norm_name and norm_name in norm_text:
            found.append(org)
            continue
        # fuzzy match gegen den ganzen Text
        score = fuzz.partial_ratio(norm_name, norm_text)
        if score >= threshold:
            found.append(org)

    # Duplikate entfernen, falls nötig
    unique = {o["nodegoat_id"]: o for o in found}
    return list(unique.values())

def match_organization_entities(
    raw_orgs: List[Dict[str, str]],
    org_list: List[Dict[str, str]],
    threshold: int = 85
) -> List[Dict[str, Any]]:
    # apply cleaning and extract non-null
    cleaned_names = []
    for ent in raw_orgs:
        orig = ent.get("original_input", ent.get("name", ""))
        clean = extract_organization(orig)
        if clean:
            cleaned_names.append(clean)
    # collapse Männerchor + Murg
    collapsed, i = [], 0
    while i < len(cleaned_names):
        curr = cleaned_names[i]
        nxt = cleaned_names[i+1] if i+1 < len(cleaned_names) else None
        if curr == "Männerchor" and nxt == "Murg":
            collapsed.append("Männerchor Murg")
            i += 2
        else:
            collapsed.append(curr)
            i += 1
    # fuzzy match
    matched = []
    for name in collapsed:
        best_match, score = match_organization({"name": name}, org_list, threshold)
        if best_match:
            matched.append({**best_match, "match_score": score, "confidence": "fuzzy"})
    return matched

# Optional: anpassen oder dynamisch setzen, falls du einen Konfigurationspfad nutzt
CSV_ORG_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-organisationen.csv"

# Globale Liste aller bekannten Organisationen
try:
    KNOWN_ORGS = load_organizations_from_csv(CSV_ORG_PATH)
except Exception as e:
    KNOWN_ORGS = []
    print(f"[WARN] Konnte export-organisationen.csv nicht laden: {e}")

def match_organization_by_name(name: str, threshold: int = 85) -> Optional[Dict[str, str]]:
    """
    Führt ein Fuzzy-Matching gegen alle bekannten Organisationen durch.
    Gibt das beste Match zurück (inkl. Name, nodegoat_id etc.), wenn Score ≥ threshold.
    """
    clean = extract_organization(name)
    if not clean:
        return None
    match, score = match_organization({"name": clean}, KNOWN_ORGS, threshold)
    return match
