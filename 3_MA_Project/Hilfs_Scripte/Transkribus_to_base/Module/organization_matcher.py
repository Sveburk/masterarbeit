import pandas as pd
from rapidfuzz import fuzz
from typing import List, Dict, Optional, Any
import re


def load_organizations_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """
    Lädt bekannte Organisationen aus einer CSV-Datei und gibt sie als Liste von Dicts zurück.
    CSV erwartet Spalten: Name, Alternativer Organisationsname, nodegoat ID, Typus, [hat Feldpostnummer] Feldpostnummer

    :param csv_path: Pfad zur CSV-Datei
    :return: Liste von Organisations-Einträgen
    """
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str).fillna("")

    organizations: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        name = row.get("Name", "").strip()
        alt_raw = row.get("Alternativer Organisationsname", "").strip()
        alt_names = [n.strip() for n in re.split(r"[;,]", alt_raw) if n.strip()]
        all_names = [name] + alt_names

        org_entry = {
            "name": name,
            "alternate_names": alt_names,
            "nodegoat_id": row.get("nodegoat ID", "").strip(),
            "type": row.get("Typus", "").strip(),
            "feldpostnummer": row.get("[hat Feldpostnummer] Feldpostnummer", "").strip(),
            "all_names": all_names
        }
        organizations.append(org_entry)
    return organizations


def normalize_org_name(name: str) -> str:
    """
    Entfernt Sonderzeichen, setzt auf lowercase und trimmt Leerzeichen.

    :param name: Roh-Name
    :return: Normalisierter Name
    """
    return re.sub(r"[^a-zA-Z0-9äöüÄÖÜß ]", "", name).lower().strip()


def match_organization(
    input_org: Dict[str, str],
    candidates: List[Dict[str, str]],
    threshold: int = 85
) -> (Optional[Dict[str, str]], int):
    """
    Fuzzy-Match einer Organisation gegen bekannte Kandidaten.

    :param input_org: {'name': org_name}
    :param candidates: Liste bekannter Organisationseinträge
    :param threshold: Mindestscore für Match
    :return: (best_match, score)
    """
    norm_input = normalize_org_name(input_org.get("name", ""))

    best_match: Optional[Dict[str, str]] = None
    best_score: int = 0

    for org in candidates:
        for cand in org.get("all_names", []):
            score = fuzz.ratio(norm_input, normalize_org_name(cand))
            if score > best_score and score >= threshold:
                best_score = score
                best_match = org

    return best_match, best_score


def match_organization_from_text(
    org_name: str,
    org_list: List[Dict[str, str]],
    threshold: int = 85
) -> Optional[Dict[str, str]]:
    """
    Wrapper: baut ein input_org und ruft match_organization auf.
    Spezialregel: Nur bei exakt 'Männerchor Murg' (direkt aufeinanderfolgend) wird auf diese feste Kombination gematcht.
    Zusätzlich: Falls der Roh-String einer nodegoat_id entspricht, wird direkt diese Organisation zurückgegeben.

    :param org_name: Roh-String aus dem XML oder Nodegoat-ID
    :param org_list: Liste geladener Organization-Einträge
    :param threshold: Schwellwert
    :return: Gematchter Organization-Eintrag oder None
    """
    raw = org_name.strip()

    # Nodegoat-ID-Fallback: wenn raw genau einer nodegoat_id entspricht
    for org in org_list:
        if raw and raw == org.get("nodegoat_id", ""):
            return org

    # Spezialregel für exakte Folge 'männerchor murg'
    norm = normalize_org_name(raw)
    if re.search(r"\bmännerchor\s+murg\b", norm):
        search_name = "Männerchor Murg"
    else:
        search_name = raw

    input_dict = {"name": search_name}
    match, score = match_organization(input_dict, org_list, threshold=threshold)
    return match


def match_organization_entities(
    raw_orgs: List[Dict[str, str]],
    org_list: List[Dict[str, str]],
    threshold: int = 85
) -> List[Dict[str, Any]]:
    """
    Verarbeitet die rohen Organisationseinträge aus extract_custom_attributes:
    - Fasst nur 'Männerchor' gefolgt von 'Murg' zu 'Männerchor Murg' zusammen
    - Führt Fuzzy-Matching durch
    - Gibt nur die gematchten Organisationen zurück, inkl. match_score & confidence
    """
    # 1) Roh-Namen extrahieren
    raw_names = [
        ent.get("original_input", ent.get("name", "")).strip()
        for ent in raw_orgs
    ]

    # 2) Kollabieren: nur 'Männerchor' gefolgt von 'Murg'
    collapsed: List[str] = []
    i = 0
    while i < len(raw_names):
        curr = raw_names[i]
        nxt = raw_names[i+1] if i+1 < len(raw_names) else None

        if curr == "Männerchor" and nxt == "Murg":
            collapsed.append("Männerchor Murg")
            i += 2
        else:
            collapsed.append(curr)
            i += 1

    # 3) Fuzzy-Matching für jeden Eintrag
    matched: List[Dict[str, Any]] = []
    for name in collapsed:
        # wir holen uns sowohl das Match-Objekt als auch den Score
        best_match, score = match_organization(
            {"name": name},
            org_list,
            threshold=threshold
        )
        if best_match:
            entry = {
                **best_match,        # name, type, nodegoat_id, alternate_names, feldpostnummer
                "match_score": score,
                "confidence": "fuzzy"
            }
            matched.append(entry)

    return matched
