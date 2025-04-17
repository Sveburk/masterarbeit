
import pandas as pd
from rapidfuzz import fuzz
from typing import List, Dict, Optional
import re


def load_organizations_from_csv(csv_path: str) -> List[Dict[str, str]]:
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str).fillna("")

    organizations = []
    for _, row in df.iterrows():
        name = row.get("Name", "").strip()
        alt_names_raw = row.get("Alternativer Organisationsname", "").strip()
        alt_names = [n.strip() for n in re.split(r"[;,]", alt_names_raw) if n.strip()]
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
    return re.sub(r"[^a-zA-Z0-9äöüÄÖÜß ]", "", name).lower().strip()


def match_organization(input_org: Dict[str, str], candidates: List[Dict[str, str]], threshold: int = 85) -> (Optional[Dict[str, str]], int):
    """
    Vergleicht eine Organisation gegen eine Liste bekannter Organisationen.
    Gibt das beste Match und den Score zurück.
    """
    input_name = input_org.get("name", "")
    norm_input = normalize_org_name(input_name)

    best_match = None
    best_score = 0

    for org in candidates:
        for cand_name in org.get("all_names", []):
            score = fuzz.ratio(norm_input, normalize_org_name(cand_name))
            if score > best_score and score >= threshold:
                best_score = score
                best_match = org

    return best_match, best_score


def match_organization_from_text(org_name: str, org_list: List[Dict[str, str]], threshold: int = 85) -> Optional[Dict[str, str]]:
    """
    Wrapper für Textinput – baut ein input_org-Dict und ruft das Matcher-Backend auf.
    """
    input_dict = {"name": org_name}
    match, score = match_organization(input_dict, org_list, threshold=threshold)
    return match
