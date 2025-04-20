# ================================================================
# Assigned_Roles_Module.py  
# ================================================================

import re
import pandas as pd
import xml.etree.ElementTree as ET  
from typing import List, Dict

# Pfad zur CSV mit Rollen-Mappings
CSV_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-roles.csv"

# Lade CSV und baue ROLE_MAPPINGS_DE dynamisch
_df = pd.read_csv(CSV_PATH, sep=";", dtype=str).fillna("")
ROLE_MAPPINGS_DE: Dict[str, str] = {}
for _, row in _df.iterrows():
    schema_role = row["Rollenname"].strip()
    # kanonischer Name selbst
    key = schema_role.lower()
    ROLE_MAPPINGS_DE[key] = schema_role
    # alternative Bezeichnungen (falls vorhanden, durch Komma getrennt)
    alt = row.get("Alternativer Rollenname", "").strip()
    for alt_name in alt.split(","):
        alt_name = alt_name.strip()
        if alt_name:
            ROLE_MAPPINGS_DE[alt_name.lower()] = schema_role

# Basis‑Vokabular: alle in ROLE_MAPPINGS_DE enthaltenen Keys
POSSIBLE_ROLES: List[str] = list(ROLE_MAPPINGS_DE.keys())

# Regex 1: „Name, Rolle … Organisation“
ROLE_AFTER_NAME_RE = re.compile(
    rf"(?P<name>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)\s*,?\s*"
    rf"(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})\s*(des|der|vom)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?",
    re.IGNORECASE | re.UNICODE
)

# Regex 2: „Rolle … Name“
ROLE_BEFORE_NAME_RE = re.compile(
    rf"(?P<role>{'|'.join(map(re.escape, POSSIBLE_ROLES))})\s+(des|der|vom)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?\s+"
    rf"(?P<name>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)",
    re.IGNORECASE | re.UNICODE
)

def map_role_to_schema_entry(role_string: str) -> str:
    norm = role_string.strip().lower()
    return ROLE_MAPPINGS_DE.get(norm, "None")

def assign_roles_to_known_persons(persons: List[Dict[str, str]],
                                  full_text: str) -> List[Dict[str, str]]:
    for regex in (ROLE_AFTER_NAME_RE, ROLE_BEFORE_NAME_RE):
        for match in regex.finditer(full_text):
            name = match.group("name") or ""
            role = match.group("role")
            org  = (match.group("organisation") or "").strip()

            fn_parts = name.strip().split()
            if len(fn_parts) < 2:
                continue
            fn_candidate = " ".join(fn_parts[:-1])
            ln_candidate = fn_parts[-1]

            for p in persons:
                if (p.get("familyname") == ln_candidate and
                        fn_candidate in p.get("forename", "")):
                    p["role"]                   = role
                    p["role_schema"]            = map_role_to_schema_entry(role)
                    p["associated_organisation"] = org
    return persons



def main():
    xml_file = "0002_p002.xml"  # Pfad zu deiner XML-Datei
    root = ET.parse(xml_path).getroot()
    full_text = extract_text_from_xml(root)

    persons = [
        {"forename": "Alfons", "familyname": "Zimmermann", "role": "", "associated_organisation": ""},
        {"forename": "Otto", "familyname": "Bollinger", "role": "", "associated_organisation": ""}
    ]

    enriched = assign_roles_to_known_persons(persons, full_text)
    pprint(enriched)

if __name__ == "__main__":
    main()


def main():
    # Simulierte Liste aus einer XML-Extraktion
    persons = [
        {"forename": "Alfons", "familyname": "Zimmermann", "role": "", "associated_organisation": ""},
        {"forename": "Otto", "familyname": "Bollinger", "role": "", "associated_organisation": ""}
    ]

    # Inhalt aus dem content_transcription-Feld eines Transkribus-Dokuments
    full_text = """München 28.V.1941
Lieber Otto!
Nur wer die Sehnsucht kennt weiß was ich leide
Ich wandle traurig her in schwarzer Seide.
die Sehnsucht brennt du bist so fern
Ach lieber Otto wie hab ich dich gern.
Ich schnitt es gern in alle Rinden
Ach Otto wann u. wo kann ich dich finden?
Deine Dich nie vergessende
Lina Fingerdick
An 
Herrn
Otto Bollinger
z.H.d Herrn Alfons Zimmermann
Vereinsführer des Männerchor
Murg
Laufenburg (Baden)
Rhina"""

    result = assign_roles_to_known_persons(persons, full_text)
    pprint(result)

if __name__ == "__main__":
    main()
