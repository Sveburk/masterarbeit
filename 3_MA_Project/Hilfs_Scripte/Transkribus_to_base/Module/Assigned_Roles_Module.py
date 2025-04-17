# ================================================================
# Assigned_Roles_Module.py  
# ================================================================

import re
from typing import List, Dict

# Ground‑Truth‑Mapping  ➜  schema:Role@de
ROLE_MAPPINGS_DE = {
    "ehrenpräsident": "Ehrenpräsident",
    "ehrenmitglied":  "Ehrenmitglied",
    "vorstand":       "Vorstand",
    "schriftführer":  "Schriftführer",
    "kassierer":      "Kassierer",
    "sachwalter":     "Sachwalter, Notenwart",
    "notenwart":      "Sachwalter, Notenwart",
    "zweiter vorstand": "ZweiterVorstand",
    "dirigent":       "Dirigent",
    "chorleiter":     "Chorleiter",
    "ehrenführer":    "Ehrenführer",
}

# Basis‑Wortschatz für Rollen
POSSIBLE_ROLES = list(set(ROLE_MAPPINGS_DE.keys()) | {
    "vereinsführer", "leiter", "obmann", "präsident"
})

# 1)  «Name,  Rolle …»      z.B.  „Alfons Zimmermann, Vereinsführer des Männerchor“
ROLE_AFTER_NAME_RE = re.compile(
    rf"(?P<name>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)\s*,?\s*"
    rf"(?P<role>{'|'.join(POSSIBLE_ROLES)})\s*(des|der|vom)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?",
    re.IGNORECASE | re.UNICODE
)

# 2)  «Rolle  … Name»       z.B.  „Vereinsführer des Männerchor Alfons Zimmermann“
ROLE_BEFORE_NAME_RE = re.compile(
    rf"(?P<role>{'|'.join(POSSIBLE_ROLES)})\s+(des|der|vom)?\s*"
    rf"(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?\s+"
    rf"(?P<name>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)",
    re.IGNORECASE | re.UNICODE
)

def map_role_to_schema_entry(role_string: str) -> str:
    norm = role_string.strip().lower()
    for key, mapped in ROLE_MAPPINGS_DE.items():
        if key in norm:
            return mapped
    return "None"

def assign_roles_to_known_persons(persons: List[Dict[str, str]],
                                  full_text: str) -> List[Dict[str, str]]:
    """
    Ergänzt  role, role_schema, associated_organisation  in den Personen‑Dicts.
    """
    for regex in (ROLE_AFTER_NAME_RE, ROLE_BEFORE_NAME_RE):
        for match in regex.finditer(full_text):
            name  = match.group("name") or ""
            role  = match.group("role")
            org   = (match.group("organisation") or "").strip()

            # Name in Vor‑/Nachname zerlegen
            fn_parts = name.strip().split()
            if len(fn_parts) < 2:
                continue
            fn_candidate  = " ".join(fn_parts[:-1])
            ln_candidate  = fn_parts[-1]

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
