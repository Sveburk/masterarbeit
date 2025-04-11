
import re
from typing import List, Dict
from pprint import pprint
import xml.etree.ElementTree as ET

# Ground Truth Mapping laut CSV (nur deutsche Rollenbezeichnungen)
ROLE_MAPPINGS_DE = {
    "ehrenpräsident": "Ehrenpräsident",
    "ehrenmitglied": "Ehrenmitglied",
    "vorstand": "Vorstand",
    "schriftführer": "Schriftführer",
    "kassierer": "Kassierer",
    "sachwalter": "Sachwalter, Notenwart",
    "notenwart": "Sachwalter, Notenwart",
    "zweiter vorstand": "ZweiterVorstand",
    "dirigent": "Dirigent",
    "chorleiter": "Chorleiter",
    "ehrenführer": "Ehrenführer",
}

# Regex zur Rollenerkennung (Textrollen)
POSSIBLE_ROLES = list(set(ROLE_MAPPINGS_DE.keys()) | {
    "vereinsführer", "leiter", "obmann", "präsident"  # zusätzliche gängige Rollen
})

ROLE_ORG_REGEX = re.compile(
    r"(?P<name>[A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)?)\s*,?\s*(?P<role>" + "|".join(POSSIBLE_ROLES) + r")\s*(des|der|vom)?\s*(?P<organisation>[A-ZÄÖÜ][\w\s\-]+)?",
    re.IGNORECASE | re.UNICODE
)

def map_role_to_schema_entry(role_string: str) -> str:
    """
    Gibt die standardisierte schema:Role@de zurück, basierend auf der Ground Truth.
    Falls kein Mapping vorhanden, wird 'None' (String) zurückgegeben.
    """
    normalized = role_string.strip().lower()
    for key in ROLE_MAPPINGS_DE:
        if key in normalized:
            return ROLE_MAPPINGS_DE[key]
    return "None"

def assign_roles_to_known_persons(persons: List[Dict[str, str]], full_text: str) -> List[Dict[str, str]]:
    """
    Reiche Rollen und Organisationen für bekannte Personen anhand des Kontexts im Transkripttext an.
    Die Rolle bleibt im Original (wie im Text), zusätzlich wird ein schema:Role@de-Mapping gespeichert.

    Args:
        persons: Liste erkannter Personen
        full_text: Volltext der Seite zur Kontextanalyse

    Returns:
        Liste mit angereicherten Personen-Dictionaries
    """
    for match in ROLE_ORG_REGEX.finditer(full_text):
        name = match.group("name")
        raw_role = match.group("role")
        organisation = match.group("organisation") or ""

        name_parts = name.strip().split(" ")
        if len(name_parts) >= 2:
            forename_candidate = " ".join(name_parts[:-1])
            familyname_candidate = name_parts[-1]

            for person in persons:
                if (person.get("familyname") == familyname_candidate and
                    forename_candidate in person.get("forename", "")):
                    person["role"] = raw_role  # Original aus Text
                    person["role_schema"] = map_role_to_schema_entry(raw_role)
                    person["associated_organisation"] = organisation.strip()

    return persons

def main():
    xml_file = "0002_p002.xml"  # Pfad zu deiner XML-Datei
    full_text = extract_full_text_from_transkribus_xml(xml_file)

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
