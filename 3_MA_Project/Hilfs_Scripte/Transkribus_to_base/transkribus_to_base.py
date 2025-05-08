"""
Extraktion von Basisinformationen aus Transkribus XML‑Dateien und Konvertierung in das Basis‑Schema.

Dieses Skript liest Transkribus XML‑Dateien, extrahiert die Metadaten und den Text
und konvertiert sie in das in WORKFLOW.md definierte Basis‑Schema unter Verwendung der
in document_schemas.py definierten Klassen für Objektorientierung und Datenvalidierung.
"""
# source .venv/bin/activate

# --------------- Modulpfade vorbereiten ---------------
import os
import sys
import traceback
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
import json, re, time, xml.etree.ElementTree as ET
from datetime import datetime
import openai

#Zeitstempel
now = datetime.now()
formatted = now.strftime("%d.%m.%Y. %H:%M")
print(formatted)


# Basis­verzeichnis = zwei Ebenen über diesem File  (…/3_MA_Project)
THIS_FILE = Path(__file__).resolve()
BASE_DIR  = THIS_FILE.parents[2]

THIS_FILE    = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent  # …/Transkribus_to_base
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --------------- Eigene Module (aus /Module) ---------------
from Module import (
    enrich_pipeline,

    # Schemas
    BaseDocument, Person, Place, Event, Organization,

    # Person‐Matcher
    KNOWN_PERSONS,match_person,
    deduplicate_persons,
    normalize_name, 
    load_known_persons_from_csv,
    get_best_match_info, 
    extract_person_data,
    split_and_enrich_persons,
    
    #letter-metadata-matcher
    match_authors,
    match_recipients,
    resolve_llm_custom_authors_recipients,
    


    # Organisation‐Matcher
    match_organization_from_text,
    load_organizations_from_csv, 
    match_organization_from_text, 
    match_organization_entities,

    # Type‐Matcher
    get_document_type,

    # Rollen‑Enricher
    KNOWN_ROLE_LIST,
    ROLE_MAPPINGS_DE,
    NAME_RE,assign_roles_to_known_persons,
    extract_standalone_roles,
    map_role_to_schema_entry,
    extract_role_in_token,
    process_text,

    # PlaceMatcher
    PlaceMatcher,mentioned_places_from_custom_data,

    # Validation
    validate_extended, generate_validation_summary,

    # LLM
    run_enrichment_on_directory,
)


# --------------- Externe Abhängigkeiten ---------------
import pandas as pd
import spacy
from rapidfuzz import fuzz, process

# --------------- Pfadkonfiguration ---------------
TRANSKRIBUS_DIR          = "/Users/svenburkhardt/Desktop/Transkribus_test_In"
OUTPUT_DIR               = "/Users/svenburkhardt/Desktop/Transkribus_test_Out"
OUTPUT_DIR_UNMATCHED   = os.path.join(OUTPUT_DIR, "unmatched")
OUTPUT_CSV_PATH          = os.path.join(OUTPUT_DIR, "known_persons_output.csv")

# ——— Pfade für Personen-Listen ———
CSV_PATH_KNOWN_PERSONS  = BASE_DIR / "Data" / "Nodegoat_Export" / "export-person.csv"
CSV_PATH_METADATA       = CSV_PATH_KNOWN_PERSONS
LOG_PATH                = BASE_DIR / "Data" / "new_persons.log"

# --- Pfad für Rollen ---
known_role_list = KNOWN_ROLE_LIST

# ——— Pfad für Orte ———
PLACE_CSV_PATH          = BASE_DIR / "Data" / "Nodegoat_Export" / "export-place.csv"


# ——— Pfad für Organisationen ———
ORG_CSV_PATH            = BASE_DIR / "Data" / "Nodegoat_Export" / "export-organisationen.csv"



# --------------- CSV‑Dateien laden (immer mit denselben Optionen) ---------------
read_opts = dict(sep=";", dtype=str, keep_default_na=False)

df = pd.read_csv(CSV_PATH_KNOWN_PERSONS, **read_opts)
print(df.isna().sum())           # Kontrolle: keine NaN → keine Floats



# === LLM API Key für Enrichment ===
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Lade deutsches spaCy-Modell
try:
    nlp = spacy.load("de_core_news_sm")
except:
    print("Warnung: SpaCy-Modell 'de_core_news_sm' nicht gefunden.")
    nlp = None

# === Bekannte Personen laden ===
# Lade bekannte Personen aus export-person.csv (Nodegoat)
all_known_persons_df = load_known_persons_from_csv(CSV_PATH_METADATA)

# Konvertiere in erwartetes Format für deduplicate_persons
all_known_persons_list = load_known_persons_from_csv(CSV_PATH_METADATA)
df_persons = pd.DataFrame(all_known_persons_list)


# Jetzt sicher deduplizierenz
deduplicated_persons = deduplicate_persons(all_known_persons_list)


# Wir verwenden die Funktionen aus person_matcher.py
known_persons_df = pd.read_csv(CSV_PATH_KNOWN_PERSONS, sep=";")
known_persons_list = [
    {
        "forename": str(row.get("Vorname", "") or "").strip(),
        "familyname": str(row.get("Nachname", "") or "").strip(),
        "alternate_name": str(row.get("Alternativer Vorname", "") or "").strip(),
        "title": "",
        "nodegoat_id": str(row.get("nodegoat ID", "") or "").strip()
    }
    for _, row in known_persons_df.iterrows()
]


# nodegoat_persons = load_nodegoat_persons(CSV_PATH_METADATA)


#=== Bekannte Organisationen laden ===
org_list = load_organizations_from_csv(str(ORG_CSV_PATH))
                                       
# === Bekannte Orte Laden ===
place_matcher = PlaceMatcher(PLACE_CSV_PATH)


# === Teste API KEY ===
if not OPENAI_API_KEY:
    print("Warnung: Kein API-Schlüssel gesetzt. Enrichment wird am Ende übersprungen.")



def save_new_csv(df: pd.DataFrame):
    try:
        # Erstelle das Output-Verzeichnis, wenn es nicht existiert
        os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
        
        # Speichern der DataFrame als neue CSV im Output-Ordner
        df.to_csv(OUTPUT_CSV_PATH, sep=";", index=False)
        print(f"Neue CSV gespeichert: {OUTPUT_CSV_PATH}")
    except Exception as e:
        print(f"Fehler beim Speichern der neuen CSV: {e}")

# Funktion zum Speichern der neuen Personen in CSV
def save_new_person_to_csv(forename: str, familyname: str, csv_path: str):
    """
    Speichert eine neue Person in der CSV-Datei, wenn sie noch nicht vorhanden ist.
    
    Args:
        forename (str): Vorname der Person
        familyname (str): Nachname der Person
        csv_path (str): Pfad zur CSV-Datei
    """
    # Prüfe, ob die Person bereits existiert
    if person_exists_in_known_list(forename, familyname, known_persons_list):
        print(f"{forename} {familyname} existiert bereits in der CSV.")
        return
    
        # Wenn nicht, füge die Person hinzu
    #     # Wenn nicht, füge die Person hinzu
    #     new_row = {
    #         "forename": forename,
    #         "familyname": familyname,
    #         "Alternativer Vorname": "",
    #         "[Wohnort] Location Reference": "",
    #         "[Geburt] Date Start": "",
    #         "[Tod] Date Start": "",
    #         "db:deathPlace": "",
    #         "Lfd_No.": f"{len(known_persons_df) 1:05d}"  # Neue ID mit führenden Nullen

    # }
    known_persons_df = pd.concat([known_persons_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Speichern der aktualisierten CSV
    known_persons_df.to_csv(csv_path, sep=";", index=False)
    
    # Aktualisieren der Liste bekannter Personen
    known_persons_list.append({
        "forename": forename,
        "familyname": familyname,
        "alternate_name": "",
        "title": "",
        "nodegoat_id": ""
    })
    print(f"Neue Person hinzugefügt: {forename} {familyname}")


def person_exists_in_known_list(forename: str, familyname: str, known_list: List[tuple]) -> bool:
    """
    Prüft, ob die Person in der bekannten Liste von Personen existiert, entweder exakt oder mit ähnlicher Schreibweise.
    Verwendet jetzt vorzugsweise die person_matcher.py Funktionen für bessere Konsistenz.
    
    Args:
        forename (str): Vorname der Person
        familyname (str): Nachname der Person
        known_list (list): Liste der bekannten Personen (Vorname, Nachname)
        
    Returns:
        bool: True, wenn die Person existiert, ansonsten False
    """
    # Erstelle temporäres Person-Dictionary für das Matching
    temp_person = {"forename": forename, "familyname": familyname}
    
    # Verwende die match_person Funktion aus person_matcher.py
    # und konvertiere known_list-Tupel in das erforderliche Format, falls nötig
    if not isinstance(known_list[0], dict) if known_list else False:
        # Konvertiere Tupel-Liste zu Dictionary-Liste
        known_persons_dicts = [{"forename": kf, "familyname": kl} for kf, kl in known_list]
        matched_person, score = match_person(temp_person, candidates=known_persons_dicts)
    else:
        # Verwende direkt die known_persons_list aus person_matcher
        matched_person, score = match_person(temp_person, candidates=known_persons_list)
    
    # Person existiert, wenn das Matching einen Score über 70 ergeben hat
    return matched_person is not None and score >= 70



def fuzzy_match_name(name: str, candidates: List[str], threshold: int) -> Tuple[Optional[str], int]:
    """
    Vergleicht einen Namen mit einer Liste möglicher Kandidaten und liefert den besten Match über Threshold.
    Verwendet Rapidfuzz mit vorheriger Namensnormalisierung.
    """
    best_match, best_score = None, 0
    normalized_name = normalize_name_string(name)  # <- String-basierte Normalisierung

    for candidate in candidates:
        score = fuzz.ratio(normalize_name_string(candidate), normalized_name)
        if score > best_score:
            best_score, best_match = score, candidate

    return (best_match, best_score) if best_score >= threshold else (None, 0)


def match_person_from_text(person_name: str) -> Optional[Dict[str, str]]:
    """
    Sucht eine Person anhand eines Namenstextes in der Liste bekannter Personen.
    Berücksichtigt dabei auch extrahierte Titel wie "Dr." oder "Herr".

    Args:
        person_name: Der zu suchende Personenname

    Returns:
        Matched person dictionary oder None, wenn keine Übereinstimmung gefunden wurde
    """
    if not person_name:
        return None

    # Titel und Name bereinigen (z. B. "Herr Dr. Emil Hosp")
    # Funktion ist bereits über das Module-Paket importiert
    
    cleaned_name, extracted_title = normalize_name(person_name)

    # Extrahiere Vor- und Nachname
    forename, familyname = extract_name_with_spacy(cleaned_name)

    # Erstelle Person-Dictionary
    person_dict = {
        "forename": forename,
        "familyname": familyname,
        "title": extracted_title
    }

    # Match mit bekannter Liste
    matched_person, score = match_person(person_dict)

    if matched_person and score >= 70:
        # Titel auch im Rückgabeobjekt setzen (wenn original nicht gesetzt)
        if "title" not in matched_person or not matched_person["title"]:
            matched_person["title"] = extracted_title
        return matched_person

    return None



def extract_name_with_spacy(name_text: str) -> tuple:
    """
    Verwendet spaCy, um einen Namen in Vor- und Nachnamen zu trennen.
    Berücksichtigt auch mittlere Namen.
    
    Args:
        name_text: Der zu analysierende Namenstext
        
    Returns:
        Tuple aus (Vorname, Nachname)
    """
    # Fallback-Werte
    forename = ""
    familyname = name_text
    
    # Leerzeichen am Anfang und Ende entfernen
    name_text = name_text.strip()
    
    # Wenn kein Name oder leerer String übergeben wurde
    if not name_text:
        return forename, familyname
    
    # Standard-Methode zur Namenstrennung ohne spaCy
    def split_name_standard(text):
        name_parts = text.split()
        if len(name_parts) > 1:
            # Erster Teil ist Vorname, letzter Teil ist Nachname
            forename = name_parts[0]
            # Falls mittlere Namen vorhanden sind, füge sie zum Vornamen hinzu
            if len(name_parts) > 2:
                forename += " " " ".join(name_parts[1:-1])
            familyname = name_parts[-1]
            return forename, familyname
        return "", text  # Wenn nur ein Wort, behandle es als Nachnamen
    
    # Wenn spaCy nicht geladen werden konnte, verwende die Standardmethode
    if nlp is None:
        return split_name_standard(name_text)
    
    # Analysiere den Text mit spaCy
    doc = nlp(name_text)
    
    # Sammle alle gefundenen Personenentitäten
    person_entities = [ent for ent in doc.ents if ent.label_ == "PER"]
    
    # Wenn keine Personenentitäten gefunden wurden, versuche es mit der herkömmlichen Methode
    if not person_entities:
        return split_name_standard(name_text)
    
    # Versuche, den Namen aus den gefundenen Entitäten zu extrahieren
    person_entity = person_entities[0]  # Nehme die erste gefundene Person
    
    # Prüfe, ob es mehrere Tokens im Namen gibt
    if len(person_entity) > 1:
        # Alle Tokens außer dem letzten sind Teil des Vornamens (einschließlich mittlerer Namen)
        forename = " ".join([token.text for token in person_entity[:-1]])
        # Letzter Token ist der Nachname
        familyname = person_entity[-1].text
    
    # Wenn die Aufteilung nicht funktioniert hat, versuche es mit der Standardmethode
    if not forename:
        return split_name_standard(name_text)
    
    return forename, familyname


# XML-Namespace (für Transkribus-Dateien)
NS = {"ns": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}

# Sicherstellen, dass das Ausgabeverzeichnis existiert
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR_UNMATCHED, exist_ok=True)

def extract_metadata_from_xml(root: ET.Element) -> Dict[str, str]:
    """
    Extrahiert Metadaten aus einem XML-Element
    
    Args:
        root: XML-Root-Element
        
    Returns:
        Dictionary mit Metadaten
    """
    transkribus_meta = root.find(".//ns:TranskribusMetadata", NS)
    if transkribus_meta is None:
        return {}
    
    return {
        "docId": transkribus_meta.get("docId", ""),
        "pageId": transkribus_meta.get("pageId", ""),
        "tsid": transkribus_meta.get("tsid", ""),
        "imgUrl": transkribus_meta.get("imgUrl", ""),
        "xmlUrl": transkribus_meta.get("xmlUrl", "")
    }

def extract_text_from_xml(root: ET.Element) -> str:
    """
    Extrahiert den Text aus einem XML-Element
    
    Args:
        root: XML-Root-Element
        
    Returns:
        Extrahierter Text
    """
    transcript_text = ""
    for text_equiv in root.findall(".//ns:TextEquiv/ns:Unicode", NS):
        if text_equiv.text:
              transcript_text += text_equiv.text + "\n"
    
    return transcript_text.strip()

# Diese Funktion wird nicht mehr benötigt, da wir das Dokument direkt in process_transkribus_file erstellen
# und die BaseDocument-Klasse verwenden

def fuzzy_person_match(forename: str, familyname: str, known_list: List[tuple], threshold: int = 90) -> bool:
    """
    Vergleicht einen neuen Namen mit bekannten Personen mithilfe von fuzzy matching.
    Verwendet intern die verbesserte match_person-Funktion aus person_matcher.

    Args:
        forename: Vorname der neuen Person
        familyname: Nachname der neuen Person
        known_list: Liste bekannter (Vorname, Nachname) Tupel
        threshold: Ähnlichkeitsschwelle (0–100)

    Returns:
        True, wenn eine ähnliche Person gefunden wurde, sonst False.
    """
    # Nutze die bessere Matching-Funktion aus person_matcher
    temp_person = {"forename": forename, "familyname": familyname}
    matched_person, score = match_person(temp_person)
    
    # Wenn ein Match gefunden wurde und der Score über dem Schwellwert liegt
    if matched_person and score >= threshold:
        return True
    
    # Falls wir noch Abwärtskompatibilität benötigen, die alte Methode als Fallback
    for known_forename, known_familyname in known_list:
        score_first = fuzz.ratio(forename.lower(), known_forename.lower())
        score_last = fuzz.ratio(familyname.lower(), known_familyname.lower())
        
        # Beides muss über Schwellwert liegen
        if score_first >= threshold and score_last >= threshold:
            return True

        # Optional: Nur Nachnamenvergleich mit hoher Sicherheit
        if not forename and score_last >= threshold:
            return True

    return False
def parse_custom_attrs(attr_str: str) -> Dict[str, str]:
    """
    Wandelt z.B. "offset:36; length:13;" in {"offset":"36", "length":"13"} um.
    """
    result: Dict[str, str] = {}
    # Splitte am Semikolon und filtere leere Einträge
    for part in attr_str.split(';'):
        if ':' not in part:
            continue
        key, val = part.split(':', 1)
        result[key.strip()] = val.strip()
    return result



import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional

# Assuming parse_custom_attrs, extract_person_from_custom, extract_date_from_custom,
# extract_place_from_custom are defined/imported elsewhere in your module

# ----------------------------------------------------------------------------
# Helper to pull wikidata IDs from the custom attribute string
# ----------------------------------------------------------------------------
def extract_wikidata_id(custom_str: str) -> Optional[str]:
    """
    Finds a wikiData:Q... token in the custom attribute and returns the Q-ID.
    """
    m = re.search(r'wikiData:([A-Za-z0-9]+)', custom_str)
    return m.group(1) if m else None

# ----------------------------------------------------------------------------
# Custom-Tag-Extraction
# ----------------------------------------------------------------------------

def extract_organization_from_custom(custom_attr: str, text: str) -> List[Dict[str, Any]]:
    orgs: List[Dict[str, Any]] = []
    # finde alle organization-Tags
    for m in re.finditer(r'organization\s*\{([^}]+)\}', custom_attr):
        data = parse_custom_attrs(m.group(1))
        offset = int(data.get("offset", 0))
        length = int(data.get("length", 0))
        name = text[offset: offset + length].strip()
        wikidata = extract_wikidata_id(custom_attr)
        orgs.append({
            "name": name,
            "offset": offset,
            "length": length,
            "wikidata_id": wikidata
        })
    return orgs


def extract_custom_attributes(
    root: ET.Element,
    known_persons: List[Dict[str, str]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    # Verwende known_persons_list als Standard, wenn kein known_persons übergeben wurde
    if known_persons is None:
        known_persons = known_persons_list
    result = {
        "persons": [],
        "roles": [],
        "organizations": [],
        "dates": [],
        "places": []
    }

    for text_line in root.findall(".//ns:TextLine", NS):
        custom_attr = text_line.get("custom", "")
        if not custom_attr:
            continue

        # Extract the line text
        text_equiv = text_line.find(".//ns:TextEquiv/ns:Unicode", NS)
        text_content = text_equiv.text or "" if text_equiv is not None else ""

        # 1) Personen
        persons = extract_person_from_custom(custom_attr, text_content, known_persons)
        if persons:
            result["persons"].extend(persons)

        # 2) Rollen
        for m in re.finditer(r'role\s*\{([^}]+)\}', custom_attr):
            data = parse_custom_attrs(m.group(1))
            offset = int(data.get("offset", 0))
            length = int(data.get("length", 0))
            snippet = text_content[offset: offset + length]
            result["roles"].append({
                "raw":    snippet,
                "offset": offset,
                "length": length
            })

        # 3) Organisationen
        orgs = extract_organization_from_custom(custom_attr, text_content)
        if orgs:
            result["organizations"].extend(orgs)

        # 4) Daten
        dates = extract_date_from_custom(custom_attr, text_content)
        if dates:
            result["dates"].extend(dates)

        # 5) Orte
        places = extract_place_from_custom(custom_attr, text_content)
        if places:
            result["places"].extend(places)

    # Debug-Ausgabe
    print(f"[DEBUG] Extracted entities: persons={len(result['persons'])}, "
          f"roles={len(result['roles'])}, "
          f"places={len(result['places'])}, "
          f"organizations={len(result['organizations'])}, "
          f"dates={len(result['dates'])}")

    return result


def extract_person_data(name: str) -> Dict[str, str]:
    """Zerlegt den erkannten Namen in Vor- und Nachname."""
    name = name.strip()
    parts = name.split(" ", 1)
    forename = parts[0] if parts else ""
    familyname = parts[1] if len(parts) > 1 else ""
    return {
        "forename": forename,
        "familyname": familyname,
        "alternate_name": "",
        "title": "",
        "nodegoat_id": "",
    }
    
def extract_person_from_custom(
    custom_attr: str,
    text_content: str,
    known_persons: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    """Extract person entities from custom attributes - returns a LIST of dictionaries."""
    persons: List[Dict[str, str]] = []

    for pattern in [r"(?i)person\s*\{([^}]+)\}"]:
        for person_match in re.finditer(pattern, custom_attr):
            if not text_content:
                continue

            person_data = parse_custom_attributes(person_match.group(1))
            if "offset" in person_data and "length" in person_data:
                offset = int(person_data["offset"])
                length = int(person_data["length"])
                person_name = text_content[offset : offset + length]

                person_name = re.sub(
                    r".*(Herrn?|Frau|Dr\.?|Prof\.?|Fräulein|Witwe)\s+",
                    "",
                    person_name,
                    flags=re.IGNORECASE
                ).strip()

                # Rolle aus deklinierter Form erkennen
                role = ""
                role_match = re.match(r"(?P<role>[A-ZÄÖÜa-zäöüß\-]+en)\s+(?P<name>[A-ZÄÖÜ][a-zäöüß]+)", person_name)
                if role_match:
                    role_raw = role_match.group("role").rstrip("en")
                    role = ROLE_MAPPINGS_DE.get(role_raw.lower(), "")
                    person_name = role_match.group("name")

                # Haupt-Zerlegung des Namens
                person_info = extract_person_data(person_name)
                person_info["role"] = role

                # Fallback für Titel aus known_persons
                if not person_info["forename"]:
                    possible_titles = set(p.get("title", "").strip() for p in known_persons if p.get("title"))
                    for title in sorted(possible_titles, key=lambda x: -len(x)):
                        if re.match(rf"^{re.escape(title)}\b", person_name):
                            rest = person_name[len(title):].strip()
                            person_info = extract_person_data(rest)
                            person_info["title"] = title
                            break

                # Fuzzy Matching gegen bekannte Personen
                match, score = match_person(person_info, candidates=known_persons)
                if match:
                    person_info["nodegoat_id"]    = match.get("nodegoat_id", "")
                    person_info["alternate_name"] = match.get("alternate_name", "")
                    person_info["title"]          = match.get("title", "")

                # Ortserkennung (falls im gleichen Tag vorhanden)
                place_dicts = extract_place_from_custom(custom_attr, text_content)
                person_place = place_dicts[0]["name"] if place_dicts else ""

                # Finales Dictionary aufbauen
                persons.append({
                    "forename":               person_info["forename"],
                    "familyname":             person_info["familyname"],
                    "role":                   person_info.get("role", ""),
                    "associated_place":       person_place,
                    "associated_organisation": "",
                    "alternate_name":         person_info.get("alternate_name", ""),
                    "title":                  person_info.get("title", ""),
                    "nodegoat_id":            person_info.get("nodegoat_id", "")
                })

    return persons


def extract_organization_from_custom(custom_attr: str, text_content: str) -> List[Dict[str, str]]:
    organizations = []
    # Fix regex pattern - try different variations to match actual data
    for pattern in [r"organization\s+\{([^}]+)\}", r"organization {([^}]+)}", 
                    r"org\s+\{([^}]+)\}", r"org {([^}]+)}"]:
        org_matches = re.finditer(pattern, custom_attr)
        for org_match in org_matches:
            if not text_content:
                continue
                
            org_data = parse_custom_attributes(org_match.group(1))
            if "offset" in org_data and "length" in org_data:
                offset = int(org_data.get("offset", 0))
                length = int(org_data.get("length", 0))
                
                print(f"[DEBUG] Versuche Extraktion Organisation: offset={offset}, length={length}, text='{text_content}'")
                
                if offset < len(text_content) and offset + length <= len(text_content):
                    org_name = text_content[offset:offset+length]
                    print(f"[DEBUG] Erkannte Organisation: {org_name}")

                    organizations.append({
                        "name": org_name,
                        "location": "",
                        "type": ""
                    })

    return organizations

def extract_date_from_custom(custom_attr: str, text_content: str) -> List[str]:
    dates = []
    # Fix regex pattern - try different variations
    for pattern in [r"date\s+\{([^}]+)\}", r"date {([^}]+)}"]:
        date_matches = re.finditer(pattern, custom_attr)
        for date_match in date_matches:
            if not text_content:
                continue
                
            date_data = parse_custom_attributes(date_match.group(1))
            # print(f"[DEBUG] Date data: {date_data}")
            
            if "when" in date_data:
                date_str = date_data.get("when", "")
                print(f"[DEBUG] Erkanntes Datum: {date_str}")
                
                # Process date format
                date_parts = date_str.split(".")
                if len(date_parts) == 3:
                    day, month, year = date_parts
                    formatted_date = f"{year}.{month}.{day}"
                elif len(date_parts) == 2:
                    month, year = date_parts
                    formatted_date = f"{year}.{month}"
                else:
                    # Try other date patterns
                    date_regex_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
                    if date_regex_match:
                        day, month, year = date_regex_match.groups()
                        formatted_date = f"{year}.{month}.{day}"
                    elif re.match(r"\d{4}-\d{2}-\d{2}", date_str):
                        year, month, day = date_str.split("-")
                        formatted_date = f"{year}.{month}.{day}"
                    else:
                        formatted_date = date_str
                
                # print(f"[DEBUG] Formatiertes Datum: {formatted_date}")
                dates.append(formatted_date)
    return dates

def extract_place_from_custom(custom_attr: str, text_content: str) -> List[Dict[str, Any]]:
    places = []
    # Fix regex pattern - try different variations
    for pattern in [r"place\s+\{([^}]+)\}", r"place {([^}]+)}"]:
        place_matches = re.finditer(pattern, custom_attr)
        for place_match in place_matches:
            if not text_content:
                continue
                
            place_data = parse_custom_attributes(place_match.group(1))
            if "offset" in place_data and "length" in place_data:
                offset = int(place_data.get("offset", 0))
                length = int(place_data.get("length", 0))
                
                # print(f"[DEBUG] Versuche Extraktion Ort: offset={offset}, length={length}, text='{text_content}'")
                
                if offset < len(text_content) and offset + length <= len(text_content):
                    place_name = text_content[offset:offset+length]
                    print(f"[DEBUG] Erkannter Ort: {place_name}")
                    
                    try:
                        if place_matcher and place_name:
                            match_result = place_matcher.match_place(place_name)
                            if match_result:
                                matched_data = match_result["data"]
                                print(f"[DEBUG] Match für Ort '{place_name}' → Nodegoat-ID: {matched_data.get('nodegoat_id')}")

                                places.append({
                                "name": matched_data.get("name", place_name),
                                "alternate_name": matched_data.get("alternate_place_name", ""),
                                "geonames_id": matched_data.get("geonames_id", ""),
                                "wikidata_id": matched_data.get("wikidata_id", ""),
                                "nodegoat_id": matched_data.get("nodegoat_id", ""),
                                "type": "",
                                "original_input": place_name,
                                "matched_name": match_result["matched_name"],
                                "match_score": match_result["score"],
                                "confidence": match_result.get("confidence", "unknown")
                            })


                            else:
                                places.append({
                                    "name": place_name,
                                    "country": "",
                                    "type": "",
                                    "original_input": place_name,
                                    "matched_name": None,
                                    "match_score": None,
                                    "confidence": "none"
                                })
                        elif place_name:
                            places.append({
                                "name": place_name,
                                "country": "",
                                "type": "",
                                "original_input": place_name,
                                "matched_name": None,
                                "match_score": None,
                                "confidence": "matcher_unavailable"
                            })
                    except Exception as e:
                        print(f"Fehler beim Ortsmatching für '{place_name}': {e}")
                        places.append({
                            "name": place_name,
                            "country": "",
                            "type": "",
                            "original_input": place_name,
                            "matched_name": None,
                            "match_score": None,
                            "confidence": "error",
                            "error": str(e)
                        })
    
    
    return places


def parse_custom_attributes(attr_str: str) -> Dict[str, str]:
    """
    Parst einen String mit custom-Attributen
    
    Args:
        attr_str: String mit Attributen (z.B. "offset:0; length:5;")
        
    Returns:
        Dictionary mit den geparsten Attributen
    """
    result = {}
    for part in attr_str.split(";"):
        part = part.strip()
        if not part:
            continue
        
        key_value = part.split(":", 1)
        if len(key_value) == 2:
            key, value = key_value
            result[key.strip()] = value.strip()
    
    return result

def process_transkribus_file(
    xml_path: str,
    seven_digit_folder: str,
    subdir: str
) -> Union[BaseDocument, None]:
    try:
        # 1) XML parsen
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # 2) full_doc_id & document_type ermitteln
        xml_file      = os.path.basename(xml_path)
        full_doc_id   = f"{seven_digit_folder}_{subdir}_{xml_file.replace('.xml','')}"
        document_type = get_document_type(full_doc_id, xml_path)
        print(f"[DEBUG] Dokumenttyp erkannt für {full_doc_id}: {document_type}")

        # 3) Metadaten extrahieren (sofern vorhanden)
        metadata_info = extract_metadata_from_xml(root)
        metadata_info["document_type"] = document_type

        # 4) Transkript holen und auf Länge prüfen
        transcript_text = "\n".join(
            te.text for te in root.findall(".//ns:TextEquiv/ns:Unicode", {"ns": root.tag.split("}")[0].strip("{")})
            if te.text
        )
        if not transcript_text or len(transcript_text.strip()) < 10:
            print(f"[INFO] Überspringe {xml_path} – Transkript zu kurz oder leer.")
            return None

        # --- 5) Autor & Empfänger matchen inkl. LLM-Bereinigung ---
        authors_info    = match_authors(transcript_text, document_type=document_type)
        recipients_info = match_recipients(transcript_text, document_type=document_type)
        print(f"[DEBUG] Autor erkannt: {authors_info}")
        print(f"[DEBUG] Empfänger erkannt: {recipients_info}")

        # Erzeuge temporäre Personenobjekte
        temp_authors: Optional[Person] = Person.from_dict(authors_info) if authors_info.get("forename") else None
        temp_recipients: Optional[Person] = Person.from_dict(recipients_info) if recipients_info.get("forename") else None

        # Konflikt-Log Pfad definieren (einmalig pro Seite)
        conflict_log_path = Path(OUTPUT_DIR_UNMATCHED) / f"conflict_log_{seven_digit_folder}_{subdir}.json"

        # LLM-Matching zur finalen Bereinigung (z. B. bei Abweichung zwischen XML und Text)
        temp_doc = BaseDocument(authors=temp_authors, recipients=temp_recipients)
        final_authors, final_recipients = resolve_llm_custom_authors_recipients(
            base_doc=temp_doc,
            xml_text=transcript_text,
            log_path=conflict_log_path
        )

        # Diese finalen Personenobjekte werden ins JSON übernommen
        authors    = final_authors
        recipients = final_recipients

        # Autor/Empfänger auch in mentioned_persons aufnehmen (optional, je nach Kontext)
        mentioned_persons: List[Person] = []
        if authors:
            mentioned_persons.append(authors)
        if recipients:
            mentioned_persons.append(recipients)

        # 6) Rolleninformationen aus Fließtext holen und bekannten Personen zuordnen
        role_input_persons = load_known_persons_from_csv()
        role_input_persons = assign_roles_to_known_persons(role_input_persons, transcript_text)
        print("[DEBUG] Rollenmatching aus Fließtext:")
        for p in role_input_persons:
            if p.get("role"):
                print(f" → {p['forename']} {p['familyname']}: {p['role']} ({p['role_schema']})")

        # 6a) Custom-Tags extrahieren mit angereicherten Personeninfos
        custom_data = extract_custom_attributes(root, known_persons=role_input_persons)

        # 7) Fallback: Organisationen aus Plain-Text
        custom_orgs = custom_data["organizations"]
        orgs_from_text = match_organization_from_text(transcript_text, org_list)
        for org in orgs_from_text:
            if not any(o["name"] == org["name"] for o in custom_orgs):
                custom_orgs.append({
                    "name": org["name"],
                    "location": org.get("location", ""),
                    "type":     org.get("type", "")
                })

        # 8) Personen deduplizieren und anreichern
        all_persons = custom_data["persons"]
        # 8a) Rollen aus Tokens wie 'Ehrenvorsitzender Burger' extrahieren
        roles_from_tokens = []
        for p in all_persons:
            token = p.get("raw_token", "")
            roles_from_tokens.extend(extract_role_in_token(token))

        # 8b) Kombinieren
        combined_persons = all_persons + roles_from_tokens

        # 8c) Deduplizieren
        unique_persons = deduplicate_persons(all_persons)
        
        # Mit Stammliste abgleichen (Nodegoat-ID, Titel, etc.)
        for person in unique_persons:
            match = get_best_match_info(person, KNOWN_PERSONS)
            if match["match_id"]:
                person["forename"]     = match["matched_forename"]   or person["forename"]
                person["familyname"]   = match["matched_familyname"] or person["familyname"]
                person["title"]        = match["matched_title"]      or person.get("title", "")
                person["nodegoat_id"]  = match["match_id"]
                person["match_score"]  = match["score"]
                person["confidence"]   = "fuzzy"

        # Rollenmodul anwenden
        role_input_persons = [
            {
                "forename": p.get("forename", ""),
                "familyname": p.get("familyname", ""),
                "alternate_name": p.get("alternate_name", ""),
                "title": p.get("title", ""),
                "role": p.get("role", ""),
                "associated_place": p.get("associated_place", ""),
                "associated_organisation": p.get("associated_organisation", ""),
                "nodegoat_id": p.get("nodegoat_id", "")
            }
            for p in unique_persons
        ]
        enriched_persons = assign_roles_to_known_persons(role_input_persons, transcript_text)
        combined_persons = unique_persons + [
            p for p in enriched_persons
            if not any(p["forename"] == up["forename"] and p["familyname"] == up["familyname"] for up in unique_persons)
        ]
        # 9) Neue Personen loggen
        for person in unique_persons:
            if not person_exists_in_known_list(
                person.get("forename", ""), 
                person.get("familyname", ""), 
                known_persons_list
            ):
                with open(LOG_PATH, "a", encoding="utf-8") as log_file:
                    log_file.write(f"{person.get('forename', '')} {person.get('familyname', '')}\n")

        # 10) Person-Objekte erstellen
        mentioned_persons = []
        for d in enriched_persons:
            if not d.get("forename") and not d.get("familyname") and not d.get("role"):
                continue  # Nur komplett leere Personen überspringen

            mentioned_persons.append(Person(
                forename=d.get("forename", ""),
                familyname=d.get("familyname", ""),
                alternate_name=str(d.get("alternate_name", "") or ""),
                title=str(d.get("title", "") or ""),
                role=d.get("role", ""),
                associated_place=d.get("associated_place", ""),
                associated_organisation=d.get("associated_organisation", ""),
                nodegoat_id=str(d.get("nodegoat_id", "") or ""),
                match_score=d.get("match_score"),
                confidence=d.get("confidence", "")
            ))

        # 11) Orte deduplizieren mit PlaceMatcher
        all_places = custom_data["places"]
        
        # PlaceMatcher-Format anpassen
        place_matcher_format = [
            {
                "matched_name": pl.get("matched_name", pl["name"]),
                "matched_raw_input": pl.get("original_input", pl["name"]),
                "score": pl.get("match_score", 0),
                "confidence": pl.get("confidence", "unknown"),
                "data": {
                    "name": pl.get("name", ""),
                    "alternate_place_name": pl.get("alternate_name", ""),
                    "geonames_id": pl.get("geonames_id", ""),
                    "wikidata_id": pl.get("wikidata_id", ""),
                    "nodegoat_id": pl.get("nodegoat_id", "")
                }
            }
            for pl in all_places
        ]
        
        # Verwende PlaceMatcher wenn verfügbar
        if place_matcher:
            matched_places, _ = place_matcher.deduplicate_places(place_matcher_format, document_id=full_doc_id)
            unique_places = []
            
            # Konvertiere zurück ins erwartete Format
            for mp in matched_places:
                unique_places.append({
                    "name": mp["data"].get("name") or mp.get("matched_name") or mp.get("matched_raw_input", ""),
                    "alternate_name": mp["data"].get("alternate_place_name", ""),
                    "geonames_id": mp["data"].get("geonames_id", ""),
                    "wikidata_id": mp["data"].get("wikidata_id", ""),
                    "nodegoat_id": mp["data"].get("nodegoat_id", ""),
                    "type": "",
                    "match_score": mp.get("score", 0),
                    "confidence": mp.get("confidence", "")
                })
        else:
            # Fallback falls kein PlaceMatcher verfügbar ist
            seen_places = set()
            unique_places = []
            for pl in all_places:
                key = (pl.get("name", ""), pl.get("nodegoat_id", ""))
                if key not in seen_places:
                    seen_places.add(key)
                    unique_places.append(pl)
                    
        # 12) Organization-Objekte erstellen
        mentioned_organizations = [
            Organization.from_dict(o) for o in custom_orgs
        ]
        
        # --- 13) Orte deduplizieren & transformieren ---
        place_input = custom_data["places"]

        dedup_input = [
            {
                "matched_name": pl.get("matched_name", pl.get("name", "")),
                "matched_raw_input": pl.get("original_input", pl.get("name", "")),
                "score": pl.get("match_score", 0),
                "confidence": pl.get("confidence", "unknown"),
                "data": {
                    "name": pl.get("name", ""),
                    "alternate_place_name": pl.get("alternate_name", ""),
                    "geonames_id": pl.get("geonames_id", ""),
                    "wikidata_id": pl.get("wikidata_id", ""),
                    "nodegoat_id": pl.get("nodegoat_id", "")
                }
            }
            for pl in place_input
        ]

        matched_places, _ = place_m.deduplicate_places(dedup_input, document_id=full_doc_id)


        mentioned_places = [
            Place(
                name=mp["data"].get("name", ""),
                type="",
                alternate_place_name=mp["data"].get("alternate_place_name", ""),
                geonames_id=mp["data"].get("geonames_id", ""),
                wikidata_id=mp["data"].get("wikidata_id", ""),
                nodegoat_id=mp["data"].get("nodegoat_id", "")
            )
            for mp in matched_places  # ← wichtig: matched_places verwenden!
        ]

        print(f"[DEBUG] Finale Orte im JSON:")
        for p in mentioned_places:
            print(f" - {p.name} | Geo: {p.geonames_id} | WD: {p.wikidata_id} | NG: {p.nodegoat_id}")



        # 14) BaseDocument zusammenbauen
        doc = BaseDocument(
            object_type             = "Dokument",
            attributes              = metadata_info,
            content_transcription   = transcript_text,
            authors                 = [final_authors] if final_authors else [],
            recipients              = [final_recipients] if final_recipients else [],
            mentioned_persons       = mentioned_persons,                                    #der Wichscheiss produziert den fucking verdammten drecksfehler
            mentioned_organizations = mentioned_organizations,
            mentioned_places        = mentioned_places,
            mentioned_dates         = custom_data["dates"],
            content_tags_in_german  = [],
            creation_date           = "",
            creation_place          = "",
            document_type           = document_type,
            document_format         = ""
        )


        final_authors, final_recipients = resolve_llm_custom_authors_recipients(doc, transcript_text, log_path=Path("conflict_log.json"))
        if final_authors:
            doc.authors = [final_authors] if final_authors else []
        if final_recipients:
            doc.recipients = [final_recipients] if final_recipients else []

        # 15) Dokument speichern
        output_path = os.path.join(OUTPUT_DIR, f"{full_doc_id}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(doc.to_json(indent=2))
        print(f"Gespeichert: {output_path}")

        return doc

    except Exception as e:
        print(f"Fehler bei der Verarbeitung von {xml_path}: {e}")
        traceback.print_exc()
        return None
NAME_RE = re.compile(
    r"\b[A-ZÄÖÜ][a-zäöüß]+(?:\s[A-ZÄÖÜ][a-zäöüß]+)*\b"
)



def get_place_name(pl: Dict[str, Any]) -> str:
    return pl.get("name") or pl.get("matched_name") or pl.get("matched_raw_input", "")




def main():
    print("Starte Extraktion von Transkribus-Daten...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_unmatched_persons: List[Dict[str, Any]] = []
    all_unmatched_places: Dict[str, List[Dict[str, Any]]] = {}
    all_unmatched_roles: Dict[str, List[Dict[str, Any]]] = {}

    # 1) Place-Matcher initialisieren
    place_m = PlaceMatcher(PLACE_CSV_PATH)
    if not place_m.known_name_map:
        print(f"ERROR: konnte keine Orte laden aus '{PLACE_CSV_PATH}'")
        return

    # 2) Organisationen laden
    org_list = load_organizations_from_csv(str(ORG_CSV_PATH))
    if not org_list:
        print(f"ERROR: konnte keine Organisationen laden aus '{ORG_CSV_PATH}'")
        return

    # 3) Ordnerstruktur durchlaufen
    for seven_digit_folder in os.listdir(TRANSKRIBUS_DIR):
        if not seven_digit_folder.isdigit():
            continue
        for subdir in os.listdir(os.path.join(TRANSKRIBUS_DIR, seven_digit_folder)):
            if not subdir.startswith("Akte_"):
                continue

            page_dir = os.path.join(TRANSKRIBUS_DIR, seven_digit_folder, subdir, "page")
            if not os.path.isdir(page_dir):
                continue

            print(f"\n→ Scanning folder: {page_dir}")
            for xml_file in sorted(os.listdir(page_dir)):
                if not xml_file.endswith("_preprocessed.xml"):
                    continue
                xml_path = os.path.join(page_dir, xml_file)
                print(f"Verarbeite preprocessed-Datei: {xml_file}")

                # --- XML parsen & Metadaten holen ---
                root = ET.parse(xml_path).getroot()
                custom_data = extract_custom_attributes(root)
                transcript_text = extract_text_from_xml(root)
                if not transcript_text or len(transcript_text.strip()) < 10:
                    print(f"[INFO] Überspringe {xml_file} – Transkript zu kurz oder leer.")
                    continue
                # --- DEBUG: Rollen ohne Personenbezug ---
                print("\n[DEBUG] Suche nach Rollenbezeichnungen im Transkript:")
                standalone_roles = extract_standalone_roles([], transcript_text)
                for r in standalone_roles:
                    print(" →", r.get("role"), "—", r.get("forename", ""), r.get("familyname", ""))
                    context_lines = [line for line in transcript_text.splitlines() if r["role"] in line]
                    for cl in context_lines:
                        print("     Kontext:", cl)

                # full_doc_id bestimmen
                m = re.search(r"p(\d+)", xml_file, re.IGNORECASE)
                page_num = m.group(1) if m else "001"
                full_doc_id = f"{seven_digit_folder}_{subdir}_page{page_num}"

                # Dokumenttyp
                document_type = get_document_type(full_doc_id, xml_path)
                metadata_info = extract_metadata_from_xml(root)
                metadata_info["document_type"] = document_type

                # --- 5) Autor & Empfänger matchen ---
                authors: Optional[Person] = None
                recipients: Optional[Person] = None

                # a) Autor extrahieren
                authors_info = match_authors(transcript_text, document_type=document_type)
                if getattr(authors_info, "forename", ""):
                    authors = authors_info

                # b) Empfänger extrahieren
                recipients_info = match_recipients(transcript_text, document_type=document_type)
                if getattr(recipients_info, "forename", ""):
                    recipients = recipients_info

                # c) Autor & Empfänger auch in mentioned_persons aufnehmen
                mentioned_persons: List[Person] = []
                if authors:
                    mentioned_persons.append(authors)
                if recipients:
                    mentioned_persons.append(recipients)


                # --- 6–9) Personen splitten, deduplizieren und anreichern ---
                matched_persons, unmatched_persons = split_and_enrich_persons(
                    custom_data["persons"],
                    transcript_text,
                    document_id=full_doc_id,
                    candidates=KNOWN_PERSONS
                )
                # Zähle Mehrfachnennungen
                name_counts = Counter((p["forename"].strip(), p["familyname"].strip()) for p in matched_persons)
                final_persons = deduplicate_persons(matched_persons)

                # --- 10) Rollen aus *Text* extrahieren – unabhängig von final_persons ---
                enriched_only = assign_roles_to_known_persons([], transcript_text)
                for ep in enriched_only:
                    if ep.get("familyname") == "Burger":
                        print(f"[CHECK] Burger wurde im Rollenextrakt erkannt: {ep}")

                # --- 11) Kombinieren: matched + role-extrahierte ---
                combined_raw = matched_persons + [
                    p for p in enriched_only
                    if not any(
                        p["forename"] == mp["forename"] and p["familyname"] == mp["familyname"]
                        for mp in matched_persons
                    )
                ]
                for p in combined_raw:
                    if p.get("familyname", "").lower() == "burger":
                        print(f"[CHECK] Burger wird übernommen: {p}")

                # === Gruppieren nach nodegoat_id oder Namen ===
                grouped_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for p in combined_raw:
                    key = p.get("nodegoat_id") or f"{p.get('forename','').strip()} {p.get('familyname','').strip()}"
                    grouped_by_id[key].append(p)

                rechecked_combined = []
                for key, group in grouped_by_id.items():
                    best = max(group, key=lambda x: float(x.get("match_score", 0) or 0))
                    merged = best.copy()

                    # Alle Rollen sammeln
                    all_roles = {p.get("role", "").strip() for p in group if p.get("role")}
                    merged["role"] = "; ".join(sorted(r for r in all_roles if r))

                    # Titel ggf. ergänzen
                    if not merged.get("title"):
                        for p in group:
                            if p.get("title"):
                                merged["title"] = p["title"]
                                break

                    # Confidence ggf. ergänzen
                    if not merged.get("confidence"):
                        merged["confidence"] = next((p.get("confidence", "") for p in group if p.get("confidence")), "")

                    # mentioned_count summieren
                    merged["mentioned_count"] = sum(int(p.get("mentioned_count", 1) or 1) for p in group)

                    rechecked_combined.append(merged)
                
                
                # --- 12) Deduplizieren ---
                # Gruppieren nach nodegoat_id oder name fallback
                name_to_id = {}
                for p in rechecked_combined + standalone_roles:
                    nid = p.get("nodegoat_id", "").strip()
                    if nid:
                        key = f"{p.get('forename','').strip()}|{p.get('familyname','').strip()}"
                        name_to_id[key] = nid

                # Gruppierung anhand von vorhandener ID oder Namens-Fallback
                grouped: Dict[str, List[Dict]] = defaultdict(list)
                for p in rechecked_combined + standalone_roles:
                    nid = p.get("nodegoat_id", "").strip()
                    fn = p.get("forename", "").strip()
                    ln = p.get("familyname", "").strip()
                    fallback_key = f"{fn}|{ln}"
                    key = nid if nid else name_to_id.get(fallback_key, fallback_key)
                    grouped[key].append(p)

                # Fallback-Mapping: name → nodegoat_id
                name_to_id = {}
                for key, entries in grouped_by_id.items():
                    if "|" in key:
                        for e in entries:
                            if e.get("nodegoat_id"):
                                name_to_id[key] = e["nodegoat_id"]
                                break

                # Merge Rolle und Confidence
                merged_entries = {}
                for key, entries in grouped.items():
                    actual_id = key if "|" not in key else name_to_id.get(key, key)
                    merged_entries.setdefault(actual_id, []).extend(entries)

                deduplicated_enriched_persons = []

                for key, entries in merged_entries.items():
                    print(f"\n[DEBUG] Kandidaten für Person-Key '{key}':")
                    for e in entries:
                        print(f" - Name: {e.get('forename','')} {e.get('familyname','')}, Role: {e.get('role','')}, Score: {e.get('match_score', 0)}, Confidence: {e.get('confidence', '')}, Nodegoat-ID: {e.get('nodegoat_id','')}")

                    best_entry = max(entries, key=lambda x: float(x.get("match_score", 0)))

                    # Match-Score als Dict pro Methode
                    score_dict: Dict[str, float] = {}
                    for e in entries:
                        confidence_str = e.get("confidence", "").strip()
                        score = e.get("match_score", 0)
                        try:
                            score = float(score)
                        except (ValueError, TypeError):
                            score = 0.0
                        for method in [m.strip() for m in confidence_str.split(",") if m.strip()]:
                            if method not in score_dict or score > score_dict[method]:
                                score_dict[method] = score

                    # Restlicher Merge
                    all_roles = sorted(set(r for r in [e.get("role", "").strip() for e in entries] if r))
                    all_confidences = sorted(set(c for c in [e.get("confidence", "").strip() for e in entries] if c))
                    total_mentions = sum(int(e.get("mentioned_count", 1) or 1) for e in entries)

                    merged = dict(best_entry)
                    merged["role"] = "; ".join(all_roles)
                    merged["confidence"] = ", ".join(all_confidences)
                    merged["match_score"] = score_dict
                    merged["mentioned_count"] = total_mentions

                    print(f"[DEBUG] → Gewählter Eintrag für '{key}': {merged.get('forename')} {merged.get('familyname')} | Role: '{merged['role']}', Confidence: '{merged['confidence']}', Score: {merged['match_score']}")

                    deduplicated_enriched_persons.append(merged)


                # --- 13) Konvertiere in Person-Objekte ---

                mentioned_persons = []
                for pd in deduplicated_enriched_persons:
                    mentioned_persons.append(Person(
                        forename=pd.get("forename", "").strip(),
                        familyname=pd.get("familyname", "").strip(),
                        alternate_name=pd.get("alternate_name", ""),
                        title=pd.get("title", ""),
                        role=pd.get("role", ""),
                        associated_place=pd.get("associated_place", ""),
                        associated_organisation=pd.get("associated_organisation", ""),
                        nodegoat_id=pd.get("nodegoat_id", ""),
                        match_score=pd.get("match_score", 0),
                        confidence=pd.get("confidence", ""),
                        mentioned_count=pd.get("mentioned_count", 1)
                    ))


                # --- 14) Sammle unmatched persons für Log ---
                for up in unmatched_persons:
                    token = up["raw_token"]
                    context = next((l for l in transcript_text.splitlines() if token in l), "")
                    all_unmatched_persons.append({
                        "akte": seven_digit_folder,
                        "document_id": full_doc_id,
                        "name": token,
                        "context": context.strip()
                    })

                # --- 15) Organisations-Extraktion & Matching ---
                raw_orgs     = custom_data["organizations"]
                matched_orgs = match_organization_entities(raw_orgs, org_list)
                seen, unique_orgs = set(), []
                for o in matched_orgs:
                    key = (o.get("nodegoat_id",""), o.get("name",""))
                    if key not in seen:
                        seen.add(key)
                        unique_orgs.append(o)

                mentioned_organizations = [
                    Organization(
                        name            = o["name"],
                        type            = o.get("type",""),
                        nodegoat_id     = o.get("nodegoat_id",""),
                        alternate_names = o.get("alternate_names", []),
                        feldpostnummer  = o.get("feldpostnummer",""),
                        match_score     = o.get("match_score"),
                        confidence      = o.get("confidence","")
                    )
                    for o in unique_orgs
                ]

                # --- 16) Orte deduplizieren & transformieren ---
                # (Annahme: unique_places bereits im PlaceMatcher-Teil berechnet)
                # Für Kürze hier übersprungen

                # nur eine Person pro nodegoat_id
                seen_ids = set()
                final_mentioned_persons = []
                for p in mentioned_persons:
                    if p.nodegoat_id and p.nodegoat_id in seen_ids:
                        continue
                    seen_ids.add(p.nodegoat_id)
                    final_mentioned_persons.append(p)
                mentioned_persons = final_mentioned_persons



                # --- 17) BaseDocument zusammenbauen ---
                doc = BaseDocument(
                    object_type="Dokument",
                    attributes              = metadata_info,
                    content_transcription   = transcript_text,
                    authors                  = authors,
                    recipients               = recipients,
                    mentioned_persons       = mentioned_persons,
                    mentioned_organizations = mentioned_organizations,
                    mentioned_places = mentioned_places_from_custom_data(
                        custom_data=custom_data,
                        full_doc_id=full_doc_id,
                        place_matcher=place_m,
                        get_place_name_fn=get_place_name
                    ),
                    
                    mentioned_dates         = custom_data["dates"],
                    content_tags_in_german  = [],
                    creation_date           = "",
                    creation_place          = "",
                    document_type           = document_type,
                    document_format         = ""
                )
                # 18) Autor und Empfänger aus LLM-Custom-Tags bereinigen/ergänzen
                with open(xml_path, "r", encoding="utf-8") as f:
                    xml_text = f.read()

                log_path = Path(OUTPUT_DIR_UNMATCHED) / f"{full_doc_id}_llm_conflict_log.json"
                final_authors, final_recipients = resolve_llm_custom_authors_recipients(doc, xml_text, log_path=log_path)
                doc.authors = final_authors
                doc.recipients = final_recipients


                # --- 19) Dokument speichern ---
                output_path = os.path.join(OUTPUT_DIR, f"{full_doc_id}.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(doc.to_json(indent=2))
                print(f"Gespeichert: {output_path}")

                # --- 20) Unmatched-Logs dumpen ---
                if all_unmatched_persons:
                    with open(os.path.join(OUTPUT_DIR_UNMATCHED, "unmatched_persons.json"), "w", encoding="utf-8") as fh:
                        json.dump(all_unmatched_persons, fh, ensure_ascii=False, indent=2)
                    print(f"[DEBUG] Alle unge­matchten Personen geschrieben.")

                if all_unmatched_places:
                    with open(os.path.join(OUTPUT_DIR_UNMATCHED, "unmatched_places.json"), "w", encoding="utf-8") as fh:
                        json.dump(all_unmatched_places, fh, ensure_ascii=False, indent=2)
                    print(f"[DEBUG] Alle unge­matchten Orte geschrieben.")

                if all_unmatched_roles:
                    with open(os.path.join(OUTPUT_DIR_UNMATCHED, "unmatched_roles.json"), "w", encoding="utf-8") as fh:
                        json.dump(all_unmatched_roles, fh, ensure_ascii=False, indent=2)
                    print(f"[DEBUG] Alle unge­matchten Rollen geschrieben.")

                # --- 21) LLM-Enrichment (optional) ---
                #if OPENAI_API_KEY:
                #    print("Starte LLM-Enrichment…")
                #    run_enrichment_on_directory(OUTPUT_DIR, api_key=OPENAI_API_KEY)
                #else:
                #    print("Warnung: Kein OPENAI_API_KEY – Enrichment übersprungen.")

    print("Fertig.")

if __name__ == "__main__":
    main()