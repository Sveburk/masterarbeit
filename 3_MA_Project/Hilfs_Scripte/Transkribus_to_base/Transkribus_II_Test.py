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
    extract_authors_recipients_from_mentions, 
    ensure_author_recipient_in_mentions,
    postprocess_roles,
    enrich_final_recipients,



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
                    person_info["match_score"] = score  
                    print(f"person_info{person_info},score{score}")

                # Ortserkennung (falls im gleichen Tag vorhanden)
                place_dicts = extract_place_from_custom(custom_attr, text_content)
                person_place = place_dicts[0]["name"] if place_dicts else ""

                print(f"[DEBUG] Erkannte Person aus XML: {person_info['forename']} {person_info['familyname']}, "
                      f"Nodegoat-ID: {person_info.get('nodegoat_id', '')}, Rolle: {role}")

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

        # 5) Autor/Empfänger matchen und mit LLM bereinigen
        raw_authors    = match_authors(transcript_text, document_type=document_type)
        raw_recipients = match_recipients(transcript_text, mentioned_persons)

        # Baue temp_authors (Person-Instanzen) auf – raw_authors kann Person, dict oder Liste sein
        temp_authors = []
        if isinstance(raw_authors, Person):
            temp_authors = [raw_authors]
        elif isinstance(raw_authors, dict) and raw_authors.get("forename"):
            temp_authors = [Person.from_dict(raw_authors)]
        elif isinstance(raw_authors, list):
            temp_authors = [
                p if isinstance(p, Person) else Person.from_dict(p)
                for p in raw_authors
            ]

        # Baue temp_recipients (Person-Instanzen) auf – raw_recipients kann Person, dict oder Liste sein
        temp_recipients = []
        if isinstance(raw_recipients, Person):
            temp_recipients = [raw_recipients]
        elif isinstance(raw_recipients, dict) and raw_recipients.get("forename"):
            temp_recipients = [Person.from_dict(raw_recipients)]
        elif isinstance(raw_recipients, list):
            temp_recipients = [
                p if isinstance(p, Person) else Person.from_dict(p)
                for p in raw_recipients
            ]

        # LLM-Finalisierung von Autoren und Empfängern
                # ——— LLM-Finalisierung ———
        # 1) Autoren und Empfänger heuristisch bestimmen
        authors, recipients = infer_authors_recipients(transcript_text, document_type, mentioned_persons)

        # 2) Temporäres Dokument für Einbindung in mentioned_persons
        temp_doc = BaseDocument(
            authors=            authors,
            recipients=         recipients,
            mentioned_persons=  mentioned_persons
        )
        ensure_author_recipient_in_mentions(temp_doc, transcript_text)

        # 3) Rollen für Autor:innen nachholen
        author_dicts = [
            {
                "forename": a.forename,
                "familyname": a.familyname,
                "alternate_name": a.alternate_name,
                "title": a.title,
                "nodegoat_id": a.nodegoat_id
            }
            for a in authors
        ]
        enriched = assign_roles_to_known_persons(author_dicts, transcript_text)
        enriched_authors = [
            e if isinstance(e, Person) else Person.from_dict(e)
            for e in enriched
        ]

        for auth_obj, enrich_obj in zip(authors, enriched_authors):
            if enrich_obj.role:
                auth_obj.role        = enrich_obj.role
                auth_obj.role_schema = enrich_obj.role_schema
                print(f"[DEBUG] role_schema = {auth_obj.role_schema}")

        # 4) Finale Daten übernehmen
        mentioned_persons = temp_doc.mentioned_persons

        # die Rollen an den gleichen Instanzen.

        # 5) mentionete Personen übernehmen
        mentioned_persons = temp_doc.mentioned_persons

        
        # 6) Custom-Tags extrahieren
        role_input_persons = load_known_persons_from_csv()
        role_input_persons = assign_roles_to_known_persons(role_input_persons, transcript_text)
        print("[DEBUG] Rollenmatching aus Fließtext:")
        for p in role_input_persons:
            if p.get("role"):
                print(f" → {p['forename']} {p['familyname']}: {p['role']} ({p['role_schema']})")

        # 6a) Custom-Tags extrahieren mit angereicherten Personeninfos
        custom_data = extract_custom_attributes(root, known_persons=role_input_persons)
        print("[DEBUG] Extracted custom roles:", custom_data["roles"])
        standalone = extract_standalone_roles(transcript_text)
        print("[DEBUG] Standalone roles from text:", standalone)



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
        for author in authors:
            # 1) Suche passenden enriched-Eintrag via Nodegoat-ID oder Vor-/Nachname
            match = next(
                (
                    p for p in enriched_persons
                    if (author.nodegoat_id and author.nodegoat_id == p.get("nodegoat_id"))
                    or (author.forename == p.get("forename") and author.familyname == p.get("familyname"))
                ),
                None
            )
            if not match:
                print(f"[DEBUG] Kein Match für Autor {author.forename} {author.familyname}")
                continue

            # 2) Fülle nur leere Felder in author aus match auf
            for attr in [
                "forename", "familyname", "title", "role",
                "associated_place", "associated_organisation", "nodegoat_id"
            ]:
                val_author = getattr(author, attr) or ""
                val_match  = match.get(attr) or ""
                if not val_author.strip() and val_match.strip():
                    setattr(author, attr, val_match)
                    print(f"[DEBUG] Ergänze {attr!r} bei {author.forename} {author.familyname}: {val_match!r}")
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

        # 11) Orte verarbeiten - erfolgt in einem einzigen Block weiter unten
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

        if not enriched_persons:
            print("[WARN] Keine enriched_persons – Rollen-Mapping übersprungen.")
        else:
            for author in final_authors:
                mapped = False
                for p in enriched_persons:
                    same_name = (
                        author.forename == p["forename"]
                        and author.familyname == p["familyname"]
                    )
                    same_id = (
                        author.nodegoat_id
                        and author.nodegoat_id == p.get("nodegoat_id")
                    )
                    if same_name or same_id:
                        author.role = p.get("role", "")
                        print(f"[DEBUG] Autor {author.forename} {author.familyname} → Rolle: {author.role!r}")
                        mapped = True
                        break
                if not mapped:
                    print(f"[DEBUG] Autor {author.forename} {author.familyname} – keine Rolle gefunden.")

    

        # 14) BaseDocument zusammenbauen
        doc = BaseDocument(
            object_type             = "Dokument",
            attributes              = metadata_info,
            content_transcription   = transcript_text,
            authors                 = authors,
            recipients              = recipients,
            mentioned_persons       = mentioned_persons,
            mentioned_organizations = mentioned_organizations,
            mentioned_places        = mentioned_places,
            mentioned_dates         = custom_data["dates"],
            content_tags_in_german  = [],
            creation_date           = "",
            creation_place          = "",
            document_type           = document_type,
            document_format         = ""
        )

        

        # ——— Hier die neue Rollenanreicherung einfügen ———
        # (1) Hole die Autoren-Dicts
        author_dicts = [
            {
                "forename": a.forename,
                "familyname": a.familyname,
                "alternate_name": a.alternate_name,
                "title": a.title,
                "nodegoat_id": a.nodegoat_id
            }
            for a in doc.authors
        ]
        # (1b) Hole die Empfänger-Dicts
        recipient_dicts = [{
            "forename": r.forename,
            "familyname": r.familyname,
            "alternate_name": r.alternate_name,
            "title": r.title,
            "nodegoat_id": r.nodegoat_id
        } 
        for r in doc.recipients
        ]
        enriched_recipients = assign_roles_to_known_persons(recipient_dicts, transcript_text)
        for rec, info in zip(doc.recipients, enriched_recipients):
            role = info.get("role") if isinstance(info, dict) else info.role
            schema = info.get("role_schema") if isinstance(info, dict) else getattr(info, "role_schema", "")
            if role:
                rec.role = role

                # Only update role_schema if it's empty or the source is role_only
                role_only_source = (isinstance(info, dict) and info.get("confidence") == "role_only") or \
                                   (hasattr(info, "confidence") and info.confidence == "role_only")

                if not getattr(rec, "role_schema", "") or role_only_source:
                    rec.role_schema = schema
                    print(f"[DEBUG] Updated role_schema for recipient {rec.forename} {rec.familyname}: '{schema}'")
                else:
                    print(f"[DEBUG] Kept existing role_schema for recipient {rec.forename} {rec.familyname}: '{rec.role_schema}'")
        # (2) Mappe Rollen und Schemas
        enriched = assign_roles_to_known_persons(author_dicts, transcript_text)
        # (3) Übertrage role & role_schema zurück auf die Person-Objekte
        for author, info in zip(doc.authors, enriched):
            role = info.get("role") if isinstance(info, dict) else info.role
            schema = info.get("role_schema") if isinstance(info, dict) else getattr(info, "role_schema", "")
            if role:
                author.role = role

                # Only update role_schema if it's empty or the source is role_only
                role_only_source = (isinstance(info, dict) and info.get("confidence") == "role_only") or \
                                   (hasattr(info, "confidence") and info.confidence == "role_only")

                if not getattr(author, "role_schema", "") or role_only_source:
                    author.role_schema = schema
                    print(f"[DEBUG] Updated role_schema for {author.forename} {author.familyname}: '{schema}'")
                else:
                    print(f"[DEBUG] Kept existing role_schema for {author.forename} {author.familyname}: '{author.role_schema}'")

        # Debug: Kontrolle, ob es geklappt hat
        print("[DEBUG] Final authors with roles:")
        for a in doc.authors:
            print(f" → {a.forename} {a.familyname}: role={a.role!r}, schema={a.role_schema!r}")

        # Konflikt-Log Pfad definieren (einmalig pro Seite)
        conflict_log_path = Path(OUTPUT_DIR_UNMATCHED) / f"conflict_log_{seven_digit_folder}_{subdir}.json"
                
        # Validiere das Dokument vor dem Speichern
        validation_result = validate_extended(doc)
        if validation_result.get("errors"):
            print(f"[WARN] Validierungsfehler im Dokument {full_doc_id}:")
            for err_type, errors in validation_result["errors"].items():
                print(f"  - {err_type}: {', '.join(errors)}")
        
        # 15) Dokument speichern mit Fehlerbehandlung
        output_path = os.path.join(OUTPUT_DIR, f"{full_doc_id}.json")
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(doc.to_json(indent=2))
            print(f"Gespeichert: {output_path}")
        except Exception as e:
            print(f"[ERROR] Fehler beim Speichern von {output_path}: {e}")

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

def process_transkribus_directory(transkribus_dir: str):
    place_m = PlaceMatcher(PLACE_CSV_PATH)
    org_list = load_organizations_from_csv(ORG_CSV_PATH)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for folder in os.listdir(transkribus_dir):
        if not folder.isdigit():
            continue
        process_subdirectories(folder, transkribus_dir, place_m, org_list)
def extract_page_number_from_filename(filename: str) -> str:
    match = re.search(r"p(\d+)", filename, re.IGNORECASE)
    return match.group(1) if match else "001"

def log_unmatched_entities(doc_id: str, unmatched_persons, unmatched_places, unmatched_roles, unmatched_organizations):
    log_dir = os.path.join(OUTPUT_DIR, "unmatched_logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{doc_id}_unmatched_log.txt")
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write("=== UNMATCHED PERSONS ===\n")
        for p in unmatched_persons:
            log_file.write(str(p) + "\n")
        log_file.write("\n=== UNMATCHED PLACES ===\n")
        for k, v in unmatched_places.items():
            log_file.write(f"{k}: {v}\n")
        log_file.write("\n=== UNMATCHED ROLES ===\n")
        for r in unmatched_roles:
            log_file.write(str(r) + "\n")
        log_file.write("\n=== UNMATCHED ORGANIZATIONS ===\n")
        for o in unmatched_organizations:
            log_file.write(str(o) + "\n")
    print(f"[INFO] Unmatched-Daten geloggt nach: {log_path}")

def debug_print_standalone_roles(transcript_text: str, roles: List[Dict[str, Any]]):
    print("\n[DEBUG] Gefundene Einzelrollen im Transkript:")
    for r in roles:
        print(" →", r.get("role"), "—", r.get("forename", ""), r.get("familyname", ""))
        context_lines = [line for line in transcript_text.splitlines() if r["role"] in line]
        for cl in context_lines:
            print("     Kontext:", cl)


def process_subdirectories(folder: str, base_path: str, place_m: PlaceMatcher, org_list: List[Dict[str, Any]]):
    for subdir in os.listdir(os.path.join(base_path, folder)):
        if not subdir.startswith("Akte_"):
            continue
        page_dir = os.path.join(base_path, folder, subdir, "page")
        if not os.path.isdir(page_dir):
            continue
        for xml_file in sorted(os.listdir(page_dir)):
            if xml_file.endswith("_preprocessed.xml"):
                process_single_xml(os.path.join(page_dir, xml_file), folder, subdir, xml_file, place_m, org_list)

def prepare_persons_and_infer(
    raw_persons: List[Dict[str, Any]],
    transcript: str,
    folder: str,
    document_type: str
) -> Tuple[List[Person], List[Person], List[Person]]:
    """
    1) split & enrich via split_and_enrich_persons  
    2) extract inline roles  
    3) merge tokens + roles  
    4) dedupe & group into Person-objects  
    5) infer authors & recipients  

    Returns: (grouped_persons, authors, recipients)
    """
    # 1) split & fuzzy-match
    matched, _ = split_and_enrich_persons(raw_persons, transcript, folder, KNOWN_PERSONS)

    # 2) inline-role-match on the matched persons
    # prepare dicts for assign_roles
    matched_dicts = [p.__dict__ if isinstance(p, Person) else p for p in matched]
    roles = assign_roles_to_known_persons(matched_dicts, transcript)

    # 3) merge matched tokens with roles
    combined = matched_dicts[:]
    for r in roles:
        if not any(
            r.forename == p.get("forename") and r.familyname == p.get("familyname")
            for p in combined
        ):
            combined.append(r.__dict__ if isinstance(r, Person) else r)

    # 4) dedupe & group
    grouped = deduplicate_and_group_persons(combined)

    # 5) infer authors/recipients using the fully enriched persons
    authors, recipients = infer_authors_recipients(transcript, document_type, grouped)
    return grouped, authors, recipients


def extract_and_prepare_persons(
    raw_persons: List[Dict[str, Any]],
    transcript: str,
    folder: str,
    document_type: str
) -> Tuple[List[Person], List[Person], List[Person]]:
    """
    1) split & enrich → fuzzy‐match tokens
    2) extract inline roles on matched
    3) merge tokens + roles (no duplicate names)
    4) dedupe & group → List[Person]
    5) infer authors & recipients
    Returns: (mentioned_persons, authors, recipients)
    """
    # 1) split & fuzzy-match raw custom persons
    matched, _ = split_and_enrich_persons(raw_persons, transcript, folder, KNOWN_PERSONS)
    matched_dicts = [p.__dict__ if isinstance(p, Person) else p for p in matched]

    # 2) inline roles from text using matched as context
    roles = assign_roles_to_known_persons(matched_dicts, transcript)

    # 3) merge matched tokens with inline roles
    combined = matched_dicts[:]
    for r in roles:
        if not any(
            r.forename == p.get("forename") and r.familyname == p.get("familyname")
            for p in combined
        ):
            combined.append(r.__dict__ if isinstance(r, Person) else r)

    # 4) dedupe & group → returns List[Person]
    grouped = deduplicate_and_group_persons(combined)

    person_dicts = [p.to_dict() for p in grouped]
    enriched = assign_roles_to_known_persons(person_dicts, transcript)
    for person, info in zip(grouped, enriched):
        # Wenn assign_roles eine Rolle gefunden hat, übernehmen
        if isinstance(info, dict) and info.get("role"):
            person.role = info["role"]
            person.role_schema = info.get("role_schema", "")
            print(f"[DEBUG] role_schema = {person.role_schema}")
        elif isinstance(info, Person) and info.role:
            person.role = info.role
            person.role_schema = info.role_schema if hasattr(info, "role_schema") else ""
            print(f"[DEBUG] person.role_schema from Person = {person.role_schema}")

    # 5) infer authors + recipients
    authors, recipients = infer_authors_recipients(transcript, document_type, grouped)
    recipients = sorted(
        recipients,
        key=lambda r: getattr(r, "recipient_score", 0),
        reverse=True
    )
    print("[DEBUG] after infer_authors_recipients → authors:", 
          [f"{a.forename} {a.familyname} (role_schema={a.role_schema})" for a in authors])
    print("[DEBUG] after infer_authors_recipients → recipients:", 
          [f"{r.forename} {r.familyname} (role_schema={r.role_schema})" for r in recipients])

    inline_recipients = [p for p in grouped if p.role_schema.lower() == "recipient"]
    recipients = merge_person_lists(recipients, inline_recipients)
    recipients = [
        r for r in recipients
        if r.forename.strip() or r.familyname.strip()
    ]
    return grouped, authors, recipients

def merge_person_lists(base: List[Person], extras: List[Person]) -> List[Person]:
    """
    Fügt alle Personen aus `extras` zu `base` hinzu, 
    sofern sie nicht schon in `base` (gleiches nodegoat_id oder Name) vorkommen.
    """
    merged = list(base)
    for p in extras:
        already = False
        for q in merged:
            if p.nodegoat_id and q.nodegoat_id:
                if p.nodegoat_id == q.nodegoat_id:
                    already = True
                    break
            else:
                if p.forename == q.forename and p.familyname == q.familyname:
                    already = True
                    break
        if not already:
            merged.append(p)
    return merged


def process_single_xml(
    xml_path: str,
    folder: str,
    subdir: str,
    xml_file: str,
    place_m: PlaceMatcher,
    org_list: List[Dict[str, Any]]
):
    print(f"Verarbeite Datei: {xml_file}")

    # 1) XML parsen
    root = ET.parse(xml_path).getroot()

    # 2) Metadaten + document_type
    metadata = extract_metadata_from_xml(root)
    document_id = f"{folder}_{subdir}"
    metadata["document_type"] = get_document_type(document_id, xml_path)

    # 3) Transkript
    transcript = extract_text_from_xml(root)
    if not transcript or len(transcript.strip()) < 10:
        print(f"[WARN] Leeres oder zu kurzes Transkript: {xml_file}")
        return

    # 4) Personen extrahieren, zusammenführen und Autoren/Empfänger ermitteln
    custom_data = extract_custom_attributes(root)
    mentioned_persons = custom_data.get("persons", [])
    print("[DEBUG] custom_data['roles'] in process_single_xml:", custom_data["roles"])

    # 5) Personen extrahieren, zusammenführen und Autoren/Empfänger ermitteln
    mentioned_persons, authors, recipients = extract_and_prepare_persons(
        custom_data["persons"],
        transcript,
        folder,
        metadata["document_type"]
    )
    recipients = sorted(
        recipients,
        key=lambda r: getattr(r, "recipient_score", 0),
        reverse=True
    )

    print(f"Recipients in Process–Single_XML {recipients}")
    assert isinstance(authors, list)
    assert isinstance(recipients, list)

    # 6) Custom-Rollen aus custom_data auf Authors & Recipients mappen
    for role_entry in custom_data.get("roles", []):
        role_raw = role_entry.get("raw", "").strip()
        for person in authors + recipients:
            if role_raw.lower() in f"{person.forename} {person.familyname}".lower():
                person.role = person.role or role_raw
                person.role_schema = person.role_schema or map_role_to_schema_entry(role_raw)
                print(f"[DEBUG] Assigned custom role '{role_raw}' to {person.forename} {person.familyname}")


    # 7) Sicherstellen, dass alle Authors/Recipients in mentioned_persons sind
    temp_doc = BaseDocument(
        authors=authors,
        recipients=recipients,
        mentioned_persons=mentioned_persons
    )
    ensure_author_recipient_in_mentions(temp_doc, transcript)
    authors, recipients = temp_doc.authors, temp_doc.recipients

    # 8) Rollen aus mentioned_persons final übernehmen
    for person in authors + recipients:
        for mp in temp_doc.mentioned_persons:
            if person.forename == mp.forename and person.familyname == mp.familyname:
                person.role        = mp.role
                person.role_schema = mp.role_schema
                break

    authors, recipients = temp_doc.authors, temp_doc.recipients
    mentioned_persons = temp_doc.mentioned_persons


    # 9) Organisationen matchen (und duplikatfrei halten)
    raw_orgs = custom_data.get("organizations", [])
    matched_orgs = match_organization_entities(raw_orgs, org_list)
    seen = set()
    unique_orgs = []
    for o in matched_orgs:
        key = (o.get("nodegoat_id"), o.get("name"))
        if key not in seen:
            seen.add(key)
            unique_orgs.append(o)

    # 10) Orte & Daten
    mentioned_places = mentioned_places_from_custom_data(
        custom_data, document_id, place_m, get_place_name
    )
    mentioned_dates = custom_data.get("dates", [])

    # I) BaseDocument zusammenbauen
    doc = BaseDocument(
        object_type="Dokument",
        attributes=metadata,
        content_transcription=transcript,
        authors=authors,
        recipients=recipients,
        mentioned_persons=mentioned_persons,
        mentioned_organizations=[
            Organization(
                name=o["name"],
                type=o.get("type", ""),
                nodegoat_id=o.get("nodegoat_id", ""),
                alternate_names=o.get("alternate_names", []),
                feldpostnummer=o.get("feldpostnummer", ""),
                match_score=o.get("match_score"),
                confidence=o.get("confidence", "")
            )
            for o in unique_orgs
        ],
        mentioned_places=mentioned_places,
        mentioned_dates=mentioned_dates,
        content_tags_in_german=[],
        creation_date="",
        creation_place="",
        document_type=metadata["document_type"],
        document_format=""
    )
    def mark_unmatched_persons(doc):
        for group_name in ["authors", "recipients", "mentioned_persons"]:
            for person in getattr(doc, group_name, []):
                if not person.nodegoat_id and (person.match_score is None or person.match_score == 0):
                    person.confidence = person.confidence or "unmatched"
                    print(f"[UNMATCHED] {group_name}: {person.forename} {person.familyname} → keine Nodegoat-ID, Score={person.match_score}")
    mark_unmatched_persons(doc)


    # Die erweiterte postprocess_roles-Funktion wird hier angewendet.
    # Dies stellt sicher, dass das familyname-Feld konsistent verarbeitet wird.
    # Die Funktion verifiziert und korrigiert alle Rollen-Personen-Zuordnungen im Dokument.
    postprocess_roles(doc)
    enrich_final_recipients(doc)
    print("[DEBUG] mentioned_persons nach enrich_final_recipients:", [
    f"{p.forename} {p.familyname} (score={getattr(p, 'recipient_score', '-')})"
    for p in doc.mentioned_persons
    ])
    # Nach Enrichment: Stelle sicher, dass alle enriched Recipients in mentioned_persons enthalten sind
    for r in doc.recipients:
        if r not in doc.mentioned_persons:
            doc.mentioned_persons.append(r)

    # Dann: recipients neu setzen anhand recipient_score
    doc.recipients = [
        p for p in doc.mentioned_persons
    if getattr(p, "recipient_score", 0) > 0
]


    # II) JSON speichern
    out_name = xml_file.replace("_preprocessed.xml", ".json")
    out_path = os.path.join(OUTPUT_DIR, f"{folder}_{subdir}_{out_name}")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(doc.to_json(indent=2))
    print(f"[OK] Gespeichert: {out_path}")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(doc.to_json(indent=2))


    needs_review_persons = [
        p.to_dict()
        for p in doc.mentioned_persons
        if getattr(p, "needs_review", False)
    ]

    if needs_review_persons:
        unmatched_dir = Path(OUTPUT_DIR) / "unmatched"
        unmatched_dir.mkdir(parents=True, exist_ok=True)

        unmatched_file = unmatched_dir / "unmatched_persons.json"

        # Bestehende Einträge laden (falls vorhanden)
        if unmatched_file.exists():
            with open(unmatched_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        else:
            existing = []

        # Kombinieren und Duplikate vermeiden (nach Vorname+Nachname)
        combined = {
            (p.get("forename", ""), p.get("familyname", ""), p.get("role", "")): p
            for p in existing + needs_review_persons
            if isinstance(p, dict)
        }

        with open(unmatched_file, "w", encoding="utf-8") as f:
            json.dump(list(combined.values()), f, ensure_ascii=False, indent=2)
        
        print(f"[UNMATCHED] {len(needs_review_persons)} neue Personen in unmatched_persons.json ergänzt")


def deduplicate_and_group_persons(persons: List[Dict[str, Any]]) -> List[Person]:
    """
    Enhanced deduplication that properly handles duplicate persons with same nodegoat_id or name,
    keeping only the entry with highest match_score.
    """
    # First, group by nodegoat_id or name
    nodegoat_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    name_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    role_only_items: List[Dict[str, Any]] = []

    # Step 1: Sort entries into appropriate buckets
    for p in persons:
        # Handle entries with nodegoat_id
        if p.get("nodegoat_id"):
            nodegoat_groups[p.get("nodegoat_id")].append(p)
        # Handle entries with name but no nodegoat_id
        elif p.get("forename") or p.get("familyname"):
            name_key = f"{p.get('forename', '')}|{p.get('familyname', '')}"
            name_groups[name_key].append(p)
        # Handle role-only entries (no name, no id)
        elif p.get("role"):
            role_only_items.append(p)

    final = []

    # Step 2: Process nodegoat_id groups (highest priority)
    for nodegoat_id, entries in nodegoat_groups.items():
        # Get entry with highest match_score
        best = max(entries, key=lambda x: float(x.get("match_score", 0) or 0))
        # Combine roles from all entries
        combined_roles = "; ".join(sorted(set(r.get("role", "") for r in entries if r.get("role"))))
        best["role"] = combined_roles
        best["mentioned_count"] = sum(int(e.get("mentioned_count", 1) or 1) for e in entries)
        # Ensure recipient_score exists
        if "recipient_score" not in best:
            best["recipient_score"] = 0
        # Convert to Person object and add to final list
        final.append(Person.from_dict(best))

        # Debug output
        print(f"[DEBUG] Grouped by nodegoat_id {nodegoat_id}: {best.get('forename')} {best.get('familyname')}, Score: {best.get('match_score')}")

    # Step 3: Process name groups (second priority)
    for name_key, entries in name_groups.items():
        # Get entry with highest match_score
        best = max(entries, key=lambda x: float(x.get("match_score", 0) or 0))
        # Combine roles from all entries
        combined_roles = "; ".join(sorted(set(r.get("role", "") for r in entries if r.get("role"))))
        best["role"] = combined_roles
        best["mentioned_count"] = sum(int(e.get("mentioned_count", 1) or 1) for e in entries)
        # Ensure recipient_score exists
        if "recipient_score" not in best:
            best["recipient_score"] = 0
        # Convert to Person object and add to final list
        final.append(Person.from_dict(best))

        # Debug output
        print(f"[DEBUG] Grouped by name {name_key}: {best.get('forename')} {best.get('familyname')}, Score: {best.get('match_score')}")

    # Step 4: Add role-only entries (lowest priority)
    for role_entry in role_only_items:
        # Handle role-only entries
        if "recipient_score" not in role_entry:
            role_entry["recipient_score"] = 0
        final.append(Person.from_dict(role_entry))

        # Debug output
        print(f"[DEBUG] Added role-only entry: {role_entry.get('role')}")

    print("\n[DEBUG] Finale erwähnte Personen nach Deduplikation:")
    for p in final:
        print(f" → {p.forename} {p.familyname}, Rolle: {p.role}, ID: {p.nodegoat_id}, Score: {p.match_score}, Count: {p.mentioned_count}")

    return final

def infer_authors_recipients(
    text: str,
    doc_type: Optional[str],
    persons: List[Person]
) -> Tuple[List[Person], List[Person]]:
    """
    Ermittelt Autoren und Empfänger aus dem Text, sorgt dafür, 
    dass sie Person-Instanzen sind, und gibt Debug-Infos aus.
    """
    # 1) Roh-Extraktion
    raw_author    = match_authors(text, document_type=doc_type, mentioned_persons=persons)
    raw_recipient = match_recipients(text, mentioned_persons=persons)

    # 1b) Leeren dict-Placeholder für recipient entfernen
    if isinstance(raw_recipient, dict) and not (raw_recipient.get("forename") or raw_recipient.get("familyname")):
        raw_recipient = []

    # 2) In Listen umwandeln und Dictionaires zu Person konvertieren
    def to_person_list(raw):
        result = []
        if isinstance(raw, Person):
            result = [raw]
        elif isinstance(raw, dict):
            # nur dann, wenn Name vorhanden
            if raw.get("forename") or raw.get("familyname"):
                result = [Person.from_dict(raw)]
        elif isinstance(raw, list):
            for p in raw:
                if isinstance(p, Person):
                    result.append(p)
                elif isinstance(p, dict) and (p.get("forename") or p.get("familyname")):
                    result.append(Person.from_dict(p))
        return result

    authors_list    = to_person_list(raw_author)
    recipients_list = to_person_list(raw_recipient)

    # 3) Debug-Ausgabe direkt nach Extraktion
    print("[DEBUG] infer_authors_recipients → authors_list:", 
          [f"{a.forename} {a.familyname} (role={a.role}, id={a.nodegoat_id})"
           for a in authors_list])
    print("[DEBUG] infer_authors_recipients → recipients_list:", 
          [f"{r.forename} {r.familyname} (role={r.role}, id={r.nodegoat_id})"
           for r in recipients_list])

    # 4) Temporäres Dokument für das In-Mentions Einpflegen
    temp_doc = BaseDocument(
        authors=            authors_list,
        recipients=         recipients_list,
        mentioned_persons=  persons
    )

    # 5) Autoren und Empfänger in mentioned_persons aufnehmen
    print("[DEBUG] recipients vor ensure_author_recipient_in_mentions:", 
      [f"{r.forename} {r.familyname}" for r in temp_doc.recipients])

    ensure_author_recipient_in_mentions(temp_doc, text)

    print("[DEBUG] recipients nach ensure_author_recipient_in_mentions:", 
        [f"{r.forename} {r.familyname}" for r in temp_doc.recipients])


    # 6) Debug direkt vor Rückgabe
    print("[DEBUG] infer_authors_recipients → final authors:", 
          [f"{a.forename} {a.familyname} (role={a.role}, id={a.nodegoat_id})"
           for a in temp_doc.authors])
    print("[DEBUG] infer_authors_recipients → final recipients:", 
          [f"{r.forename} {r.familyname} (role={r.role}, id={r.nodegoat_id})"
           for r in temp_doc.recipients])
    print("[DEBUG] raw_recipient:", raw_recipient)

    return temp_doc.authors, temp_doc.recipients


def main():
    process_transkribus_directory(TRANSKRIBUS_DIR)

if __name__ == "__main__":
    main()
