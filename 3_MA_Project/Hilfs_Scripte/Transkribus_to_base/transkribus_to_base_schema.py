"""
Extraktion von Basisinformationen aus Transkribus XML-Dateien und Konvertierung in das Basis-Schema.

Dieses Skript liest Transkribus XML-Dateien, extrahiert die Metadaten und den Text
und konvertiert sie in das in WORKFLOW.md definierte Basis-Schema unter Verwendung der
in document_schemas.py definierten Klassen für Objektorientierung und Datenvalidierung.
"""

# --------------- Modulpfade vorbereiten ---------------
import os
import sys

MODULE_DIR = os.path.join(os.path.dirname(__file__), "Module")
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)

# --------------- Externe Abhängigkeiten ---------------
import json
import re
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional, Union
import sys
sys.path.insert(0, "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Hilfs_Scripte/Transkribus_to_base")

import pandas as pd
try:
    import spacy
    from rapidfuzz import fuzz, process
except ImportError:
    print("Warning: Some required libraries are not installed. Limited functionality available.")
    spacy = None
    fuzz = None
    process = None


# --------------- Eigene Module (aus /Module) ---------------
# Importiere alle Klassen und Funktionen über das __init__ von Module
from .Module import (
    # document_schemas.py
    BaseDocument, Person, Place, Event, Organization,
    
    # person_matcher.py
    match_person, KNOWN_PERSONS, deduplicate_persons, normalize_name,
    fuzzy_match_name, load_known_persons_from_csv,
    
    # type_matcher.py
    get_document_type,

    # Assigned_Roles_Module.py
    assign_roles_to_known_persons,

    #LLM Enricher
    llm_enricher,

    #place_matcher.py
    PlaceMatcher,

    #validate_module.py
    validate_extended,

    #organization_matcher.py
    load_organizations_from_csv,
    match_organization_from_text,

)

#=== LLM API Key für Enrichment ===
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# === Pfadkonfiguration ===
TRANSKRIBUS_DIR = "/Users/svenburkhardt/Desktop/Transkribus_test_In"            #Testdatensatz
OUTPUT_DIR = "/Users/svenburkhardt/Desktop/Transkribus_test_Out"
OUTPUT_CSV_PATH = os.path.join(OUTPUT_DIR, "known_persons_output.csv")
#CSV_PATH_KNOWN_PERSONS = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Datenbank_Metadaten_Stand_08.04.2025/Metadata_Person-Metadaten_Personen.csv"
CSV_PATH_NODEGOAT = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-person.csv"
CSV_PATH_METADATA = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Datenbank_Metadaten_Stand_08.04.2025/Metadata_Person-Metadaten_Personen.csv"
LOG_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/new_persons.log"

# Lade deutsches spaCy-Modell
try:
    nlp = spacy.load("de_core_news_sm")
except:
    print("Warnung: SpaCy-Modell 'de_core_news_sm' nicht gefunden.")
    nlp = None

# === Bekannte Personen laden ==============================
# 1)  bereits geladene Liste aus person_matcher
known_persons_list   = KNOWN_PERSONS            # List[Dict[str, str]]

# 2) (Optional) DataFrame‑Variante, falls tabellarisch arbeiten
all_known_persons_df = pd.DataFrame(known_persons_list)


# === Bekannte Orte Laden ===
PLACE_CSV_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-place.csv"
place_matcher = PlaceMatcher(PLACE_CSV_PATH)
print("[DEBUG] Geladene Ortsdaten aus CSV:")
print(place_matcher.places_df.head())


# === Bekannte Organisationen Laden ===
CSV_PATH_ORGANIZATIONS = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Nodegoat_Export/export-organisationen.csv"
known_organizations = load_organizations_from_csv(CSV_PATH_ORGANIZATIONS)




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
        if person_exists_in_known_list(forename, familyname, KNOWN_PERSONS):
            print(f"{forename} {familyname} existiert bereits in der CSV.")
            return
    
        # Wenn nicht, füge die Person hinzu
        new_row = {
            "forename": forename,
            "familyname": familyname,
            "Alternativer Vorname": "",
            "[Wohnort] Location Reference": "",
            "[Geburt] Date Start": "",
            "[Tod] Date Start": "",
            "db:deathPlace": "",
            "Lfd_No.": f"{len(known_persons_df) + 1:05d}"  # Neue ID mit führenden Nullen
    }
    known_persons_df = pd.concat([known_persons_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Speichern der aktualisierten CSV
    known_persons_df.to_csv(csv_path, sep=";", index=False)
    
    # Aktualisieren der Liste bekannter Personen
    KNOWN_PERSONS.append((forename, familyname))
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

def fuzzy_match_person_in_list(forename: str, familyname: str, known_list: List[tuple], threshold: int = 90) -> Optional[tuple]:
    """
    Interne Funktion für Abwärtskompatibilität - verwendet das Tupel-Format (forename, familyname).
    Für neue Funktionen sollte die person_matcher.match_person Funktion verwendet werden.
    """
    best_match = None
    best_score = 0

    for known_forename, known_familyname in known_list:
        score = fuzz.token_sort_ratio(f"{forename} {familyname}", f"{known_forename} {known_familyname}")
        if score > best_score and score >= threshold:
            best_score = score
            best_match = (known_forename, known_familyname)

    return best_match

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
            
        # Sicherstellen, dass die nodegoat_id übertragen wird
        # Wenn nodegoat_id nicht direkt gesetzt ist, aber Person aus nodegoat kommt (source=nodegoat)
        if (not matched_person.get("nodegoat_id") and 
            matched_person.get("source") == "nodegoat" and 
            matched_person.get("id")):
            matched_person["nodegoat_id"] = matched_person["id"]
            
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
                forename += " " + " ".join(name_parts[1:-1])
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

#Alto-Namespace
NS_ALTO = {"alto": "http://www.loc.gov/standards/alto/ns-v4#"}
# === Transkribus-Dateien einlesen ===

def find_all_transkribus_xml_files(base_path: str) -> List[str]:
    """
    Sucht rekursiv nach XML-Dateien, sowohl in 'page' als auch 'alto' Unterordnern.
    """
    xml_files = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".xml"):
                xml_files.append(os.path.join(root, file))
    return xml_files

NS_PAGE = {"ns": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}
NS_ALTO = {"alto": "http://www.loc.gov/standards/alto/ns-v4#"}

def detect_xml_type_and_namespace(root: ET.Element):
    """
    Erkennt, ob es sich um PAGE oder ALTO handelt und gibt den passenden Namespace zurück.
    """
    tag = root.tag.lower()
    if "alto" in tag:
        return "ALTO", NS_ALTO
    elif "pcgts" in tag:
        return "PAGE", NS_PAGE
    return "UNKNOWN", {}


# Sicherstellen, dass das Ausgabeverzeichnis existiert
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# Normalisiserungsfunktion für Organisationen
def unify_organization_keys(orgs: list[dict]) -> list[dict]:
    unified = []
    for o in orgs:
        unified.append({
            "name": o.get("name", "").strip(),
            "alternateName": [a.strip() for a in o.get("alternateName", "").split(";") if a.strip()],
            "nodegoat_id": o.get("id", "").strip(),
            "wikidata_id": o.get("sameAs", "").strip(),
            "source": "nodegoat"
        })
    return unified

known_organizations = unify_organization_keys(known_organizations)


seen_place_ids = set()
def extract_custom_attributes(root: ET.Element) -> Dict[str, List[Dict[str, Any]]]:
    """
    Versucht, custom-Attribute aus den TextLine-Elementen zu extrahieren
    
    Args:
        root: XML-Root-Element
        
    Returns:
        Dictionary mit extrahierten Werten
    """
    result = {
        "persons": [],
        "organizations": [],
        "dates": [],
        "places": []
    }
    
    # Suche nach TextLine-Elementen mit custom-Attributen
    for text_line in root.findall(".//ns:TextLine", NS):
        custom_attr = text_line.get("custom", "")
        if not custom_attr:
            continue
        
        # Extrahiere Text aus diesem TextLine
        text_content = ""
        text_equiv = text_line.find(".//ns:TextEquiv/ns:Unicode", NS)
        if text_equiv is not None and text_equiv.text:
            text_content = text_equiv.text
        
        # Personen
        person_match = re.search(r"person\s+\{([^}]+)\}", custom_attr)
        if person_match and text_content:
            person_data = parse_custom_attributes(person_match.group(1))
            if "offset" in person_data and "length" in person_data:
                offset = int(person_data.get("offset", 0))
                length = int(person_data.get("length", 0))
                if offset < len(text_content) and offset + length <= len(text_content):
                    person_name = text_content[offset:offset+length]
                    
                    # Extrahiere Titel separat und bereinige Namen
                    # Funktion ist bereits über das Module-Paket importiert
                    cleaned_name, extracted_title = normalize_name(person_name)
                    forename, familyname = extract_name_with_spacy(cleaned_name)

                    
                    # Erstelle ein Person-Dictionary
                    person_dict = {
                        "forename": forename,
                        "familyname": familyname,
                        "role": "",
                        "associated_place": "",
                        "associated_organisation": "",
                        "alternate_name": ""  # Wichtig: Auch alternate_name setzen
                        
                    }
                    
                    # Füge die Person zum result hinzu
                    # Hier machen wir noch keine Deduplizierung oder Suche nach bekannten Personen
                    # Das passiert später in process_transkribus_file mit deduplicate_persons
                    result["persons"].append(person_dict)
        
        # Organisationen
        org_match = re.search(r"organization\s+\{([^}]+)\}", custom_attr)
        if org_match and text_content:
            org_data = parse_custom_attributes(org_match.group(1))
            if "offset" in org_data and "length" in org_data:
                offset = int(org_data.get("offset", 0))
                length = int(org_data.get("length", 0))
                if offset < len(text_content) and offset + length <= len(text_content):
                    org_name = text_content[offset:offset + length]

                    # Sicherstellen, dass org_name gesetzt ist
                    if org_name:
                        matched = match_organization_from_text(org_name, known_organizations)
                        if matched:
                            print(f"[DEBUG] ORG-MATCH: '{org_name}' → {matched}")
                            result["organizations"].append(matched)
                        else:
                            print(f"[DEBUG] ORG-NOMATCH: '{org_name}'")
                            result["organizations"].append({
                                "name": org_name,
                                "location": "",
                                "type": "",
                                "nodegoat_id": "",
                                "wikidata_id": ""
                            })
                    else:
                        print(f"[DEBUG] Kein org_name extrahiert.")
                else:
                    print(f"[ERROR] Ungültiger Offset oder Länge für Organisation: {org_data}")

        # Datumsangaben
        date_match = re.search(r"date\s+\{([^}]+)\}", custom_attr)
        if date_match and text_content:
            date_data = parse_custom_attributes(date_match.group(1))
            if "when" in date_data:
                # Formatiere das Datum im Format YYYY.MM.DD
                date_str = date_data.get("when", "")
                date_parts = date_str.split(".")
                if len(date_parts) == 3:
                    # Falls es bereits im DD.MM.YYYY Format ist
                    day, month, year = date_parts
                    formatted_date = f"{year}.{month}.{day}"
                elif len(date_parts) == 2:
                    # Falls es im MM.YYYY Format ist
                    month, year = date_parts
                    formatted_date = f"{year}.{month}"
                else:
                    # Versuche andere Formate
                    date_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
                    if date_match:
                        day, month, year = date_match.groups()
                        formatted_date = f"{year}.{month}.{day}"
                    elif re.match(r"\d{4}-\d{2}-\d{2}", date_str):
                        # ISO-Format (YYYY-MM-DD)
                        year, month, day = date_str.split("-")
                        formatted_date = f"{year}.{month}.{day}"
                    else:
                        formatted_date = date_str
                
                result["dates"].append(formatted_date)
        
        # Orte
        place_match = re.search(r"place\s+\{([^}]+)\}", custom_attr)
        if place_match and text_content:
            place_data = parse_custom_attributes(place_match.group(1))
            if "offset" in place_data and "length" in place_data:
                offset = int(place_data.get("offset", 0))
                length = int(place_data.get("length", 0))
                if offset < len(text_content) and offset + length <= len(text_content):
                    place_name = text_content[offset:offset + length]

                    # Groundtruth-Abgleich mit PlaceMatcher
                    try:
                        if place_matcher and place_name:
                            match_result = place_matcher.match_place(place_name)
                            if match_result:
                                matched_data = match_result.get("data", {})
                                matched_name = match_result.get("matched_name", "")
                                print(f"'{place_name}' → {matched_name} | ID: {matched_data.get('nodegoat_id', '')}")

                                alt_names_str = matched_data.get("alternate_place_name", "")
                                
                                # Eindeutige ID für Duplikatprüfung priorisieren: nodegoat > geonames > wikidata
                                unique_id = (matched_data.get("nodegoat_id") or
                                            matched_data.get("geonames_id") or
                                            matched_data.get("wikidata_id"))

                                if unique_id and unique_id not in seen_place_ids:
                                    seen_place_ids.add(unique_id)
                                    result["places"].append({
                                        "name": matched_data.get("name", place_name),
                                        "alternate_place_name": alt_names_str,
                                        "geonames_id": matched_data.get("geonames_id", ""),
                                        "wikidata_id": matched_data.get("wikidata_id", ""),
                                        "nodegoat_id": matched_data.get("nodegoat_id", ""),
                                        "original_input": place_name,
                                        "matched_name": matched_name,
                                        "match_score": match_result.get("score", None),
                                        "confidence": match_result.get("confidence", "unknown")
                                    })

                                elif unique_id in seen_place_ids:
                                    print(f"[DEBUG] Ort bereits verarbeitet → übersprungen: '{place_name}' (ID: {unique_id})")
                            else:
                                # Fallback, wenn kein Groundtruth-Match gefunden wurde
                                result["places"].append({
                                    "name": place_name,
                                    "alternate_place_name": "",
                                    "geonames_id": "",
                                    "wikidata_id": "",
                                    "nodegoat_id": "",
                                    "original_input": place_name,
                                    "matched_name": None,
                                    "match_score": None,
                                    "confidence": "none"
                                })

                    except Exception as e:
                        print(f"Fehler beim Ortsmatching für '{place_name}': {e}")
                        # Fehlerfall: Ort trotzdem sichern
                        result["places"].append({
                            "name": place_name,
                            "alternate_place_name": "",
                            "geonames_id": "",
                            "wikidata_id": "",
                            "nodegoat_id": "",
                            "original_input": place_name,
                            "matched_name": None,
                            "match_score": None,
                            "confidence": "error",
                            "error": str(e)
                        })

        return result


def clean_place_dict(place: dict) -> dict:
    """
    Entfernt nicht erlaubte Keys aus dem Orts-Dictionary für das Place-Schema.
    Behält alternate_place_name im ursprünglichen Format bei und korrigiert Geonames-IDs.
    """
    allowed_keys = ["name", "alternate_place_name", "geonames_id", "wikidata_id", "nodegoat_id", "type", "country"]
    result = {}
    for k in allowed_keys:
        if k in place:
            value = place[k] if pd.notna(place.get(k)) else ""
            if k == "geonames_id" and isinstance(value, str) and value.endswith('.0'):
                value = value[:-2]  # ".0" am Ende entfernen
            elif k != "alternate_place_name":
                value = str(value).strip()
            result[k] = value
    return result


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
def extract_places_spacy(text):
    places = set()
    if nlp:
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ == 'LOC':
                places.add(ent.text)
    return list(places)



def process_transkribus_file(
    xml_path: str,
    seven_digit_folder: str,
    subdir: str
) -> Optional[BaseDocument]:
    try:
        # 💡 Filename aus Pfad extrahieren
        transkribus_id = seven_digit_folder
        akte_folder    = subdir
        page_name      = os.path.splitext(os.path.basename(xml_path))[0]
        filename_for_type = f"{transkribus_id}_{akte_folder}_{page_name}"

        # Dokumenttyp bestimmen
        document_type = get_document_type(
            filename=filename_for_type,
            xml_path=xml_path,
            debug=True
        )
        print(f"[DEBUG] Dokumenttyp erkannt für {filename_for_type}: {document_type}")

        # XML parsen
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Metadaten & Transkript
        metadata_info     = extract_metadata_from_xml(root)
        metadata_info["document_type"] = document_type
        transcript_text   = extract_text_from_xml(root)

        # Organisationen matchen (hier musst du org_name und known_organizations setzen)
        matched_organizations = []
        # → Beispiel (wenn du einen org_name extrahierst):
        # match = match_organization_from_text(org_name, known_organizations)
        # if match:
        #     matched_organizations = [Organization(**match)]

        # Custom Data
        custom_data = extract_custom_attributes(root)
        if not custom_data:
            print(f"[ERROR] Keine custom_data in {xml_path}")
            return None
        spacy_places = extract_places_spacy(transcript_text)

        # Custom-Orte aus XML
        custom_places_names = {p['name'] for p in custom_data["places"]}

        # Neue Orte ergänzen, die nicht bereits durch custom erkannt wurden
        for place_name in spacy_places:
            if place_name not in custom_places_names:
                match_result = place_matcher.match_place(place_name)
                if match_result:
                    matched_data = match_result.get("data", {})
                    matched_name = match_result.get("matched_name", place_name)
                    unique_id = matched_data.get("nodegoat_id") or matched_data.get("geonames_id") or matched_data.get("wikidata_id")

                    custom_data["places"].append({
                        "name": matched_data.get("name", place_name),
                        "alternate_place_name": matched_data.get("alternate_place_name", ""),
                        "geonames_id": matched_data.get("geonames_id", ""),
                        "wikidata_id": matched_data.get("wikidata_id", ""),
                        "nodegoat_id": matched_data.get("nodegoat_id", ""),
                        "original_input": place_name,
                        "matched_name": matched_name,
                        "match_score": match_result.get("score", None),
                        "confidence": match_result.get("confidence", "spacy+matcher")
                    })
                else:
                    custom_data["places"].append({
                        "name": place_name,
                        "alternate_place_name": "",
                        "geonames_id": "",
                        "wikidata_id": "",
                        "nodegoat_id": "",
                        "original_input": place_name,
                        "matched_name": None,
                        "match_score": None,
                        "confidence": "spacy"
                    })

        # --- Personen deduplizieren und matchen ---
        all_persons    = custom_data.get("persons", [])
        unique_persons = deduplicate_persons(all_persons)

        # --- Organisationen matchen ---
        matched_organizations = []
        for org_mention in custom_data.get("organizations", []):
            name = org_mention.get("name", "").strip()
            if not name:
                continue
            org_match = match_organization_from_text(name, known_organizations)
            if org_match:
                matched_organizations.append(Organization(**org_match))

        # --- Rollen‑Enrichment ---
        role_inputs = [
            {
                "forename": p["forename"],
                "familyname": p["familyname"],
                "role": p.get("role", ""),
                "associated_organisation": p.get("associated_organisation", ""),
                "nodegoat_id": p.get("nodegoat_id", "")
            }
            for p in unique_persons
        ]
        enriched_person_dicts = assign_roles_to_known_persons(role_inputs, transcript_text)

        # --- Person‑Objekte bauen ---
        mentioned_persons = [
            Person(
                forename=d["forename"],
                familyname=d["familyname"],
                role=d.get("role", ""),
                associated_place=d.get("associated_place", ""),
                associated_organisation=d.get("associated_organisation", ""),
                nodegoat_id=d.get("nodegoat_id", "")
            )
            for d in enriched_person_dicts
        ]

        # BaseDocument zusammenbauen
        doc = BaseDocument(
            object_type="Dokument",
            attributes=metadata_info,
            content_transcription=transcript_text,
            mentioned_persons=mentioned_persons,
            mentioned_organizations=matched_organizations,
            mentioned_places=[Place(**clean_place_dict(pl)) for pl in custom_data["places"]],
            mentioned_dates=custom_data["dates"],
            content_tags_in_german=[],
            author=Person(),
            recipient=Person(),
            creation_date="",
            creation_place="",
            document_type=document_type,
            document_format=""
        )

        # Debug: erkannte Organisationen
        for org in matched_organizations:
            print(f"[DEBUG] Org‑Eintrag: {org}")
            print(f"[DEBUG] nodegoat_id: {org.nodegoat_id} | type: {type(org.nodegoat_id)}")
        print(f"[DEBUG] Organisationen erkannt: {[org.name for org in matched_organizations]}")

        return doc

    except Exception as e:
        print(f"Fehler bei der Verarbeitung von {xml_path}: {e}")
        return None

    

# Unmatched Places ohne Geonames-ID oder Nodegoat-ID werden seperat zur manuellen Überprüfung gespeichert
def export_unmatched_places_to_csv(output_dir: str, csv_filename: str = "unmatched_places_report.csv"):
    """
     Exportiert alle Orte ohne Groundtruth-ID in gruppierter Form:
    - Spalte 1: fehlender Ortsname
    - Spalte 2: Liste von Aktennamen mit Kontext in Klammern

    Args:
        output_dir (str): Pfad zum Ordner mit JSON-Dateien
        csv_filename (str): Dateiname der Output-CSV
    """
    from collections import defaultdict

    unmatched_places = defaultdict(list)

    for filename in os.listdir(output_dir):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(output_dir, filename)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            aktenname = filename.replace(".json", "")
            places = data.get("mentioned_places", [])
            full_text = data.get("content_transcription", "")

            for place in places:
                if not place.get("geonames_id") and not place.get("nodegoat_id"):
                    ort = place.get("name", "")
                    if not ort:
                        continue

                    context_sentence = ""
                    if full_text:
                        sentences = full_text.split(".")
                        for sentence in sentences:
                            if ort in sentence:
                                context_sentence = sentence.strip()
                                break

                    akteninfo = f"{aktenname} ({context_sentence})" if context_sentence else aktenname
                    unmatched_places[ort].append(akteninfo)

        except Exception as e:
            print(f"Fehler beim Verarbeiten von {filename}: {e}")

    # Speichern als CSV
    if unmatched_places:
        rows = []
        for ort, aktenliste in unmatched_places.items():
            rows.append({
                "ort": ort,
                "vorkommen": " | ".join(aktenliste)
            })
        df = pd.DataFrame(rows)
        output_path = os.path.join(output_dir, csv_filename)
        df.to_csv(output_path, sep=";", index=False)
        print(f"\n⚠️ Gruppierter Unmatched Places-Report gespeichert: {output_path}")
    else:
        print("\n✅ Keine unmatched Places gefunden.")
        
   
def main():
    print("Starte Extraktion von Transkribus-Daten mit Objektorientierung und Validierung...")
    processed_files = 0
    validated_files = 0
    validation_summary_data = []

    for seven_digit_folder in os.listdir(TRANSKRIBUS_DIR):
        folder_path = os.path.join(TRANSKRIBUS_DIR, seven_digit_folder)

        if not os.path.isdir(folder_path) or not seven_digit_folder.isdigit():
            continue

        for subdir in os.listdir(folder_path):
            subdir_path = os.path.join(folder_path, subdir)

            if not os.path.isdir(subdir_path) or not subdir.startswith("Akte_"):
                continue

            # ✨ Versuche zuerst "alto", wenn nicht vorhanden, fallback auf "page"
            preferred_folders = ["alto", "page"]
            page_folder = None
            for pf in preferred_folders:
                candidate = os.path.join(subdir_path, pf)
                if os.path.isdir(candidate):
                    page_folder = candidate
                    break

            if not page_folder:
                print(f"Kein 'alto' oder 'page' Ordner in {subdir_path}")
                continue

            for xml_file in os.listdir(page_folder):
                if not xml_file.endswith(".xml"):
                    continue

                xml_path = os.path.join(page_folder, xml_file)
                print(f"Verarbeite: Transkribus-ID {seven_digit_folder}, {subdir}, Datei {xml_file}")

                page_match = re.search(r"p(\d+)", xml_file, re.IGNORECASE)
                page_number = page_match.group(1) if page_match else "001"

                doc = process_transkribus_file(xml_path, seven_digit_folder, subdir)
                if doc is None:
                    print(f"[SKIP] Datei konnte nicht verarbeitet werden: {xml_path}")
                    continue

                # JSON ausgeben und validieren
                output_filename = f"{seven_digit_folder}_{subdir}_page{page_number}.json"
                output_path = os.path.join(OUTPUT_DIR, output_filename)

                validation_errors = doc.validate()
                validation_errors.update(validate_extended(doc))
                is_valid = len(validation_errors) == 0
                if is_valid:
                    validated_files += 1

                with open(output_path, "w", encoding="utf-8") as json_out:
                    json_out.write(doc.to_json())
                processed_files += 1

                print(f"JSON gespeichert: {output_path} (Validierung: {'Erfolgreich' if is_valid else 'Fehlgeschlagen'})")
                print(f"Verarbeitung abgeschlossen. {processed_files} Dateien wurden verarbeitet.")
                print(f"Davon {validated_files} Dokumente ohne Validierungsfehler und {processed_files - validated_files} mit Validierungsfehlern.")
                
                # Hier iterieren wir über die tatsächlich erkannten Personen im Dokument
                for p in doc.mentioned_persons:
                    print(f"{p.forename} {p.familyname} → Nodegoat‑ID: {p.nodegoat_id}")

    #from validation_module import generate_validation_summary
    #generate_validation_summary(validation_summary_data)

    # if OPENAI_API_KEY:
    #       print("\nStarte LLM-Enrichment der generierten JSON-Dateien...")
    #       llm_enricher.run_enrichment_on_directory(OUTPUT_DIR, api_key=OPENAI_API_KEY)                    #prudiziert vorerst ein zweites File, muss später überschreiben!
    # else:
    #       print("\nKein OpenAI API Key gefunden. LLM-Enrichment wird übersprungen.")
    # export_unmatched_places_to_csv(OUTPUT_DIR)

if __name__ == "__main__":
    main()