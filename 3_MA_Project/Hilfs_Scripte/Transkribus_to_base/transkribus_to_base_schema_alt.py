"""
Extraktion von Basisinformationen aus Transkribus XML‑Dateien und Konvertierung in das Basis‑Schema.

Dieses Skript liest Transkribus XML‑Dateien, extrahiert die Metadaten und den Text
und konvertiert sie in das in WORKFLOW.md definierte Basis‑Schema unter Verwendung der
in document_schemas.py definierten Klassen für Objektorientierung und Datenvalidierung.
"""
# --------------- Modulpfade vorbereiten ---------------
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
import json, re, time, xml.etree.ElementTree as ET

# Basis­verzeichnis = zwei Ebenen über diesem File  (…/3_MA_Project)
THIS_FILE = Path(__file__).resolve()
BASE_DIR  = THIS_FILE.parents[2]

MODULE_DIR = THIS_FILE.parent / "Module"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

# --------------- Externe Abhängigkeiten ---------------
import pandas as pd
import spacy
from rapidfuzz import fuzz, process

# --------------- Pfadkonfiguration ---------------
TRANSKRIBUS_DIR          = "/Users/svenburkhardt/Desktop/Transkribus_test_In"
OUTPUT_DIR               = "/Users/svenburkhardt/Desktop/Transkribus_test_Out"
OUTPUT_CSV_PATH          = os.path.join(OUTPUT_DIR, "known_persons_output.csv")

CSV_PATH_KNOWN_PERSONS   = BASE_DIR / "Data" / "Nodegoat_Export" / "export-person.csv"
CSV_PATH_METADATA        = CSV_PATH_KNOWN_PERSONS
LOG_PATH                 = BASE_DIR / "Data" / "new_persons.log"

# --------------- CSV‑Dateien laden (immer mit denselben Optionen) ---------------
read_opts = dict(sep=";", dtype=str, keep_default_na=False)

df = pd.read_csv(CSV_PATH_KNOWN_PERSONS, **read_opts)
print(df.isna().sum())           # Kontrolle: keine NaN → keine Floats

# --------------- Eigene Module (aus /Module) ---------------
from Module import (
    # document_schemas.py
    BaseDocument, Person, Place, Event, Organization,

    # person_matcher.py
    match_person, KNOWN_PERSONS, deduplicate_persons, normalize_name, load_known_persons_from_csv,get_best_match_info,

    # type_matcher.py
    get_document_type,

    # Assigned_Roles_Module.py
    assign_roles_to_known_persons,

    # llm_enricher.py
    llm_enricher,

    # place_matcher.py
    PlaceMatcher,
)

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


# Jetzt sicher deduplizieren
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



# === Bekannte Orte Laden ===
PLACE_CSV_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Datenbank_Metadaten_Stand_08.04.2025/Metadata_Places-Tabelle 1.csv"
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
    if person_exists_in_known_list(forename, familyname, KNOWN_PERSONS):
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
    #         "Lfd_No.": f"{len(known_persons_df) + 1:05d}"  # Neue ID mit führenden Nullen

    # }
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


def extract_custom_attributes(root: ET.Element, known_persons: List[Dict[str, str]] = KNOWN_PERSONS) -> Dict[str, List[Dict[str, Any]]]:
    result = {
        "persons": [],
        "organizations": [],
        "dates": [],
        "places": []
    }

    for text_line in root.findall(".//ns:TextLine", NS):
        custom_attr = text_line.get("custom", "")
        if not custom_attr:
            continue

        text_content = ""
        text_equiv = text_line.find(".//ns:TextEquiv/ns:Unicode", NS)
        if text_equiv is not None and text_equiv.text:
            text_content = text_equiv.text

        # Debug the raw custom attribute
        print(f"[DEBUG] Processing custom attribute: '{custom_attr}'")
        print(f"[DEBUG] Text content: '{text_content}'")

        # Extract persons - returns a list
        persons = extract_person_from_custom(custom_attr, text_content, KNOWN_PERSONS)
        if persons:
            result["persons"].extend(persons)

        # Extract organizations
        orgs = extract_organization_from_custom(custom_attr, text_content)
        if orgs:
            result["organizations"].extend(orgs)

        # Extract dates
        dates = extract_date_from_custom(custom_attr, text_content)
        if dates:
            result["dates"].extend(dates)

        # Extract places
        places = extract_place_from_custom(custom_attr, text_content)
        if places:
            result["places"].extend(places)

    # Debug the result
    print(f"[DEBUG] Extracted entities: persons={len(result['persons'])}, places={len(result['places'])}, "
          f"organizations={len(result['organizations'])}, dates={len(result['dates'])}")
    
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
    

def extract_person_from_custom(custom_attr: str, text_content: str, known_persons: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Extract person entities from custom attributes - returns a LIST of dictionaries."""
    persons = []
    
    # Durchsuche custom_attr nach Mustern wie 'person { ... }'
    for pattern in [r"person\s+\{([^}]+)\}", r"person {([^}]+)}"]:
        person_matches = re.finditer(pattern, custom_attr)
        for person_match in person_matches:
            if not text_content:
                continue

            # Parsed die individuellen Key-Value-Attribute innerhalb von { ... }
            person_data = parse_custom_attributes(person_match.group(1))
            
            if "offset" in person_data and "length" in person_data:
                offset = int(person_data.get("offset", 0))
                length = int(person_data.get("length", 0))

                print(f"[DEBUG] Versuche Extraktion Person: offset={offset}, length={length}, text='{text_content}'")

                if offset < len(text_content) and offset + length <= len(text_content):
                    # Schneidet den entsprechenden Namens-String aus text_content aus
                    person_name = text_content[offset:offset+length]
                    print(f"[DEBUG] Erkannter Personenname: {person_name}")

                    # Nutzt die person_matcher-Funktion, um Personendaten (Vorname, Nachname etc.) zu extrahieren
                    person_info = extract_person_data(person_name)

                    # Versucht, einen Match in den known_persons (z.B. export-person.csv) zu finden
                    match, score = match_person(person_info, candidates=known_persons_list)
                    if match:
                            person_info["nodegoat_id"] = match.get("nodegoat_id", "")
                            person_info["alternate_name"] = match.get("alternate_name", "")
                            person_info["title"] = match.get("title", "")

                    # Baue das Dictionary auf, das du letztlich z.B. ins JSON übernimmst
                    persons.append({
                        "forename": person_info["forename"],
                        "familyname": person_info["familyname"],
                        "role": "",
                        "associated_place": "",
                        "associated_organisation": "",
                        "alternate_name": person_info["alternate_name"],
                        "title": person_info["title"],
                        "nodegoat_id": person_info.get("nodegoat_id", "")
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
            print(f"[DEBUG] Date data: {date_data}")
            
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
                
                print(f"[DEBUG] Formatiertes Datum: {formatted_date}")
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
                
                print(f"[DEBUG] Versuche Extraktion Ort: offset={offset}, length={length}, text='{text_content}'")
                
                if offset < len(text_content) and offset + length <= len(text_content):
                    place_name = text_content[offset:offset+length]
                    print(f"[DEBUG] Erkannter Ort: {place_name}")
                    
                    try:
                        if place_matcher and place_name:
                            match_result = place_matcher.match_place(place_name)
                            if match_result:
                                matched_data = match_result["data"]
                                print(f"[DEBUG] Match für Ort '{place_name}' → Nodegoat-ID: {match_result.get('nodegoat_id')}")
                                places.append({
                                    "name": matched_data.get("Name", place_name),
                                    "alternate_name": matched_data.get("Alternativer Name", ""),
                                    "geonames_id": matched_data.get("GeoNames", ""),
                                    "wikidata_id": matched_data.get("WikidataID", ""),
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
def process_transkribus_file(xml_path: str, seven_digit_folder: str, subdir: str) -> Union[BaseDocument, None]:
    try:
        # XML parsen
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Seitenzahl aus Dateiname extrahieren
        xml_file = os.path.basename(xml_path)
        page_match = re.search(r"p(\d+)", xml_file, re.IGNORECASE)
        page_number = page_match.group(1) if page_match else "001"

        # Dokumenttyp ermitteln
        full_doc_id = f"{seven_digit_folder}_{subdir}_{xml_file.replace('.xml', '')}"
        document_type = get_document_type(seven_digit_folder, page_number)
        print(f"[DEBUG] Dokumenttyp erkannt für {full_doc_id}: {document_type}")

        # Metadaten & Transkript
        metadata_info = extract_metadata_from_xml(root)
        metadata_info["document_type"] = document_type
        transcript_text = extract_text_from_xml(root)
        if not transcript_text or len(transcript_text.strip()) < 10:
            print(f"[INFO] Überspringe {xml_path} – Transkript zu kurz oder leer.")
            return None

        # Custom Tags extrahieren
        custom_data = extract_custom_attributes(root)

        # Personen deduplizieren
        all_persons = custom_data["persons"]
        unique_persons = deduplicate_persons(all_persons)
        # ------------------------------------------------------------------
        #   Personen mit Stammliste abgleichen (Nodegoat‑ID, Titel, …)
        # ------------------------------------------------------------------
        for p in unique_persons:
            match = get_best_match_info(p, KNOWN_PERSONS)

            # nur wenn ein Match vorliegt (Score >= 70 in get_best_match_info)
            if match["match_id"]:
                p["forename"]        = match["matched_forename"]   or p["forename"]
                p["familyname"]      = match["matched_familyname"] or p["familyname"]
                p["title"]           = match["matched_title"]      or p.get("title", "")
                p["nodegoat_id"]     = match["match_id"]
                p["match_score"]     = match["score"]              # optional fürs Debuggen


        mentioned_persons = []
        for person in unique_persons:
            person_obj = Person(
                forename=person.get("forename", ""),
                familyname=person.get("familyname", ""),
                alternate_name=person.get("alternate_name", ""),
                title=person.get("title", ""),
                role=person.get("role", ""),
                associated_place=person.get("associated_place", ""),
                associated_organisation=person.get("associated_organisation", ""),
                nodegoat_id=person.get("nodegoat_id", "")
                )



            if not person_exists_in_known_list(person.get("forename", ""), person.get("familyname", ""), KNOWN_PERSONS):
                with open(LOG_PATH, "a", encoding="utf-8") as log_file:
                    log_file.write(f"{person.get('forename', '')} {person.get('familyname', '')}\n")

            mentioned_persons.append(person_obj)

        
        # Rollenmodul anwenden auf vollständig erkannte Personen
        role_input_persons = [
            {
                "forename": p["forename"],
                "familyname": p["familyname"],
                "alternate_name": p.get("alternate_name", ""),
                "title": p.get("title", ""),
                "role": p.get("role", ""),
                "associated_place": p.get("associated_place", ""),
                "associated_organisation": p.get("associated_organisation", ""),
                "nodegoat_id": p.get("nodegoat_id", "")
            }
            for p in unique_persons
        ]

        enriched_dicts = assign_roles_to_known_persons(role_input_persons, transcript_text)



        mentioned_persons = [
            Person(
                forename=d.get("forename", ""),
                familyname=d.get("familyname", ""),
                alternate_name=str(d.get("alternate_name", "") or ""),  # NaN absichern
                title=str(d.get("title", "") or ""),
                role=d.get("role", ""),
                associated_place=d.get("associated_place", ""),
                associated_organisation=d.get("associated_organisation", ""),
                nodegoat_id=str(d.get("nodegoat_id", "") or "")
            )
            for d in enriched_dicts
        ]



        # BaseDocument zusammenbauen
        doc = BaseDocument(
            object_type="Dokument",
            attributes=metadata_info,
            content_transcription=transcript_text,
            mentioned_persons=mentioned_persons,
            mentioned_organizations=[Organization(**o) for o in custom_data["organizations"]],
            mentioned_places=[
                Place(
                    name=pl.get("name", ""),
                    type=pl.get("type", ""),
                    alternate_place_name=pl.get("alternate_name", ""),
                    geonames_id=pl.get("geonames_id", ""),
                    wikidata_id=pl.get("wikidata_id", ""),
                    nodegoat_id=pl.get("nodegoat_id", "")
                )
                for pl in custom_data["places"]
            ],
            mentioned_dates=custom_data["dates"],
            content_tags_in_german=[],
            author=Person(),
            recipient=Person(),
            creation_date="",
            creation_place="",
            document_type=document_type,
            document_format=""
        )

        return doc

    except Exception as e:
        print(f"Fehler bei der Verarbeitung von {xml_path}: {e}")
        traceback.print_exc()
        return None

def main():
    print("Starte Extraktion von Transkribus-Daten mit Objektorientierung und Validierung...")
    processed_files = 0
    validated_files = 0

    # Stelle sicher, dass das Ausgabeverzeichnis existiert
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Prüfe, ob das Eingabeverzeichnis existiert
    if not os.path.exists(TRANSKRIBUS_DIR) or not os.path.isdir(TRANSKRIBUS_DIR):
        print(f"ERROR: Das Eingabeverzeichnis {TRANSKRIBUS_DIR} existiert nicht oder ist kein Verzeichnis.")
        return

    # Prüfe, ob Orte und Personen korrekt geladen wurden
    print(f"Status: {len(KNOWN_PERSONS)} bekannte Personen geladen.")
    print(f"Status: Place Matcher verfügbar: {place_matcher is not None}")
    if place_matcher and hasattr(place_matcher, 'places_df'):
        print(f"Status: {len(place_matcher.places_df)} bekannte Orte geladen.")

    # Iteriere über alle 7-stelligen Ordner (Transkribus-IDs)
    transkribus_folders = [d for d in os.listdir(TRANSKRIBUS_DIR) 
                          if os.path.isdir(os.path.join(TRANSKRIBUS_DIR, d)) and d.isdigit()]
    
    if not transkribus_folders:
        print(f"Warnung: Keine gültigen Transkribus-Ordner in {TRANSKRIBUS_DIR} gefunden.")
    
    for seven_digit_folder in transkribus_folders:
        folder_path = os.path.join(TRANSKRIBUS_DIR, seven_digit_folder)
        print(f"Verarbeite Transkribus-Ordner: {seven_digit_folder}")

        # Iteriere über alle "Akte_*" Unterordner
        akte_folders = [d for d in os.listdir(folder_path) 
                       if os.path.isdir(os.path.join(folder_path, d)) and d.startswith("Akte_")]
        
        if not akte_folders:
            print(f"Warnung: Keine 'Akte_*'-Unterordner in {folder_path} gefunden.")
            
        for subdir in akte_folders:
            subdir_path = os.path.join(folder_path, subdir)
            print(f"Verarbeite Akte: {subdir}")

            # Finde den "page" Unterordner
            page_folder = os.path.join(subdir_path, "page")
            if not os.path.isdir(page_folder):
                print(f"Kein 'page' Ordner in {subdir_path}")
                continue

            # Verarbeite alle XML-Dateien im "page" Ordner
            xml_files = [f for f in os.listdir(page_folder) if f.endswith(".xml")]
            
            if not xml_files:
                print(f"Warnung: Keine XML-Dateien in {page_folder} gefunden.")
                
            for xml_file in xml_files:
                xml_path = os.path.join(page_folder, xml_file)
                print(f"\nVerarbeite: Transkribus-ID {seven_digit_folder}, {subdir}, Datei {xml_file}")

                # Extrahiere Seitenzahl aus Dateiname
                page_match = re.search(r"p(\d+)|(\d+)_p", xml_file, re.IGNORECASE)
                page_number = page_match.group(1) if page_match and page_match.group(1) else \
                             page_match.group(2) if page_match and page_match.group(2) else "001"

                # Verarbeite die XML-Datei
                doc = process_transkribus_file(xml_path, seven_digit_folder, subdir)
                if doc:
                    validation_errors = doc.validate()
                    is_valid = len(validation_errors) == 0

                    if not is_valid:
                        print(f"Warnung: Dokument {seven_digit_folder}_{subdir}_page{page_number} enthält Validierungsfehler: {validation_errors}")
                    else:
                        validated_files += 1

                    print(f"[DEBUG] Speichere JSON mit {len(doc.mentioned_persons)} Personen, {len(doc.mentioned_places)} Orten etc.")

                    output_filename = f"{seven_digit_folder}_{subdir}_page{page_number}.json"
                    output_path = os.path.join(OUTPUT_DIR, output_filename)

                    if doc is not None and hasattr(doc, "to_json"):
                        with open(output_path, "w", encoding="utf‑8") as f:
                            f.write(doc.to_json(indent=2))   # oder einfach doc.to_json()
                        print(f"JSON gespeichert: {output_path} (Validierung: {'Erfolgreich' if is_valid else 'Fehlgeschlagen'})")

    print(f"\nVerarbeitung abgeschlossen.")
    print(f"- {processed_files} Dateien wurden verarbeitet.")
    print(f"- {validated_files} Dokumente ohne Validierungsfehler")
    print(f"- {processed_files - validated_files} Dokumente mit Validierungsfehlern")

    # LLM-Enrichment aktivieren, wenn gewünscht (noch auskommentiert)
    if OPENAI_API_KEY:
        print("\nLLM-Enrichment ist vorbereitet, aber aktuell deaktiviert.")
        # print("\nStarte LLM-Enrichment der generierten JSON-Dateien...")
        # llm_enricher.run_enrichment_on_directory(OUTPUT_DIR, api_key=OPENAI_API_KEY)
    else:
        print("\nKein OpenAI API Key gefunden. LLM-Enrichment ist nicht möglich.")

if __name__ == "__main__":
    main()