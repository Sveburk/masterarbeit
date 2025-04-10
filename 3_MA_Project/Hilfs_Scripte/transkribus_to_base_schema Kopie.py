"""
Extraktion von Basisinformationen aus Transkribus XML-Dateien und Konvertierung in das Basis-Schema.

Dieses Skript liest Transkribus XML-Dateien, extrahiert die Metadaten und den Text
und konvertiert sie in das in WORKFLOW.md definierte Basis-Schema unter Verwendung der
in document_schemas.py definierten Klassen für Objektorientierung und Datenvalidierung.
"""

import os
import json
import sys
import os
sys.path.append(os.path.dirname(__file__))
from person_matcher import (
    match_person, 
    KNOWN_PERSONS, 
    deduplicate_persons, 
    normalize_name,
    load_known_persons_from_csv
)


import xml.etree.ElementTree as ET
import re
import time
from typing import Dict, List, Any, Optional, Union
import spacy
import pandas as pd
import pandas as pd
# Konstanten definieren

#TRANSKRIBUS_DIR = "/mnt/c/Users/sorin/PycharmProjects/masterarbeit/3_MA_Project/Data/Transkribus_Export_06.03.2025_Akte_001-Akte_150"      #alter export
#OUTPUT_DIR = "/mnt/c/Users/sorin/PycharmProjects/masterarbeit/3_MA_Project/Data/Base_Schema_Output"

TRANSKRIBUS_DIR = "/Users/svenburkhardt/Desktop/Transkribus_test_In"           #Testdansatz
OUTPUT_DIR = "//Users/svenburkhardt/Desktop/Transkribus_test_Out"
OUTPUT_CSV_PATH = os.path.join(OUTPUT_DIR, "known_persons_output.csv")
# Definiere den Pfad zur bekannten Personenliste (Tipp: Verwende denselben Pfad wie in person_matcher.py)
CSV_PATH_KNOWN_PERSONS = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Datenbank_Metadaten_Stand_08.04.2025/Metadata_Person-Metadaten_Personen.csv"
# Logdatei für neue Personen
LOG_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/new_persons.log"




# Hinweis: Wir können die Funktionen von person_matcher.py wiederverwenden

# Import Rapidfuzz für Fuzzy-Matching (Namensvergleich und zusammenfügung bei unklaren Schreibweisen)
from rapidfuzz import fuzz, process

# Import des Person Matchers für konsistente Personenerkennung
from person_matcher import match_person, load_known_persons_from_csv, normalize_name, fuzzy_match_name

# Import der Schema-Klassen
from document_schemas import BaseDocument, Person, Place, Event, Organization

# Lade deutsches spaCy-Modell
try:
    nlp = spacy.load("de_core_news_sm")
except:
    # Fallback für den Fall, dass das Modell nicht installiert ist
    print("Warnung: SpaCy-Modell 'de_core_news_sm' nicht gefunden. Verwende Fallback-Methode für Namensaufteilung.")
    nlp = None

# Stelle sicher, dass diese Zeilen *nach* allen anderen Imports eingefügt werden,
# damit spaCy, pandas etc. schon importiert sind.


# Lade bekannte Personen aus der CSV über die person_matcher-Funktionen
known_persons_list = load_known_persons_from_csv(CSV_PATH_KNOWN_PERSONS)

# Wir verwenden die Funktionen aus person_matcher.py
known_persons_df = pd.read_csv(CSV_PATH_KNOWN_PERSONS, sep=";")

# Für Abwärtskompatibilität älterer Funktionen, die noch Tupel verwenden
KNOWN_PERSONS = list(zip(
    known_persons_df["schema:givenName"].fillna("").str.strip(),
    known_persons_df["schema:familyName"].fillna("").str.strip()
))


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
            "schema:givenName": forename,
            "schema:familyName": familyname,
            "schema:alternateName": "",
            "schema:homeLocation": "",
            "schema:birthDate": "",
            "schema:deathDate": "",
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
    
    Args:
        person_name: Der zu suchende Personenname
        
    Returns:
        Matched person dictionary oder None, wenn keine Übereinstimmung gefunden wurde
    """
    if not person_name:
        return None
        
    # Extrahiere Vor- und Nachname aus dem Text
    forename, familyname = extract_name_with_spacy(person_name)
    
    # Verwende die match_person-Funktion aus person_matcher.py
    person_dict = {"forename": forename, "familyname": familyname}
    matched_person, score = match_person(person_dict)
    
    if matched_person and score >= 70:
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
                    
                    # Versuche mit spaCy Vor- und Nachname zu extrahieren
                    forename, familyname = extract_name_with_spacy(person_name)
                    
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
                    org_name = text_content[offset:offset+length]
                    result["organizations"].append({
                        "name": org_name,
                        "location": "",
                        "type": ""
                    })
        
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
                    place_name = text_content[offset:offset+length]
                    result["places"].append({
                        "name": place_name,
                        "country": "Deutschland",  # Standardwert
                        "type": ""
                    })
    
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

def process_transkribus_file(xml_path: str, seven_digit_folder: str, subdir: str) -> Union[BaseDocument, None]:
    """
    Verarbeitet eine Transkribus XML-Datei und extrahiert die Daten
    
    Args:
        xml_path: Pfad zur XML-Datei
        seven_digit_folder: Name des übergeordneten Ordners (Transkribus-ID)
        subdir: Name des Unterordners (z.B. "Akte_001")
        
    Returns:
        BaseDocument mit den extrahierten Daten oder None bei Fehler
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Metadaten und Text extrahieren
        metadata_info = extract_metadata_from_xml(root)
        transcript_text = extract_text_from_xml(root)
        
        # Versuche, custom-Attribute zu extrahieren
        custom_data = extract_custom_attributes(root)
        
        # Extrahierte Personen sammeln
        all_persons = []
        for person in custom_data["persons"]:
            all_persons.append(person)
            
        # Verwende den person_matcher um Duplikate zu erkennen und zu vermeiden
        unique_persons = deduplicate_persons(all_persons, known_candidates=known_persons_list)
        
        # Identifiziere neue Personen, die nicht in der bekannten Personenliste sind
        mentioned_persons = []
        for person in unique_persons:
            # Erstelle ein Personenobjekt für das Dokument
            person_obj = Person(
                forename=person.get("forename", ""), 
                familyname=person.get("familyname", ""),
                role=person.get("role", ""),
                associated_place=person.get("associated_place", ""),
                associated_organisation=person.get("associated_organisation", "")
            )
            
            # Prüfe, ob die Person bereits bekannt ist oder neu hinzugefügt wurde
            if not person_exists_in_known_list(person.get("forename", ""), person.get("familyname", ""), KNOWN_PERSONS):
                # Logge die neue Person
                with open(LOG_PATH, "a", encoding="utf-8") as log_file:
                    log_file.write(f"{person.get('forename', '')} {person.get('familyname', '')}\n")
                    
                # Optional: Speichere die Person zur permanenten Datenbank hinzu
                # save_new_person_to_csv(person.get("forename", ""), person.get("familyname", ""), CSV_PATH_KNOWN_PERSONS)
            
            # Person zum Dokument hinzufügen
            mentioned_persons.append(person_obj)
        
        # Erstelle das BaseDocument Objekt mit den verarbeiteten Personen
        doc = BaseDocument(
            object_type="Dokument",
            attributes=metadata_info,
            content_transcription=transcript_text,
            mentioned_persons=mentioned_persons,
            mentioned_organizations=[Organization(**o) for o in custom_data["organizations"]],
            mentioned_places=[Place(**pl) for pl in custom_data["places"]],
            mentioned_dates=custom_data["dates"],
            content_tags_in_german=[],
            author=Person(),  # Placeholder für Autor
            recipient=Person(),  # Placeholder für Empfänger
            creation_date="",
            creation_place="",
            document_type="",
            document_format=""
        )
        
        return doc
    
    except Exception as e:
        print(f"Fehler bei der Verarbeitung von {xml_path}: {e}")
        return None

def main():
    """Hauptfunktion"""
    print("Starte Extraktion von Transkribus-Daten mit Objektorientierung und Validierung...")
    processed_files = 0
    validated_files = 0
    
    # Iteriere über alle 7-stelligen Ordner (Transkribus-IDs)
    for seven_digit_folder in os.listdir(TRANSKRIBUS_DIR):
        folder_path = os.path.join(TRANSKRIBUS_DIR, seven_digit_folder)
        
        # Prüfe, ob es ein gültiger Ordner ist
        if not os.path.isdir(folder_path) or not seven_digit_folder.isdigit():
            continue
        
        # Iteriere über alle "Akte_*" Unterordner
        for subdir in os.listdir(folder_path):
            subdir_path = os.path.join(folder_path, subdir)
            
            if not os.path.isdir(subdir_path) or not subdir.startswith("Akte_"):
                continue
            
            # Finde den "page" Unterordner
            page_folder = os.path.join(subdir_path, "page")
            if not os.path.isdir(page_folder):
                print(f"Kein 'page' Ordner in {subdir_path}")
                continue
            
            # Verarbeite alle XML-Dateien im "page" Ordner
            for xml_file in os.listdir(page_folder):
                if not xml_file.endswith(".xml"):
                    continue
                
                xml_path = os.path.join(page_folder, xml_file)
                print(f"Verarbeite: Transkribus-ID {seven_digit_folder}, {subdir}, Datei {xml_file}")
                
                # Extrahiere Seitenzahl aus Dateiname
                page_match = re.search(r"p(\d+)", xml_file, re.IGNORECASE)
                if page_match:
                    page_number = page_match.group(1)
                else:
                    page_number = "001"
                
                # Verarbeite die XML-Datei
                doc = process_transkribus_file(xml_path, seven_digit_folder, subdir)
                if doc:
                    # Prüfe, ob der Autor vollständig unbekannt ist (weder Vor- noch Nachname)
                    unknown_author = not doc.author.forename.strip() and not doc.author.familyname.strip()
                    
                    # Validiere das Dokument
                    validation_errors = doc.validate()
                    is_valid = len(validation_errors) == 0
                    
                    if not is_valid:
                        print(f"Warnung: Dokument {seven_digit_folder}_{subdir}_page{page_number} enthält Validierungsfehler: {validation_errors}")
                    else:
                        validated_files += 1
                    
                    # Speichere das Ergebnis
                    output_filename = f"{seven_digit_folder}_{subdir}_page{page_number}.json"
                    output_path = os.path.join(OUTPUT_DIR, output_filename)
                    
                    # Verwende die to_json-Methode des BaseDocument
                    with open(output_path, "w", encoding="utf-8") as json_out:
                        json_out.write(doc.to_json())
                    
                    print(f"JSON gespeichert: {output_path} (Validierung: {'Erfolgreich' if is_valid else 'Fehlgeschlagen'})")
                    processed_files += 1
    
    print(f"Verarbeitung abgeschlossen. {processed_files} Dateien wurden verarbeitet.")
    print(f"Davon {validated_files} Dokumente ohne Validierungsfehler und {processed_files - validated_files} mit Validierungsfehlern.")

if __name__ == "__main__":
    main()
# [WARNUNG] personen_liste nicht gefunden – Matching-Block nicht eingefügt.