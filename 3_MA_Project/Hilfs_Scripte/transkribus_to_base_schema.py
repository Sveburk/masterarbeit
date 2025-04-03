"""
Extraktion von Basisinformationen aus Transkribus XML-Dateien und Konvertierung in das Basis-Schema.

Dieses Skript liest Transkribus XML-Dateien, extrahiert die Metadaten und den Text
und konvertiert sie in das in WORKFLOW.md definierte Basis-Schema.
"""

import os
import json
import xml.etree.ElementTree as ET
import re
from typing import Dict, List, Any, Optional

# Konstanten definieren
TRANSKRIBUS_DIR = "/mnt/c/Users/sorin/PycharmProjects/masterarbeit/3_MA_Project/Data/Transkribus_Export_06.03.2025_Akte_001-Akte_150"
OUTPUT_DIR = "/mnt/c/Users/sorin/PycharmProjects/masterarbeit/3_MA_Project/Data/Base_Schema_Output"

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

def create_base_schema(metadata_info: Dict[str, str], transcript_text: str) -> Dict[str, Any]:
    """
    Erstellt die Basis-Schema-Struktur
    
    Args:
        metadata_info: Metadaten aus dem XML
        transcript_text: Extrahierter Text
        
    Returns:
        Dictionary mit dem Basis-Schema
    """
    return {
        "object_type": "Dokument",
        "attributes": metadata_info,
        "author": {
            "forename": "",
            "familyname": "",
            "role": "",
            "associated_place": "",
            "associated_organisation": ""
        },
        "recipient": {
            "forename": "",
            "familyname": "",
            "role": "",
            "associated_place": "",
            "associated_organisation": ""
        },
        "mentioned_persons": [],
        "mentioned_organizations": [],
        "mentioned_events": [],
        "creation_date": "",
        "creation_place": "",
        "mentioned_dates": [],
        "mentioned_places": [],
        "content_tags_in_german": [],
        "content_transcription": transcript_text,
        "document_type": "",  # Wird später gefüllt
        "document_format": ""  # Wird später gefüllt
    }

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
                    # Versuche Vor- und Nachname zu trennen
                    name_parts = person_name.split()
                    if len(name_parts) > 1:
                        forename = name_parts[0]
                        familyname = " ".join(name_parts[1:])
                    else:
                        forename = ""
                        familyname = person_name
                    
                    result["persons"].append({
                        "forename": forename,
                        "familyname": familyname,
                        "role": "",
                        "associated_place": "",
                        "associated_organisation": ""
                    })
        
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

def process_transkribus_file(xml_path: str, seven_digit_folder: str, subdir: str) -> Dict[str, Any]:
    """
    Verarbeitet eine Transkribus XML-Datei und extrahiert die Daten
    
    Args:
        xml_path: Pfad zur XML-Datei
        seven_digit_folder: Name des übergeordneten Ordners (Transkribus-ID)
        subdir: Name des Unterordners (z.B. "Akte_001")
        
    Returns:
        Dictionary mit dem Basis-Schema
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Metadaten und Text extrahieren
        metadata_info = extract_metadata_from_xml(root)
        transcript_text = extract_text_from_xml(root)
        
        # Basis-Schema erstellen
        result = create_base_schema(metadata_info, transcript_text)
        
        # Versuche, custom-Attribute zu extrahieren
        custom_data = extract_custom_attributes(root)
        
        # Füge extrahierte Daten hinzu
        result["mentioned_persons"] = custom_data["persons"]
        result["mentioned_organizations"] = custom_data["organizations"]
        result["mentioned_dates"] = custom_data["dates"]
        result["mentioned_places"] = custom_data["places"]
        
        return result
    
    except Exception as e:
        print(f"Fehler bei der Verarbeitung von {xml_path}: {e}")
        return None

def main():
    """Hauptfunktion"""
    print("Starte Extraktion von Transkribus-Daten...")
    processed_files = 0
    
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
                result = process_transkribus_file(xml_path, seven_digit_folder, subdir)
                if result:
                    # Speichere das Ergebnis
                    output_filename = f"{seven_digit_folder}_{subdir}_page{page_number}.json"
                    output_path = os.path.join(OUTPUT_DIR, output_filename)
                    
                    with open(output_path, "w", encoding="utf-8") as json_out:
                        json.dump(result, json_out, indent=4, ensure_ascii=False)
                    
                    print(f"JSON gespeichert: {output_path}")
                    processed_files += 1
    
    print(f"Verarbeitung abgeschlossen. {processed_files} Dateien wurden verarbeitet.")

if __name__ == "__main__":
    main()