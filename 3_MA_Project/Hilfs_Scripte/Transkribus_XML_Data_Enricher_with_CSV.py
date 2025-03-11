import os
import re
import xml.etree.ElementTree as ET
import pandas as pd

# Verzeichnisse festlegen
#xml_base_dir = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Transkribus_Export_06.03.2025_Akte_001-Akte_150"
xml_base_dir = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Transkribus_Export_06.03.2025_Akte_001-Akte_150"

csv_file_path = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Akten_Gesamtübersicht.csv" 
#output_dir = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Enriched_Transkribus_XML"
output_dir = "/Users/svenburkhardt/Downloads/Test_export_CSV_XML"

os.makedirs(output_dir, exist_ok=True)

# XML-Namespace (wie in deinen XML-Dateien)
NS = {"ns": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}

# CSV einlesen (bei fehlerhaften Zeilen werden diese übersprungen)
df_csv = pd.read_csv(csv_file_path, sep=";", dtype=str, on_bad_lines='skip')
# Nur Zeilen, in denen "Akte_Scan" einen gültigen Text enthält, berücksichtigen
df_csv = df_csv[df_csv["Akte_Scan"].str.contains("Akte", na=False)]

# Funktion zur Extraktion der Seitenzahl aus dem Akte_Scan-Feld
def extract_page_from_akte_scan(akte_scan):
    m = re.search(r"_S(\d+)", akte_scan)
    if m:
        return m.group(1)
    return None

df_csv["csv_page_number"] = df_csv["Akte_Scan"].apply(extract_page_from_akte_scan)

# Gruppiere die CSV-Daten nach Transkribus-ID (die entspricht dem 7-stelligen Ordnernamen)
csv_data_by_transkribus = {}
for _, row in df_csv.iterrows():
    doc_id = str(row["Transkribus-ID"]).strip()
    if row["csv_page_number"] is None:
        continue
    if doc_id not in csv_data_by_transkribus:
        csv_data_by_transkribus[doc_id] = []
    csv_data_by_transkribus[doc_id].append(row)

# Verarbeitung der XML-Dateien
for seven_digit_folder in os.listdir(xml_base_dir):
    folder_path = os.path.join(xml_base_dir, seven_digit_folder)
    if not os.path.isdir(folder_path) or not seven_digit_folder.isdigit():
        continue  # Nur Ordner mit 7 Ziffern

    # Durchsuche alle Unterordner, die mit "Akte_" beginnen (jede Akte als Sinneinheit)
    for subdir in os.listdir(folder_path):
        subdir_path = os.path.join(folder_path, subdir)
        if not os.path.isdir(subdir_path) or not subdir.startswith("Akte_"):
            continue

        page_folder = os.path.join(subdir_path, "page")
        if not os.path.isdir(page_folder):
            print(f"❌ Kein Page-Ordner in {subdir_path}")
            continue

        # Iteriere über alle XML-Dateien im Page-Ordner
        for xml_file in os.listdir(page_folder):
            if not xml_file.endswith(".xml"):
                continue

            xml_path = os.path.join(page_folder, xml_file)
            print(f"Verarbeitung: Transkribus-ID {seven_digit_folder}, {subdir}, Datei {xml_file}")

            try:
                tree = ET.parse(xml_path)
                root = tree.getroot()
            except Exception as e:
                print(f"❌ Fehler beim Parsen von {xml_path}: {e}")
                continue

            # Optional: Extrahiere den transkribierten Text (falls benötigt)
            transcript_text = ""
            for text_equiv in root.findall(".//ns:TextEquiv/ns:Unicode", NS):
                if text_equiv.text:
                    transcript_text += text_equiv.text + "\n"
            transcript_text = transcript_text.strip()

            # Extrahiere Metadaten aus <TranskribusMetadata>
            transkribus_meta = root.find(".//ns:TranskribusMetadata", NS)
            if transkribus_meta is None:
                print(f"⚠️ Keine Transkribus-Metadaten in {xml_file}")
                continue

            doc_id = transkribus_meta.get("docId", "").strip()
            page_id = transkribus_meta.get("pageId", "").strip()
            img_url = transkribus_meta.get("imgUrl", "").strip()
            xml_url = transkribus_meta.get("xmlUrl", "").strip()

            # Extrahiere die Seitenzahl aus dem XML-Dateinamen (z. B. "p001.xml" → "001")
            page_match = re.search(r"p(\d+)", xml_file, re.IGNORECASE)
            if page_match:
                xml_page_number = page_match.group(1)
            else:
                xml_page_number = "001"

            # Suche in den CSV-Daten nach dem passenden Eintrag
            csv_match = None
            if doc_id in csv_data_by_transkribus:
                for row in csv_data_by_transkribus[doc_id]:
                    if row["csv_page_number"] == xml_page_number:
                        csv_match = row.to_dict()
                        break

            # Erstelle ein neues Element für die CSV-Daten
            csv_elem = ET.Element("CSVData")
            if csv_match:
                for key, value in csv_match.items():
                    child = ET.SubElement(csv_elem, key)
                    child.text = value
            else:
                note = ET.SubElement(csv_elem, "Note")
                note.text = "Kein CSV-Match gefunden"

            # Füge das CSVData-Element in das XML ein (als letztes Kind des Wurzelelements)
            root.append(csv_elem)

            # Speichere das angereicherte XML in das Output-Verzeichnis
            output_filename = f"{seven_digit_folder}_{subdir}_{xml_file}"
            output_path = os.path.join(output_dir, output_filename)
            try:
                tree.write(output_path, encoding="utf-8", xml_declaration=True)
                print(f"✅ XML gespeichert: {output_path}")
            except Exception as e:
                print(f"❌ Fehler beim Speichern von {output_path}: {e}")
