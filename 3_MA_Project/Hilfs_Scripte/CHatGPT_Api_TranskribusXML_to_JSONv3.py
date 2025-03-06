import json
import os
import xml.etree.ElementTree as ET
import openai
import re
import time

# ------------------------------
# BASIS-EINSTELLUNGEN
# ------------------------------
start_time = time.time()

# Zähler
total_files = 0
total_in_tokens = 0
total_out_tokens = 0

# Kosten-Konstanten
input_cost_per_mio_in_dollars = 2.5
output_cost_per_mio_in_dollars = 10

# OpenAI API
api_key = "sk-OUnUKfiRurjwDl4pHMgNS6YBYhTFv65_L4jqhxZgelT3BlbkFJ2BP4s-8K1L37Ccs3a6JfiE843sUsjAXBcNRIjDPbQA"
client = openai.OpenAI(api_key=api_key)
model = "gpt-4"       # Oder was Du verwenden möchtest
temperature = 0.0

# Verzeichnisse
base_input_directory = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Rise_API_Course/Rise_Api_course_Input"
output_directory = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Rise_API_Course/Rise_Api_course_output"
os.makedirs(output_directory, exist_ok=True)

# -------------------------------------------------------------
# ALLE 7-stelligen ORDNER im Basis-Verzeichnis durchlaufen
# -------------------------------------------------------------
for seven_digit_folder in os.listdir(base_input_directory):
    folder_path = os.path.join(base_input_directory, seven_digit_folder)

    # Prüfe, ob es tatsächlich ein Ordner ist, 7-stelliger Name etc.
    if (not os.path.isdir(folder_path)
        or not seven_digit_folder.isdigit()
        or len(seven_digit_folder) != 7):
        continue

    # Jetzt ALLE Unterordner (z. B. "Akte_123_pdf") in diesem 7-stelligen Ordner ansehen
    for subdir in os.listdir(folder_path):
        subdir_path = os.path.join(folder_path, subdir)

        # Prüfe, ob subdir tatsächlich ein Ordner ist:
        # und ob er mit "Akte_" anfängt (und evtl. "_pdf" endet).
        if not os.path.isdir(subdir_path):
            continue

        if not subdir.startswith("Akte_"):
            # Andere Ordner ignorieren
            continue

        # Nun liegt in diesem Ordner "subdir" (z. B. "Akte_123_pdf") ein "page"-Ordner
        page_folder = os.path.join(subdir_path, "page")
        if not os.path.isdir(page_folder):
            # Falls kein "page"-Ordner vorhanden ist, überspringen
            continue

        # ---------------------------------------------------------
        # Hier liegen jetzt die XML-Seiten => alle .xml iterieren
        # ---------------------------------------------------------
        for xml_file in os.listdir(page_folder):
            if not xml_file.endswith(".xml"):
                continue

            xml_path = os.path.join(page_folder, xml_file)
            print(f"> 🟢 Starte Verarbeitung für Ordner {seven_digit_folder}, '{subdir}', Seite {xml_file}")
            total_files += 1

            # Seitenzahl extrahieren (beginnend bei 1)
            page_number_match = re.search(r"p(\d+)", xml_file,re.IGNORECASE)
            if page_number_match:
                page_number = f"{int(page_number_match.group(1)) + 1:03d}"
            else:
                page_number = "001"

            transcript_text = ""
            metadata_info = {}

            # ---------------------------------------------------------
            # 1) XML EINLESEN
            # ---------------------------------------------------------
            try:
                tree = ET.parse(xml_path)
                root = tree.getroot()
                ns = {"ns": "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"}

                transkribus_meta = root.find(".//ns:TranskribusMetadata", ns)
                if transkribus_meta is not None:
                    metadata_info = {
                        "docId": transkribus_meta.get("docId", ""),
                        "pageId": transkribus_meta.get("pageId", ""),
                        "tsid": transkribus_meta.get("tsid", ""),
                        "imgUrl": transkribus_meta.get("imgUrl", ""),
                        "xmlUrl": transkribus_meta.get("xmlUrl", "")
                    }

                for text_equiv in root.findall(".//ns:TextEquiv/ns:Unicode", ns):
                    if text_equiv.text:
                        transcript_text += text_equiv.text + "\n"

            except Exception as e:
                print(f"> Fehler beim Lesen der XML {xml_path}: {e}")
                continue

            # Wenn kein Transkript, nächste Seite
            if not transcript_text.strip():
                print(f"> Kein Text in {xml_path} gefunden. Überspringe...")
                continue

            # ---------------------------------------------------------
            # JSON-GRUNDELEMENT
            # ---------------------------------------------------------
            json_structure = {
                "object_type": "Dokument",
                "attributes": {
                    "docId": metadata_info.get("docId", ""),
                    "pageId": metadata_info.get("pageId", ""),
                    "tsid": metadata_info.get("tsid", ""),
                    "imgUrl": metadata_info.get("imgUrl", ""),
                    "xmlUrl": metadata_info.get("xmlUrl", "")
                },
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
                "content_transcription": transcript_text.strip(),
                "document_type_options": [
                    "Brief", "Protokoll", "Postkarte", "Rechnung",
                    "Regierungsdokument", "Karte", "Noten", "Zeitungsartikel",
                    "Liste", "Website", "Notizzettel", "Offerte"
                ],
                "document_format_options": ["Handschrift", "Maschinell", "mitUnterschrift", "Bild"]
            }

            # ---------------------------------------------------------
            # PROMPT FÜR DIE API
            # ---------------------------------------------------------
            prompt = f"""
            I am providing a text transcript from the Männerchor Murg corpus (Germany), covering 1925–1945, including the Third Reich period, which may influence language and context.

            **Your role:** Historian. Analyze each image, extract the text, and compare relevant information with absolute accuracy (**temperature = 0.0**).

            **Instructions:**
            - Identify the correct **document type** from "document_type_options"—choose only the best match.
            - Extract and structure metadata in JSON:
              - **Author, recipient, mentioned persons**
              - **Locations, dates (format: "yyyy.mm.dd")**
              - **Events, sender/recipient, geographical references**
              - **Content tags**
              - Select ONLY ONE correct **document type** from the "document_type_options" list and write it under "document_type".
              - Select ONLY ONE correct **document format** from the "document_format_options" list and write it under "document_format".

            **Formatting requirements:**
            - Output **UTF-8** text, preserving **German umlauts (ä, ö, ü, ß)**—no HTML entities.
            - Keep real **line breaks**, not \\n.
            - Extract and include **image tags**: "Handschrift", "Maschinell", "mitUnterschrift", "Bild".

            Text:
            {transcript_text}

            {json.dumps(json_structure, indent=4, ensure_ascii=False)}
            """

            # ---------------------------------------------------------
            # 2) OPENAI-API-AUFRUF
            # ---------------------------------------------------------
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a historian analyzing historical documents with precision."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=temperature
                )
            except Exception as e:
                print(f"> ❌ Fehler bei API-Anfrage für {xml_file}: {e}")
                continue

            # Token-Infos extrahieren
            if response and hasattr(response, "usage"):
                total_in_tokens += response.usage.prompt_tokens
                total_out_tokens += response.usage.completion_tokens
            else:
                print("> ⚠️ Warnung: Keine Token-Informationen erhalten!")

            # Antwortinhalt prüfen
            if not response or not response.choices:
                print("> ❌ Keine Antwort von der API erhalten.")
                continue

            response_text = response.choices[0].message.content.strip()
            if not response_text:
                print("> ❌ Leere Antwort vom Modell erhalten.")
                continue

            # ---------------------------------------------------------
            # 3) JSON PARSEN & SPEICHERN
            # ---------------------------------------------------------
            # Markdown-Block ggf. herauslösen
            pattern = r"`json\s*(.*?)\s*`"
            match = re.search(pattern, response_text, re.DOTALL)
            if match:
                response_text = match.group(1).strip()

            # JSON parsen
            try:
                parsed_json = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"> Fehler beim Parsen der API-Antwort: {e}")
                print(f"> Antwort war: {response_text[:500]}")
                continue

            # Datei speichern
            output_filename = f"Akte_{subdir}_P{page_number}.json"
            output_file = os.path.join(output_directory, output_filename)
            print(f">  Debug: Speichere JSON unter {output_file}")

            try:
                with open(output_file, "w", encoding="utf-8") as json_out:
                    json.dump(parsed_json, json_out, indent=4, ensure_ascii=False)
                print(f"> JSON gespeichert: {output_file}")
            except Exception as e:
                print(f"> Fehler beim Speichern von {output_file}: {e}")
                continue

# ---------------------------------------------------------
# NACH ABSCHLUSS: STATISTIK AUSGEBEN
# ---------------------------------------------------------
end_time = time.time()
total_time = end_time - start_time
print(f"\n✅ Verarbeitete Dateien: {total_files}")
print(f"⏳ Gesamtzeit: {total_time:.2f} Sekunden")

if total_files > 0:
    total_cost_in = (total_in_tokens / 1e6) * input_cost_per_mio_in_dollars
    total_cost_out = (total_out_tokens / 1e6) * output_cost_per_mio_in_dollars
    total_cost = total_cost_in + total_cost_out
    print(f"📊 Total processing time: {total_time:.2f} seconds")
    print(f"🔢 Total token cost (in/out): {total_in_tokens} / {total_out_tokens}")
    print(f"💰 Total cost (in/out): ${total_cost_in:.2f} / ${total_cost_out:.2f}")
    print(f"✅ Total cost: ${total_cost:.2f}")
else:
    print("⚠️ No files were processed. Skipping cost calculation.")
