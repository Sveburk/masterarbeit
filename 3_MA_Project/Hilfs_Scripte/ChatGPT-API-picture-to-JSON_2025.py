#"""This script uses the OpenAI ChatGPT API to process images and extract information from them. The script reads
#images from a directory, resizes them, and sends them to the API along with a prompt. The API generates a response
#containing the extracted information in JSON format. The script saves the extracted information to a JSON file with
#the same name as the image file. The script processes multiple images in a batch."""

# Import the required libraries
import base64
import json
import os
import re
import time
from io import BytesIO
from PIL import Image
from openai import OpenAI

# Startzeit erfassen
start_time = time.time()
total_files = 0
total_in_tokens = 0
total_out_tokens = 0
input_cost_per_mio_in_dollars = 2.5
output_cost_per_mio_in_dollars = 10

# Verzeichnisse definieren
image_directory = "/Users/svenburkhardt/Downloads/Akte_001_jpg_4_ChatGPT"
output_directory = "/Users/svenburkhardt/Downloads/Akte_001_jpg_4_ChatGPT_Output"

# Output-Verzeichnis leeren
for root, _, filenames in os.walk(output_directory):
    for filename in filenames:
        os.remove(os.path.join(root, filename))

# API-Einstellungen
api_key = os.getenv("OPENAI_API_KEY")
model = "gpt-4o"
temperature = 0.0
client = OpenAI(api_key=api_key)

# Bilder verarbeiten
for root, _, filenames in os.walk(image_directory):
    file_number = 1
    total_files = len(filenames)
    for filename in filenames:
        if filename.endswith(".jpg"):
            print(f"> Verarbeite Datei ({file_number}/{total_files}): {filename}")
            image_id = filename.split(".")[0]

            with Image.open(os.path.join(root, filename)) as img:
                print("> Bild wird skaliert...", end=" ")
                img.thumbnail((1024, 1492))
                buffered = BytesIO()
                img.save(buffered, format="JPEG")
                base64_image = base64.b64encode(buffered.getvalue()).decode("utf-8")
                print("Fertig.")

            # JSON-Grundstruktur
            json_structure = {
                "object_type": "Dokument",
                "attributes": {
                    "docId": image_id,
                    "pageId": image_id,
                    "tsid": "",
                    "imgUrl": "",
                    "xmlUrl": ""
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
                "content_transcription": "",
                "document_type_options": [
                    "Brief", "Protokoll", "Postkarte", "Rechnung",
                    "Regierungsdokument", "Karte", "Noten", "Zeitungsartikel",
                    "Liste", "Website", "Notizzettel", "Offerte"
                ],
                "document_format_options": ["Handschrift", "Maschinell", "mitUnterschrift", "Bild"]
            }
            # Prompt für die API
            prompt = f"""
            I am providing an image from the Männerchor Murg corpus (Germany), covering 1925–1945, including the Third Reich period, which may influence language and context.
            
            **Your role:** Historian. Analyze the image, extract text, and return structured metadata in JSON format.
            
            **Instructions:**
            - Identify correct document type and format.
            - Extract metadata: author, recipient, persons, locations, dates, events.
            - Preserve German umlauts (ä, ö, ü, ß) and line breaks.
            
            Image ID: {image_id}
            
            {json.dumps(json_structure, indent=4, ensure_ascii=False)}
            """

            workload = [
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]},
                {"role": "system", "content": "You are a historian analyzing historical documents. Respond only in JSON."}
            ]

            # API-Abfrage
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=workload,
                    temperature=temperature
                )
            except Exception as e:
                print(f"> Fehler bei API-Anfrage: {e}")
                continue

            response_text = response.choices[0].message.content.strip()
            print(f"> Antwort erhalten für {filename}. Tokenverbrauch (in/out): {response.usage.prompt_tokens}/{response.usage.completion_tokens}")
            total_in_tokens += response.usage.prompt_tokens
            total_out_tokens += response.usage.completion_tokens

            # JSON aus Antwort extrahieren
            pattern = r"```json\s*(.*?)\s*```"
            match = re.search(pattern, response_text, re.DOTALL)
            if match:
                response_text = match.group(1).strip()

            try:
                parsed_json = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"> Fehler beim Parsen: {e}\nAntwort: {response_text[:500]}")
                continue

            # Datei speichern
            output_filename = f"{image_id}.json"
            output_path = os.path.join(output_directory, output_filename)
            with open(output_path, "w", encoding="utf-8") as json_out:
                json.dump(parsed_json, json_out, indent=4, ensure_ascii=False)
            print(f"> JSON gespeichert: {output_path}")

            file_number += 1

# Gesamtzeit berechnen
end_time = time.time()
total_time = end_time - start_time
print("----------------------------------------")
print(f"Gesamtzeit: {total_time:.2f} Sekunden")
print(f"Gesamter Tokenverbrauch (in/out): {total_in_tokens} / {total_out_tokens}")
print(f"Durchschnittlicher Tokenverbrauch pro Bild: {total_out_tokens / total_files:.2f}")
print(f"Gesamtkosten: ${total_in_tokens / 1e6 * input_cost_per_mio_in_dollars:.2f} / ${total_out_tokens / 1e6 * output_cost_per_mio_in_dollars:.2f}")
print("----------------------------------------")
