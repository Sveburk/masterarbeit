import json
import os
import re
import time
try:
    from PyPDF2 import PdfReader
except ImportError:
    print("PyPDF2 is not installed. Please install it using 'pip install PyPDF2'")
    exit(1)

from openai import OpenAI

# Zeitmessung starten
start_time = time.time()
total_files = 0
total_in_tokens = 0
total_out_tokens = 0
input_cost_per_mio_in_dollars = 2.5
output_cost_per_mio_in_dollars = 10

# Definiere die Input- und Output-Ordner
input_directory = "/Users/svenburkhardt/Library/Mobile Documents/com~apple~CloudDocs/1 Uni/Master/1_Studienfächer/Digital Humanities/FS2025/Rise_API_Course/Rise_Api_course_Input"
output_directory = "/Users/svenburkhardt/Library/Mobile Documents/com~apple~CloudDocs/1 Uni/Master/1_Studienfächer/Digital Humanities/FS2025/Rise_API_Course/Rise_Api_course_output"

# Stelle sicher, dass der Output-Ordner existiert
os.makedirs(output_directory, exist_ok=True)

# Setze API-Schlüssel, Modell und Temperatur
api_key = "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"
model = "gpt-4o"
temperature = 0.0

# OpenAI-Client initialisieren
client = OpenAI(api_key=api_key)

# Durchlaufe alle Unterordner im Input-Ordner
for subdir in os.listdir(input_directory):
    subdir_path = os.path.join(input_directory, subdir)
    if os.path.isdir(subdir_path):
        pdf_file = None
        for item in os.listdir(subdir_path):
            item_path = os.path.join(subdir_path, item)
            if os.path.isfile(item_path) and item.lower().endswith(".pdf"):
                pdf_file = item_path
                break  # Nur die erste gefundene PDF verwenden
        if pdf_file is None:
            continue  # Falls kein PDF gefunden wurde, zum nächsten Ordner springen

        total_files += 1
        print("----------------------------------------")
        print(f"> Verarbeite PDF ({total_files}): {pdf_file}")

        # Extrahiere den Transkripttext aus der PDF
        transcript_text = ""
        try:
            with open(pdf_file, "rb") as f:
                reader = PdfReader(f)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        transcript_text += page_text + "\n"
        except Exception as e:
            print(f"> Fehler beim Lesen der PDF {pdf_file}: {e}")
            continue

        # Erstelle den Prompt für die API
        prompt = f"""
I am providing you a text transcript extracted from acorpus of the Männerchor Murg from Germany,
    dating from 1925 to 1945 and covers the so-called Third Reich, which may be reflected in 
    language and context.
    Your Role is beeing a Historian, the task is to analyze each image for its text content and extract and compare relevant information.
    Keep in mind the context I provided. The data will be used for scientific research, so it is essential 
    that the data is absolutely accurate, the temperature should therfore be 0,0.
    If there is no data for a particular item, please write "None". Use the list under "document_type_options" 
    to identify and select the appropriate document type for the current document. Choose only the type that best matches.

I am interested in: Metadata such as author, recipient, other mentioned persons, location(s), date(s), 
    and events including Sender, Recipient, and geographical places as well as content tags in a structured JSON file.
    It is urgent that you ensure all text is output in UTF-8, especially German umlauts (ä, ö, ü) and the ß character, without using HTML entities.
    Represent any line breaks in the text as real line breaks rather than `\n`. For Dates youse the format "yyyy.mm.dd".
    The pictures do have Tags in them, namely   "Handschrift", "Maschinell", "mitUnterschrift", "Bild". Extract and mention those in the Json below.
    The JSON should be structured like this:

{transcript_text}

Please analyze this transcript and extract relevant metadata into a structured JSON file with the following format:


```
json
[
  {{
    "object_type": "Dokument",
    "attributes": {{
        "document_id": "{os.path.basename(pdf_file)}",
        "author": {{
            "forename": "", 
            "familyname": "", 
            "role": "", 
            "associated_place": "",  
            "associated_organisation": ""
        }},
        "recipient": {{
            "forename": "", 
            "familyname": "", 
            "role": "", 
            "associated_place": "",
            "associated_organisation": ""
        }},
        "mentioned_persons": [
            {{
                "forename": "", 
                "familyname": "", 
                "role": "", 
                "associated_place": "", 
                "associated_organisation": ""
            }}
        ],
        "mentioned_organizations": [
            {{
                "Organization_name": "", 
                "associated_place": ""
            }}
        ],
        "mentioned_events": [
            {{
                "date": "", 
                "description": "", 
                "associated_place": "", 
                "associated_organisation": ""
            }}
        ],
        "creation_date": "{transcript_text[:10]}",  # Extrahiere Datum aus Transkript (YYYY-MM-DD)
        "creation_place": [""],
        "mentioned_dates": [
            {{
                "day": "", 
                "month": "", 
                "year": ""
            }}
        ],
        "mentioned_places": [""],
        "content_tags_in_german": [""],
        "content_transcription": "{transcript_text}",
        "document_type_options": ["Brief", "Protokoll", "Postkarte", "Rechnung", "Regierungsdokument", "Karte", "Noten", "Zeitungsartikel", "Liste", "Website", "Notizzettel", "Offerte"],
        "document_format_options": ["Handschrift", "Maschinell", "mitUnterschrift", "Bild"]
    }}
  }}
]
"""

        # API-Aufruf INNERHALB der Schleife!
        answer = client.chat.completions.create(
            messages=workload,
            model=model,
            temperature=temperature
        )
        print("Übermittlung an API abgeschlossen.")

        # Extrahiere die Antwort aus der API
        answer_text = answer.choices[0].message.content
        print("> API-Antwort erhalten.")

        # JSON aus der API-Antwort extrahieren
        pattern = r"```\s*json(.*?)\s*```"
        match = re.search(pattern, answer_text, re.DOTALL)
        if match:
            answer_text = match.group(1).strip()
            try:
                answer_data = json.loads(answer_text)
            except json.JSONDecodeError as e:
                print(f"> Fehler beim Parsen von JSON: {e}")
                continue  # Zum nächsten Dokument springen

            # Speichere das JSON-File pro PDF-Dokument
            output_filename = os.path.join(output_directory, f"{os.path.splitext(os.path.basename(pdf_file))[0]}.json")
            with open(output_filename, "w", encoding="utf-8") as json_file:
                json.dump(answer_data, json_file, indent=4)
                print(f"> Antwort gespeichert: {output_filename}")

# Berechne die gesamte Verarbeitungszeit
end_time = time.time()
total_time = end_time - start_time

# Berechne die Gesamtkosten
if total_files > 0:
    total_cost_in = (total_in_tokens / 1e6) * input_cost_per_mio_in_dollars
    total_cost_out = (total_out_tokens / 1e6) * output_cost_per_mio_in_dollars
    print(f"Total processing time: {total_time:.2f} seconds")
    print(f"Total token cost (in/out): {total_in_tokens} / {total_out_tokens}")
    print(f"Total cost (in/out): ${total_cost_in:.2f} / ${total_cost_out:.2f}")
else:
    print("No files were processed. Skipping cost calculation.")
