import json
import os
import xml.etree.ElementTree as ET
from openai import OpenAI

# API & Verzeichnisse
api_key = "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"
client = OpenAI(api_key=api_key)
model = "gpt-4o"
temperature = 0.0

base_input_directory = "Rise_Api_course_Input"
output_directory = "Rise_Api_course_output"
os.makedirs(output_directory, exist_ok=True)

# Durchlaufe die relevanten Ordnerstrukturen
for seven_digit_folder in os.listdir(base_input_directory):
    folder_path = os.path.join(base_input_directory, seven_digit_folder)
    if not os.path.isdir(folder_path) or not seven_digit_folder.isdigit() or len(seven_digit_folder) != 7:
        continue

    for subdir in os.listdir(folder_path):
        if not subdir.startswith("Akte_") or not subdir.endswith("_pdf"):
            continue

        page_folder = os.path.join(folder_path, subdir, "page")
        if not os.path.isdir(page_folder):
            continue

        for xml_file in os.listdir(page_folder):
            if not xml_file.endswith(".xml"):
                continue

            xml_path = os.path.join(page_folder, xml_file)
            print(f"> Verarbeite XML: {xml_path}")

            transcript_text = ""
            metadata_info = {}

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

            if not transcript_text.strip():
                print(f"> Kein Text in {xml_path} gefunden. Überspringe...")
                continue

            prompt = f"""
I am providing a text transcript from the Männerchor Murg corpus (Germany), covering 1925–1945, including the Third Reich period, which may influence language and context.

**Your role:** Historian. Analyze each image, extract the text, and compare relevant information with absolute accuracy (**temperature = 0.0**).

**Instructions:**  
- If data is missing, write `"None"`.  
- Identify the correct **document type** from `"document_type_options"`—choose only the best match.  
- Extract and structure metadata in JSON:  
  - **Author, recipient, mentioned persons**  
  - **Locations, dates (format: "yyyy.mm.dd")**  
  - **Events, sender/recipient, geographical references**  
  - **Content tags**  

**Formatting requirements:**  
- Output **UTF-8** text, preserving **German umlauts (ä, ö, ü, ß)**—no HTML entities.  
- Keep real **line breaks**, not `\n`.  
- Extract and include **image tags**: `"Handschrift"`, `"Maschinell"`, `"mitUnterschrift"`, `"Bild"`.  

Text to analyze:
{transcript_text}
"""
            
            response = client.completions.create(
                model=model,
                prompt=prompt,
                temperature=temperature,
                max_tokens=2048
            )

            json_structure = json.loads(response.choices[0].text.strip())

            try:
                json_string = json.dumps(json_structure, indent=4, ensure_ascii=False)
                print("> JSON erfolgreich generiert.")
            except Exception as e:
                print(f"> Fehler beim Erstellen der JSON-Struktur: {e}")
                continue

            output_file = os.path.join(output_directory, os.path.basename(xml_path).replace(".xml", ".json"))
            with open(output_file, "w", encoding="utf-8") as json_out:
                json_out.write(json_string)

            print(f"> JSON gespeichert: {output_file}")
