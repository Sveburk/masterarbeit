"""This script uses the OpenAI ChatGPT API to process images and extract information from them. The script reads
images from a directory, resizes them, and sends them to the API along with a prompt. The API generates a response
containing the extracted information in JSON format. The script saves the extracted information to a JSON file with
the same name as the image file. The script processes multiple images in a batch."""

# Import the required libraries
import base64
import json
import os
import re
import time
from io import BytesIO
from PIL import Image

# Import the OpenAI client
from openai import OpenAI

# Save the start time, set the image and output directories
start_time = time.time()
total_files = 0
total_in_tokens = 0
total_out_tokens = 0
input_cost_per_mio_in_dollars = 2.5
output_cost_per_mio_in_dollars = 10

image_directory = "/Users/svenburkhardt/Library/Mobile Documents/com~apple~CloudDocs/1 Uni/Master/1_Studienfächer/Digital Humanities/FS2025/Rise_API_Course/Rise_Api_course_Input"
output_directory = "/Users/svenburkhardt/Library/Mobile Documents/com~apple~CloudDocs/1 Uni/Master/1_Studienfächer/Digital Humanities/FS2025/Rise_API_Course/Rise_Api_course_output'"

# Clear the output directory
for root, _, filenames in os.walk(output_directory):
    for filename in filenames:
        os.remove(os.path.join(root, filename))

# Set the API key, model, section, and temperature

#api_key = os.getenv("OPENAI_API_KEY")    #personal api_key =
api_key =  "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"
model = "gpt-4o"
section = "A"
temperature = 0.5

# Create an instance of the OpenAI API client
client = OpenAI(api_key=api_key)

# Process each image in the image_data directory
for root, _, filenames in os.walk(image_directory):
    file_number = 1
    total_files = len(filenames)
    for filename in filenames:
        if filename.endswith(".jpg"):
            print("----------------------------------------")
            print(f"> Processing file ({file_number}/{total_files}): {filename}")
            image_id = filename.split(".")[0]

            with Image.open(os.path.join(root, filename)) as img:
                # Preserve the aspect ratio while resizing the image to fit within 1024x1492
                print("> Resizing the image...", end=" ")
                img.thumbnail((1024, 1492))

                # Save the resized image to a BytesIO object
                buffered = BytesIO()
                img.save(buffered, format="JPEG")

                # Convert the resized image to base64
                base64_image = base64.b64encode(buffered.getvalue()).decode("utf-8")
                print("Done.")

            # Create the prompt for the model
            print("> Sending the image to the API and requesting answer...", end=" ")
            prompt = f"""
    The page id is "{image_id}".
    I am providing you with images of documents from a corpus of the Männerchor Murg from Germany,
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
    Represent any line breaks in the text as real line breaks rather than `\n`.
    The pictures do have Tags in them, namely   "Handschrift", "Maschinell", "mitUnterschrift", "Bild". Extract and mention those in the Json below.
    The JSON should be structured like this:
    ```json
    {{
        "page_id": "",
        "metadata": {{
            "author": {{
                "forename": "",
                "familyname": "",
                "role": ""
            }},
            "recipient": {{
                "forename": "",
                "familyname": "",
                "role": "",
                "position": ""
            }},
            "mentioned_persons": [
                {{
                    "forename": "",
                    "familyname": "",
                    "role": "",
                    "position": ""
                }}
            ],
            "mentioned_organization": [
                {{
                    "Organization_name": "",
                    "place_name": "",
                }}
            ],
            "mentioned_events": [
                {{
                    "date": "",
                    "description": ""
                }}
            ],
            "creation_dates": [
                {{
                    "day": "",
                    "month": "",
                    "year": ""
                }}
            ],
            "creation_place": [
                ""  
            ],
            "mentioned_dates": [
                {{
                    "day": "",
                    "month": "",
                    "year": ""
                }}
            ],
            mentioned_places": [
                ""
            ],
            }},
            "content_tags": [
                ""
            ],
            "document_type_options": [
                "Brief",
                "Protokoll",
                "Postkarte",
                "Rechnung",
                "Regierungsdokument",
                "Karte",
                "Noten",
                "Zeitungsartikel",
                "Liste",
                "Website",
                "Notizzettel",
                "Offerte"
            ],
            "document_format_options": [
                "Handschrift",
                "Maschinell"
                "mitUnterschrift",
                "Bild",
            
        }}
    """

            workload = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]
                },
                {
                    "role": "system",
                    "content": "You are a precise list-reading machine and your answers are plain JSON."
                }
            ]

            answer = client.chat.completions.create(messages=workload,
                                                    model=model,
                                                    temperature=temperature)
            print("Done.")

            # Extract the answer from the response
            answer_text = answer.choices[0].message.content
            print("> Received an answer from the API. Token cost (in/out):", answer.usage.prompt_tokens, "/",
                  answer.usage.completion_tokens)
            total_in_tokens += answer.usage.prompt_tokens
            total_out_tokens += answer.usage.completion_tokens

            print("> Processing the answer...")
            # Save the answer to a json file. The filename should be the image_id with a .json extension
            # The response from the API is a string which encloses the JSON object. We need to remove the enclosing
            # quotes to get the JSON object. ```json [data] ``` -> [data]
            pattern = r"```\s*json(.*?)\s*```"
            match = re.search(pattern, answer_text, re.DOTALL)
            if match:
                # Extract the JSON content
                answer_text = match.group(1).strip()

                # Parse the JSON content into a Python object
                try:
                    answer_data = json.loads(answer_text)
                except json.JSONDecodeError as e:
                    print(f"> Failed to parse JSON: {e}")
                    answer_data = None

                if answer_data:
                    # Create the answers directory if it doesn't exist
                    os.makedirs(output_directory, exist_ok=True)

                    # Save the answer to a JSON file
                    with open(f"{output_directory}/{image_id}.json", "w", encoding="utf-8") as json_file:
                        json.dump(answer_data, json_file, indent=4)
                        print(f"> Saved the answer for {image_id} to {output_directory}/{image_id}.json")
            else:
                print("> No match found for the JSON content.")

            # File complete: Increment the file number
            file_number += 1
            print("> Processing the answer... Done.")

# Calculate and print the total processing time
end_time = time.time()
total_time = end_time - start_time
print("----------------------------------------")
print(f"Total processing time: {total_time:.2f} seconds")
print(f"Total token cost (in/out): {total_in_tokens} / {total_out_tokens}")
print(f"Average token cost per image: {total_out_tokens / total_files}")
print((
    f"Total cost (in/out): ${total_in_tokens / 1e6 * input_cost_per_mio_in_dollars:.2f} / "
    f"${total_out_tokens / 1e6 * output_cost_per_mio_in_dollars:.2f}"
))
print("----------------------------------------")