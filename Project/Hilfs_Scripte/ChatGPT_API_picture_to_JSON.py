#This code was mainly created and later adopted during a Workshop with RISE on AI and API on 25.10.24
# Import the required libraries
import base64  # for encoding image data as a base64 string
import json  # for handling JSON data structures
import os  # for interacting with the operating system and file structure
import re  # for regular expressions used in parsing API responses
import time  # for tracking the script’s processing time
from io import BytesIO  # for handling in-memory image data
from PIL import Image  # for processing and resizing images

# Import the OpenAI client
from openai import OpenAI  # OpenAI client to interact with the ChatGPT API

# Initialize time tracking and counters for API cost calculation
start_time = time.time()  # Record the start time of the script execution
total_files = 0  # Counter for the total number of files processed
total_in_tokens = 0  # Counter for input tokens to the API
total_out_tokens = 0  # Counter for output tokens from the API
input_cost_per_mio_in_dollars = 2.5  # Cost per million input tokens
output_cost_per_mio_in_dollars = 10  # Cost per million output tokens

# Directories for input images and output JSON files
image_directory = "/Users/svenburkhardt/Developer/masterarbeit/Project/Data/Json_file_test_folder/Test_Data_JSON_files_openai"
output_directory = "/Users/svenburkhardt/Developer/masterarbeit/Project/Data/Json_file_test_folder/Test_answer_JSON_files_openai"

# Clear any pre-existing files in the output directory to prevent old data mix-ups
for root, _, filenames in os.walk(output_directory):
    for filename in filenames:
        os.remove(os.path.join(root, filename))  # Remove each file in the output directory

# Set the API key, model parameters, and other configurations
organization_id = "org-kCXwagm4pKQHcMdTVlQ8Qp7m"  # Organization ID for OpenAI
project_id = "proj_HphMz16KxX6eEVA7LXbZ3dD9"  # Project ID


api_key = os.getenv("OPENAI_API_KEY")                                    # API Key in Umgebungsvariable#
if api_key is None:
    raise ValueError("API key not found. Please set the OPENAI_API_KEY environment variable.")


model = "gpt-4o"  # Model name used for the API request
section = "A"  # Optional section parameter (if applicable)
temperature = 0.5  # Controls randomness in the API's response generation

# Create an instance of the OpenAI API client
client = OpenAI(api_key=api_key)  # Initialize the API client with the given API key

# Process each image file in the specified image directory
for root, _, filenames in os.walk(image_directory):
    file_number = 1  # Tracks the current file number for processing feedback
    total_files = len(filenames)  # Count the total number of files to process
    for filename in filenames:
        if filename.endswith(".jpg"):  # Only process files with a .jpg extension
            print("----------------------------------------")
            print(f"> Processing file ({file_number}/{total_files}): {filename}")
            
            # Extract the filename without the .jpg extension for use as image_id
            image_id = filename.rsplit(".jpg", 1)[0]  # Splits before ".jpg" to get just the name
            
            # Open, resize, and convert the image to a base64-encoded string
            with Image.open(os.path.join(root, filename)) as img:
                print("> Resizing the image...", end=" ")
                img.thumbnail((1024, 1492))  # Resizes the image while preserving aspect ratio

                # Save the resized image in memory as a base64-encoded string for API transmission
                buffered = BytesIO()
                img.save(buffered, format="JPEG")
                base64_image = base64.b64encode(buffered.getvalue()).decode("utf-8")
                print("Done.")

            # Formulate the prompt for the ChatGPT model with context and expected JSON structure
            print("> Sending the image to the API and requesting answer...", end=" ")
            prompt = f"""
            The page id is "{image_id}".
            I am providing you with images of documents from a corpus of the Männerchor Murg from Germany,
            dating from 1925 to 1945.
            This period covers the Weimar Republic and the so-called Third Reich, which may be reflected in 
            language and context.
            Your task is to analyze each image for its text content and extract and compare relevant information.
            Keep in mind the context I provided. The data will be used for scientific research, so it is essential 
            that the data is absolutely accurate.
            If there is no data for a particular item, please write "None". Use the list under "document_type_options" 
            to identify and select the appropriate document type for the current document. Choose only the type that best matches.

            I am interested in: Metadata such as author, recipient, other mentioned persons, location(s), date(s), 
            and events including Sender, Recipient, and geographical places as well as content tags in a structured JSON file.
            Please ensure that all German text, especially umlauts (ä, ö, ü) and the ß character, is encoded and outputted in UTF-8 format.
            The JSON should be structured like this:
            ```json
            {{
                "page_id": "",
                "metadata": {{
                    "author": {{
                        "forename": "",
                        "familyname": "",
                        "role": "",
                        "position": ""
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
                    "named_entities": [
                        {{
                            "type": "",
                            "name": ""
                        }}
                    ],
                    "events": [
                        {{
                            "date": "",
                            "description": ""
                        }}
                    ],
                    "dates": [
                        {{
                            "day": "",
                            "month": "",
                            "year": ""
                        }}
                    ],
                    "places_mentioned": [
                        ""
                    ],
                    "summary": {{
                        "german": "",
                        "english": ""
                    }},
                    "content_tags": [
                        ""
                    ],
                    "content_transcription": "",
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
                    ]
                }}
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