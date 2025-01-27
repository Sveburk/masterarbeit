import requests
import csv

# API-Login
url = "https://readcoop.eu/transkribus/api/login"
credentials = {"user": "mail@sven-burkhardt.de", "pw": "Sven97"}
session = requests.post(url, data=credentials).json()["sessionId"]

# Dokumente abrufen
collection_id = "1903711"  # ID deiner Sammlung
documents_url = f"https://readcoop.eu/transkribus/api/collections/{collection_id}"
headers = {"cookie": f"JSESSIONID={session}"}
response = requests.get(documents_url, headers=headers)
documents = response.json()["colDocs"]

# CSV exportieren
with open("transkribus_documents.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Document Name", "Document ID"])  # Spaltenüberschriften
    for doc in documents:
        writer.writerow([doc["title"], doc["docId"]])

print("Export abgeschlossen: transkribus_documents.csv")
