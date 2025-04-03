Workflow to structure and extract data
======================================

Goal
-----
1. **Data Collection**: Gather all data and imagery from the export folder 3_MA_Project/Transkribus_Export_*. The data is organized in documents.
2. **Schema Creation**: Create a schema for the types of data to be processed.
3. **Data Processing**: Process the collected data to extract entities, normalize them, and resolve duplicates.
4. **Metadata Update**: Update the metadata of the processed data to include additional information. Refer to "Akten_Gesamtübersicht.csv" where each Transkribus document has a dedicated entry, identified by the column "Transkribus-ID"
5. **Data Export**: Export the processed data in a structured format for further analysis.
6. **Documentation**: Document the workflow and provide instructions for future users.

## Workflow Steps

### Data Collection
All data is downloaded and ready to process. Be aware that we will download part of the data again.
Each document has a dedicated folder in the export folder. In there you find a subfolder (=data) and a PDF (=raw version).
In the subfolder you find metadata xml files and the images. In another subfolder called "pages" you find the PAGE XML files.

### Schema Creation
In "Akten_Gesamtübersicht.csv" you find metadata for each document. The column "Transkribus-ID" is the unique identifier for each document.
You'll find a column called "Dokumententyp" indicating the type of document.  For each type of document, we will create a schema.
The schemas should have a common base schema, which includes the following fields:
```python
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
```

Each document type could have additional fields. For now, this base schema suffices.


### Data Processing
In "Akten_Gesamtübersicht.csv" you find metadata for each document. The column "Transkribus-ID" is the unique identifier for each document.
You'll find a column called "Dokumententyp" indicating the type of document. Process the data to output json which validates against its type-schema.

#### Implementation Status
- ✅ Created `transkribus_to_base_schema.py` script in `3_MA_Project/Hilfs_Scripte/` that:
  - Extracts metadata from Transkribus XML files
  - Parses custom attributes to identify persons, organizations, dates, and places
  - Formats dates in YYYY.MM.DD format
  - Outputs structured JSON files following the base schema
  - Successfully processed 365 files from the Transkribus export folders

- ✅ Created `document_schemas.py` with object-oriented schema definitions for:
  - Base document class with validation methods
  - Person, Organization, Place and Event classes
  - Specialized document types (Brief, Postkarte, Protokoll)

- Next steps:
  - Document type classification based on content
  - Multi-page document consolidation
  - Integration with CSV metadata

### Metadata Update
The metadata is updated in the "Akten_Gesamtübersicht.csv" file. The column "Transkribus-ID" is the unique identifier for each document.

### Data Export
The processed data is exported in a structured format (JSON) for further analysis. The output files are stored in the "output" directory.
Optional, I would like an sqlite database, but this is not necessary for the first version.

### Documentation
This file is the workflow documentation. It should be updated as the workflow is developed.