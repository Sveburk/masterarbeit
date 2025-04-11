import pandas as pd
import re

# Pfad zur CSV mit Ground Truth-Typen
CSV_TYPE_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Akten_Gesamtübersicht.csv"

# Lade CSV einmal beim Import
try:
    type_df = pd.read_csv(CSV_TYPE_PATH, sep=";")
    type_df.columns = [col.strip() for col in type_df.columns]  # Spaltennamen trimmen
except Exception as e:
    print(f"[Fehler] Konnte Ground Truth CSV nicht laden: {e}")
    type_df = pd.DataFrame()

import xml.etree.ElementTree as ET
import os
from typing import Optional


import pandas as pd
import re
import xml.etree.ElementTree as ET
from typing import Optional

# CSV-Pfad und Laden (nur einmal beim Import)
CSV_TYPE_PATH = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Akten_Gesamtübersicht.csv"
try:
    type_df = pd.read_csv(CSV_TYPE_PATH, sep=";", dtype=str)
    type_df.columns = [col.strip() for col in type_df.columns]
    type_df["csv_page_number"] = type_df["Akte_Scan"].str.extract(r"_S(\d{3})")[0]
    type_df["Transkribus-ID"] = type_df["Transkribus-ID"].str.strip()
except Exception as e:
    print(f"[Fehler] Konnte Ground Truth CSV nicht laden: {e}")
    type_df = pd.DataFrame()

def get_document_type(filename: str, xml_path: Optional[str] = None, debug: bool = False) -> str:
    match = re.match(r"(\d{7})_Akte_.*?p(?:age)?(\d+)", filename, re.IGNORECASE)
    if not match:
        if debug:
            print(f"[DEBUG] Kein Match für Dateiname: {filename}")
        return ""

    transkribus_id = match.group(1)
    page_number = match.group(2).zfill(3)

    row = type_df[
        (type_df["Transkribus-ID"] == transkribus_id) &
        (type_df["csv_page_number"] == page_number)
    ]

    if not row.empty:
        doc_type = row.iloc[0].get("Dokumententyp", "").strip()
        if debug:
            print(f"[DEBUG] Typ aus CSV: {doc_type} für ID {transkribus_id}, Seite {page_number}")
        return doc_type

    if xml_path:
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for elem in root.iter():
                if elem.tag.endswith("CSVData"):
                    typ = elem.findtext("Dokumententyp", default="").strip()
                    if typ:
                        if debug:
                            print(f"[DEBUG] Fallback-Typ aus XML: {typ}")
                        return typ
        except Exception as e:
            if debug:
                print(f"[DEBUG] Fehler beim XML-Fallback: {e}")

    if debug:
        print(f"[DEBUG] Kein Typ gefunden für {filename}")
    return ""

# Beispiel-Call
if __name__ == "__main__":
    print(get_document_type("6489763_Akte_078_pdf_page002", debug=True))
    print(get_document_type("6489763_Akte_078.1_pdf_page003", debug=True))
