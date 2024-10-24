import os
import re

def check_missing_and_found_files(dest_folder):
    # Liste aller erwarteten Zahlen von 001 bis 425
    expected_numbers = [f"{i:03d}" for i in range(1, 426)]

    # Liste, um gefundene Zahlen zu speichern
    found_numbers = []

    # Durchgehen des Ordners und Überprüfen der Dateinamen
    for root, dirs, files in os.walk(dest_folder):
        for file in files:
            # Überprüfen, ob der Dateiname dem Schema "Akte_Zahl" entspricht
            match = re.match(r"Akte_(\d{3})", file)
            if match:
                found_number = match.group(1)
                if found_number not in found_numbers:
                    found_numbers.append(found_number)

    # Fehlende Zahlen berechnen
    missing_numbers = [num for num in expected_numbers if num not in found_numbers]

    # Sortiere die Liste der gefundenen Zahlen
    found_numbers_sorted = sorted(found_numbers)

    return missing_numbers, found_numbers_sorted


# Pfad zum Zielordner mit den JPEG-Dateien
dest_folder = r"/Users/svenburkhardt/Documents/D_Murger_Männer_Chor_Forschung/Masterarbeit/JPEG_Akten_Scans"

# Funktion aufrufen und fehlende und gefundene Zahlen ermitteln
missing_numbers, found_numbers_sorted = check_missing_and_found_files(dest_folder)

# Fehlende und gefundene (sortierte) Zahlen ausgeben
if missing_numbers:
    print(f"Die folgenden Zahlen fehlen in den Dateinamen: {missing_numbers}")
else:
    print(f"Alle Zahlen von 001 bis 425 sind vorhanden.", f"Gefundene Zahlen: {found_numbers_sorted}")


