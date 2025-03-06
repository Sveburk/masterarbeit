import os
import re
import xattr
import plistlib
import csv

def get_finder_tags(filepath):
    """
    Liest die macOS-Finder-Tags (falls vorhanden) aus den
    erweiterten Dateiattributen (xattrs) einer Datei aus
    und entfernt \n1 bis \n10.
    """
    try:
        exattrs = xattr.xattr(filepath)
        data = exattrs.get(b'com.apple.metadata:_kMDItemUserTags')
        if data:
            tags = plistlib.loads(data)
            
            # Entfernt exakt \n gefolgt von Ziffern 1 bis 9 oder 10
            # also \n1, \n2, ... \n9 oder \n10
            pattern = r'\n(?:[1-9]|10)'
            cleaned_tags = [re.sub(pattern, '', tag) for tag in tags]
            return cleaned_tags
    except Exception as e:
        print(f"Fehler beim Auslesen der Finder-Tags für {filepath}: {e}")
    return []

def read_image_tags(directory, csv_output):
    """
    Geht alle JPEGs im Verzeichnis durch, ermittelt die Finder-Tags
    und schreibt sie in eine CSV-Datei (Spalten: Dateiname, Finder-Tags).
    """
    files = [f for f in os.listdir(directory) if f.lower().endswith(('.jpg', '.jpeg'))]
    
    with open(csv_output, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(["Dateiname", "Finder-Tags"])
        
        for filename in files:
            path = os.path.join(directory, filename)
            finder_tags = get_finder_tags(path)
            
            if finder_tags:
                tags_string = ", ".join(finder_tags)
            else:
                tags_string = ""
            
            writer.writerow([filename, tags_string])
            
            # Konsolenausgabe zur Kontrolle
            print(f"Datei: {filename}")
            if finder_tags:
                print(f"  Finder-Tags: {finder_tags}")
            else:
                print("  Keine Finder-Tags vorhanden.")

if __name__ == "__main__":
    dir_path = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/JPEG_Akten_Scans"
    csv_output_path = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/finder_tags.csv"
    
    read_image_tags(dir_path, csv_output_path)
    print(f"\nCSV-Datei wurde erstellt: {csv_output_path}")
