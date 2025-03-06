import pandas as pd
import re

def normalisiere_akten_nummer_finder(eintrag):
    """
    Extrahiert aus einem Eintrag wie "Akte_229_S001.jpg" die Nummer (z.B. "229")
    und füllt sie auf 3 Stellen auf.
    """
    m = re.search(r'Akte_(\d+)', eintrag)
    if m:
        num = m.group(1)
        return num.zfill(3)
    return None

def normalisiere_akten_nummer_transkribus(akte_wert):
    """
    Entfernt den Präfix "Akte_" aus einem Eintrag (z.B. "Akte_001") und füllt ihn auf 3 Stellen auf.
    """
    if isinstance(akte_wert, str):
        if akte_wert.startswith("Akte_"):
            num = akte_wert.replace("Akte_", "")
        else:
            num = akte_wert
        return num.zfill(3)
    return None

def erstelle_gesamtuebersicht(finder_tags_csv, akten_uebersicht_csv, transkribus_csv, ausgabe_csv):
    """
    Liest drei CSV-Dateien ein:
      - finder_tags_csv: Enthält Seiteninformationen. Die Spalte heißt entweder "Dateiname" oder "Akte".
      - akten_uebersicht_csv: Enthält Akteninformationen. Die Spalte "Lage im Ordner" wird in "AkteNummer" umbenannt.
      - transkribus_csv: Enthält weitere Übersichtsinformationen (Spalte "Akte"), die in "AkteNummer" umbenannt werden.
    
    Normalisiert die Aktennummern in allen DataFrames (auf 3 Stellen) und führt beide Übersichten auf alle Seiten zusammen.
    Das Ergebnis wird als CSV gespeichert.
    """
    
    # 1) finder_tags CSV einlesen
    df_finder = pd.read_csv(finder_tags_csv, sep=';')
    # Falls "Dateiname" nicht existiert, verwende "Akte" als Spaltenname:
    spalte = "Dateiname" if "Dateiname" in df_finder.columns else "Akte"
    df_finder['AkteNummer'] = df_finder[spalte].apply(normalisiere_akten_nummer_finder)
    
    # 2) Aktenübersicht einlesen und Spalte umbenennen
    df_akten = pd.read_csv(akten_uebersicht_csv, sep=';')
    df_akten.rename(columns={'Lage im Ordner': 'AkteNummer'}, inplace=True)
    df_akten['AkteNummer'] = df_akten['AkteNummer'].astype(str).str.zfill(3)
    
    # 3) Transkribus-Collection Übersicht einlesen und Spalte umbenennen
    df_collection = pd.read_csv(transkribus_csv, sep=';')
    df_collection.rename(columns={'Akte': 'AkteNummer'}, inplace=True)
    df_collection['AkteNummer'] = df_collection['AkteNummer'].apply(normalisiere_akten_nummer_transkribus)
    
    # 4) Merge der DataFrames: Zuerst df_finder mit df_akten
    df_merged = pd.merge(df_finder, df_akten, how='left', on='AkteNummer')
    # Anschließend mit df_collection
    df_merged_final = pd.merge(df_merged, df_collection, how='left', on='AkteNummer')
    
    # 5) Ergebnis als CSV speichern
    df_merged_final.to_csv(ausgabe_csv, sep=';', index=False)
    print(f"Gesamtübersicht erzeugt: {ausgabe_csv}")

if __name__ == "__main__":
    finder_tags_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/finder_tags.csv"
    akten_uebersicht_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Akten_Übersicht-Männerchor_Murg_II_WK.csv"
    transkribus_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Transkibus_Collection_Übersicht_Maennerchor_Murg.csv"
    ausgabe_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Gesamtübersicht.csv"
    
    erstelle_gesamtuebersicht(finder_tags_csv, akten_uebersicht_csv, transkribus_csv, ausgabe_csv)
