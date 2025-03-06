import pandas as pd
import re

def erstelle_gesamtuebersicht(
    finder_tags_csv,
    akten_uebersicht_csv,
    ausgabe_csv
):
    """
    Liest die CSV mit Finder-Tags (Seitenebene) und die CSV mit Akteninfos (Aktebene) ein.
    Extrahiert aus dem Dateinamen die Akten-Nummer und führt beide Tabellen zusammen.
    Das Ergebnis wird als CSV gespeichert.
    """
    
    # 1) finder_tags.csv einlesen
    df_finder = pd.read_csv(finder_tags_csv, sep=';')

    # 2) Akten_Übersicht.csv einlesen
    df_akten = pd.read_csv(akten_uebersicht_csv, sep=';')

    # 3) Aus dem Dateinamen die Akten-Nummer extrahieren.
    df_finder['AkteNummer'] = df_finder['Dateiname'].str.extract(r'Akte_(\d+)_')

    # 4) In df_akten heißt die Akte-Spalte "Lage im Ordner".
    df_akten.rename(columns={'Lage im Ordner': 'AkteNummer'}, inplace=True)

    # 5) Beide Spalten auf String setzen, um Merge-Probleme zu vermeiden
    df_finder['AkteNummer'] = df_finder['AkteNummer'].astype(str)
    df_akten['AkteNummer'] = df_akten['AkteNummer'].astype(str)

    # 6) Beide DataFrames zusammenführen
    df_merged = pd.merge(df_finder, df_akten, how='left', on='AkteNummer')

    # 7) Ergebnis speichern
    df_merged.to_csv(ausgabe_csv, sep=';', index=False)
    print(f"Gesamtübersicht erzeugt: {ausgabe_csv}")

if __name__ == "__main__":
    finder_tags_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/finder_tags.csv"
    akten_uebersicht_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Akten_Übersicht-Männerchor_Murg_II_WK.csv"
    ausgabe_csv = "/Users/svenburkhardt/Developer/masterarbeit/3_MA_Project/Data/Gesamtübersicht.csv"
    
    erstelle_gesamtuebersicht(finder_tags_csv, akten_uebersicht_csv, ausgabe_csv)
