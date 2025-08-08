#!/usr/bin/env python3
import os
import openai
from pathlib import Path
from tqdm import tqdm
import re
import xml.etree.ElementTree as ET


# --- Kosten-Konstanten (GPT-4o, Stand April 2025) ---
INPUT_COST_PER_1K = 0.0011   # USD per 1K input tokens (non-cached)
OUTPUT_COST_PER_1K = 0.0044  # USD per 1K output tokens

# --- Konfiguration: Pfade anpassen falls nötig ---
TRANSKRIBUS_DIR = Path("/Users/svenburkhardt/Downloads/export_job_17826913")

# --- Hilfsfunktionen ---
def get_api_client() -> openai.OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Kein OPENAI_API_KEY in der Umgebung gefunden.")
    return openai.OpenAI(api_key=api_key)

def annotate_with_llm(xml_content: str,
                      client: openai.OpenAI,
                      model: str = "gpt-4o",
                      temperature: float = 0.0) -> dict:
    """
    Sendet `xml_content` an die LLM und gibt ein dict zurück:
      {
        "annotated_xml": <string>,
        "llm_metadata": {
            "model": ...,
            "input_tokens": ...,
            "output_tokens": ...,
            "cost_usd": ...
        }
      }
    """
    prompt = f"""
    Systemrolle:
    Du bist ein spezialisiertes XML-Annotationstool für historische Transkribus-Dokumente.

    Aufgabe:
    Analysiere das gesamte PAGE-XML-Dokument. Extrahiere Entitäten aus dem Unicode-Text aller <TextLine>-Elemente und füge strukturierte `custom="..."`-Attribute hinzu. 

    Strikte Regeln:

    1. Dokumentanalyse:
    - Verarbeite ausschließlich <TextLine>-Elemente.
    - Verwende nur <Unicode>-Inhalte als Eingabetext.

    2. Globale Personenerkennung:
    - Erkenne Personen (inkl. Titel, Vorname, Nachname).
    - Speichere `offset` und `length` für jede erkannte Person pro TextLine.
    - Verwende **immer dieselben Offsets** bei wiederholten Nennungen im Dokument.

    3. Empfängerkennung (`recipient`):
    - Der Kopfbereich endet an der ersten komplett leeren TextLine.
    - Erkenne dort Anreden (z. B. „Herr“, „Frau“, „Sehr geehrter Herr …“).
    - Verknüpfe Anrede mit passender Person und annotiere mit:
    `recipient {{offset:X; length:Y;}}`
    
    4. Autorenkennung (`author`):
    - Der Fußbereich beginnt nach der letzten Grußformel (z. B. „Mit freundlichen Grüßen“).
    - Namen → `author {{offset:X; length:Y;}}`.
    - Funktion (z. B. „Chorleiter“) → `role {{offset:X; length:Y;}}`.

     5. Ort- und Datumsannotation:
    - **Absendeort** (creation_place) und **Erstellungsdatum** (creation_date):
        zusätzlich zu den Tags place und date hinzu:
        creation_place {{offset:X; length:Y;}} und creation_date {{offset:X; length:Y; when:TT.MM.JJJJ;}}.
    - **Empfangsort** (recipient_place):
        Füge im Empfänger-Block die passende Zeile mit:
        place {{offset:X; length:Y;}}.

    6. Entitäten pro Zeile (in dieser Reihenfolge):
    Füge **ein** Attribut `custom="..."` ein mit nur den tatsächlich erkannten Entitäten:


    person {{offset:X; length:Y;}}
    recipient {{offset:X; length:Y;}}
    author {{offset:X; length:Y;}}
    organization {{offset:X; length:Y;}}
    place {{offset:X; length:Y;}}
    date {{offset:X; length:Y; when:TT.MM.JJJJ;}}
    role {{offset:X; length:Y;}}
    event {{offset:X; length:Y;}} → optional mit when:TT.MM.JJJJ;

    Hinweise:
    - Füge **nur tatsächlich vorhandene Entitäten** ein.
    - Keine Platzhalter.
    - Format für `date` und `event` (falls Datum erkennbar): `when:TT.MM.JJJJ;`
    - Mehrzeilige Events (z. B. bei Bindestrich am Ende oder fortgeführtem Satz) erhalten dieselbe `event`-Annotation in allen betroffenen Zeilen.

    6. XML-Regeln:
    - **Verändere nur** `custom`-Attribute innerhalb von `<TextLine>`.
    - Belasse alle anderen XML-Strukturen vollständig unverändert.

    7. Ausgabe:
    - Gib ausschließlich ein vollständiges, wohlgeformtes XML zurück.
    - Kein Freitext, kein Kommentar, kein Markdown.

    Beispielausgabe:
    <?xml version="1.0" encoding="UTF-8"?>
    <PcGts xmlns="http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15">
    <Page imageFilename="dummy.jpg" imageWidth="1000" imageHeight="1000">
        <TextRegion id="r1">
        <TextLine id="tl1" custom="place {{offset:0; length:7;}} creation_place {{offset:0; length:7;}} date {{offset:8; length:9; when:28.05.1942;}} creation_date {{offset:8; length:9; when:28.05.1942;}}">
            <Coords points="0,0 100,0 100,10 0,10"/>
            <TextEquiv><Unicode>München 28.V.1942</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl2" custom="recipient {{offset:7; length:5;}} person {{offset:7; length:5;}} place {{offset:15; length:6;}} recipient_place {{offset:15; length:6;}}">
            <Coords points="0,20 100,20 100,30 0,30"/>
            <TextEquiv><Unicode>Lieber Otto, Berlin</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl3" custom="event {{offset:24; length:38; when:28.05.1942;}} place {{offset:42; length:7;}}">
            <Coords points="0,40 100,40 100,50 0,50"/>
            <TextEquiv><Unicode>Heute abend fand ein Konzert im Opernhaus in München statt, und ich</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl4" custom="organization {{offset:43; length:28;}} place {{offset:66; length:16;}}">
            <Coords points="0,60 100,60 100,70 0,70"/>
            <TextEquiv><Unicode>lauschte den himmlischen Stimmen des Männerchors Hintertuüpfingen eV.</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl5" custom="organization {{offset:34; length:3;}} organization {{offset:40; length:18;}}">
            <Coords points="0,80 100,80 100,90 0,90"/>
            <TextEquiv><Unicode>Das alles fand im Rahmen des WhW - des Winterhilfswerk statt.</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl6" custom="organization  {{offset:50; length:17;}} place {{offset:72; length:4;}} place {{offset:83; length:6;}}">
            <Coords points="0,100 100,100 100,110 0,110"/>
            <TextEquiv><Unicode>Ich hoffe wir sehen uns bald bei einem Auftritt des Männerchors Murg wieder, oder in Hänner?</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl7" custom="role {{offset:14; length:14;}} person {{offset:29; length:4;}}">
            <Coords points="0,120 100,120 100,130 0,130"/>
            <TextEquiv><Unicode>Grüss mir den Vereinsführer Asal,</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl8">
            <Coords points="0,140 100,140 100,150 0,150"/>
            <TextEquiv><Unicode>Alles Liebe,</Unicode></TextEquiv>
        </TextLine>
        <TextLine id="tl9" custom="author {{offset:6; length:17;}} person {{offset:6; length:17;}}">
            <Coords points="0,160 100,160 100,170 0,170"/>
            <TextEquiv><Unicode>Deine Lina Fingerdick</Unicode></TextEquiv>
        </TextLine>
        <!-- Neue Zeile für den Empfangsort -->
        <TextLine id="tl10" custom="salutation {{offset:0; length:2;}} recipient {{offset:3; length:13;}} address {{offset:18; length:21;}} place {{offset:41; length:4;}}">
            <Coords points="0,180 100,180 100,190 0,190"/>
            <TextEquiv>
            <Unicode>An Otto Bolliger, Adolf-Hitler Platz 1, Murg</Unicode>
            </TextEquiv>
        </TextLine>
        </TextRegion>
    </Page>
    </PcGts>

    Hier ist das zu annotierende XML:

```
{xml_content}
```

"""
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        messages=[
            {"role": "system", "content": "You are an XML‐annotation assistant."},
            {"role": "user",   "content": prompt}
        ]
    )

    usage = resp.usage
    in_tok = usage.prompt_tokens
    out_tok = usage.completion_tokens
    cost   = round(in_tok/1000*INPUT_COST_PER_1K + out_tok/1000*OUTPUT_COST_PER_1K, 4)

    return {
        "annotated_xml": resp.choices[0].message.content,
        "llm_metadata": {
            "model": model,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "cost_usd": cost
        }
    }

def process_file(xml_path: Path, client: openai.OpenAI):
    """Annotiert eine einzelne XML und speichert sie direkt im gleichen Ordner."""
    try:
        # Ordnername und Pfad ermitteln
        folder_name = xml_path.parent.name
        folder_path = xml_path.parent.resolve()

        # Neue Ausgabe
        print(f"\nVerarbeite Datei: {xml_path.name}")
        print(f"  • Verzeichnis: {folder_path}")
        print(f"  • Ordnername : {folder_name}")
        print("  → Starte Annotation via OpenAI…", end="", flush=True)

        xml_text = xml_path.read_text(encoding="utf-8")
        print(f"\nVerarbeite Datei: {xml_path.name}")


        result    = annotate_with_llm(xml_text, client)
        annotated = clean_llm_output(result["annotated_xml"])
        meta      = result["llm_metadata"]

        # ✅ Check if LLM returned valid XML
        if not annotated.strip().startswith("<"):
            raise ValueError(
                f"LLM did not return valid XML for {xml_path.name}.\n"
                f"Response starts with: {annotated[:200]!r}"
            )
        try:
            ET.fromstring(annotated)  # validiert grob das XML
        except ET.ParseError as pe:
            raise ValueError(
                f"Returned XML is not well-formed for {xml_path.name}: {pe}\n"
                f"Start of XML: {annotated[:200]!r}"
            )


        print(" ✓ zurück, speichere…")

        # hier direkt neben der Originaldatei speichern
        out_path = xml_path.with_name(f"{xml_path.stem}_preprocessed{xml_path.suffix}")
        out_path.write_text(annotated, encoding="utf-8")
        folder_name = xml_path.parent.name
        
        print(f"    → Ordner      : {folder_name}")
        print(f"    → Gespeichert unter: {out_path}")
        print(f"    • Model: {meta['model']}")
        print(f"    • Input-Tokens: {meta['input_tokens']}, Output-Tokens: {meta['output_tokens']}")
        print(f"    • Geschätzte Kosten: ${meta['cost_usd']}\n")

        return out_path

    except Exception as e:
        print(f"  ✗ Fehler bei {xml_path.name}: {e}")
        return None
    
def clean_llm_output(raw: str) -> str:
    """
    Extrahiert den XML-Teil aus der LLM-Antwort, entfernt Markdown-Wrapper wie ```xml\n...\n```.
    """
    # Versuche, Block mit ```xml ... ``` zu extrahieren
    match = re.search(r"```xml\s*(.*?)```", raw, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Fallback: Wenn keine ```xml Blöcke vorhanden, aber es irgendwo mit < beginnt
    xml_start = raw.find("<?xml")
    if xml_start >= 0:
        return raw[xml_start:].strip()

    return raw.strip()  # wenn alles andere fehlschlägt

def main():
    client = get_api_client()

    # find all XMLs in the Transkribus structure
    xml_files = []
    for seven in sorted(TRANSKRIBUS_DIR.iterdir()):
        if not seven.is_dir() or not seven.name.isdigit():
            continue
        for sub in sorted(seven.iterdir()):
            if not sub.is_dir() or not sub.name.startswith("Akte_"):
                continue

            page_dir = sub / "page"
            if not page_dir.is_dir():
                continue


            # --- Neuer Check: Ordner überspringen, wenn ≥50 % schon vorverarbeitet ---
            all_xmls = list(page_dir.glob("*.xml"))
            if all_xmls:
                preproc = [p for p in all_xmls if p.stem.endswith("_preprocessed")]
                ratio = len(preproc) / len(all_xmls)
                if ratio >= 0.5:
                    print(f"Überspringe Ordner {page_dir.name}: "
                        f"{len(preproc)}/{len(all_xmls)} Dateien vorverarbeitet ({ratio:.0%}).")
                    continue

            # nur die noch nicht vorverarbeiteten Dateien zur Liste hinzufügen
            to_process = [p for p in sorted(all_xmls)
                        if not p.stem.endswith("_preprocessed")]
            xml_files.extend(to_process)


    print(f"Starte LLM-Annotation für {len(xml_files)} Dateien…")
    for xml_path in tqdm(xml_files, unit="file"):
        process_file(xml_path, client)
if __name__ == "__main__":
    main()
