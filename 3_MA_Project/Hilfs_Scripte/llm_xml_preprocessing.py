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
TRANSKRIBUS_DIR = Path("/Users/svenburkhardt/Desktop/Transkribus_test_In")

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
Du bist ein spezialisiertes XML-Annotationstool für historische Transkribus-Dokumente.

Deine Aufgabe:

1. Lies das komplette XML-Dokument ein und analysiere den darin enthaltenen Unicode-Text aller <TextLine>-Elemente.

2. **Globale Personenerkennung**  
   a) Durchsuche das gesamte Dokument nach Personennamen (inkl. Vornamen, Nachnamen, ggf. Titeln).  
   b) Merke Dir jede gefundene Person einmalig mit Offset und Länge im jeweiligen <TextLine>-Unicode-Text.  
   c) Verwende für alle späteren Referenzen dieselben Offsets.

3. **Empfänger-Suche (recipient)**  
   a) Identifiziere im **Kopfbereich** (alles bis zur ersten Leerzeile) eine mögliche Anrede wie  
      „Herr“, „Frau“, „Sehr geehrter Herr …“ oder vergleichbare Ausdrücke.  
   b) Ordne die gefundene Anrede der passenden Person aus Schritt 2 zu.  
   c) Annotiere sie als `recipient {{offset:X; length:Y;}}` im jeweiligen <TextLine>.

4. **Autor-Suche (author)**  
   a) Identifiziere im **Fußbereich** (alles nach der letzten Grußformel wie „Mit freundlichen Grüßen“, „Heil Hitler“, etc.)  
      eine oder mehrere Personen, die als Verfasser auftreten (z. B. „Max Mustermann, Vereinsführer“).  
   b) Annotiere den Namen als `author {{offset:X; length:Y;}}`.  
   c) Falls im Text eine Funktionsbezeichnung (z. B. „Chorleiter“) vorkommt, annotiere diese zusätzlich als separate `role {{offset:X; length:Y;}}`.

5. **Annotation pro TextLine**  
   Füge jedem <TextLine> (sofern zutreffend) **ein einziges** Attribut `custom="..."` hinzu, das alle erkannten Entitäten in dieser festen Reihenfolge enthält:
   
    person {{offset:X; length:Y;}}  
    recipient {{offset:X; length:Y;}}  
    author {{offset:X; length:Y;}}  
    organization {{offset:X; length:Y;}}  
    place {{offset:X; length:Y;}}  
    date {{offset:X; length:Y; when:TT.MM.JJJJ;}}  
    role {{offset:X; length:Y;}}

– Füge nur die Entitäten hinzu, die **tatsächlich** vorkommen (keine leeren Platzhalter).  
– Gib `date` immer im Format `TT.MM.JJJJ` mit dem Zusatz `when:` an.

6. **Behalte alle anderen Inhalte des XML unverändert bei.**  
Du darfst ausschließlich `custom="..."`-Attribute in <TextLine>-Tags verändern oder hinzufügen.

7. **Antwortformat:** Gib ausschließlich das vollständige, gültige und annotierte XML zurück – **ohne** erklärenden Text oder Markdown-Formatierung.

**Beispiel**  
<!-- Kopfzeile mit Empfänger und Ort -->
<TextLine id="tl_header"
          custom="recipient {{offset:17; length:13;}} place {{offset:32; length:10;}}">
  <Unicode>Sehr geehrter Herr Müller, Murg/Baden</Unicode>
</TextLine>

<!-- Organisation -->
<TextLine id="tl_org"
          custom="organization {{offset:0; length:15;}}">
  <Unicode>Männerchor Murg</Unicode>
</TextLine>

<!-- Datum -->
<TextLine id="tl_date"
          custom="date {{offset:0; length:10; when:01.09.1939;}}">
  <Unicode>01.09.1939</Unicode>
</TextLine>

<!-- Grußformel mit Autor -->
<TextLine id="tl_footer"
          custom="author {{offset:29; length:15;}} role {{offset:46; length:10;}}">
  <Unicode>Mit freundlichen Grüßen Max Mustermann, Chorleiter</Unicode>
</TextLine>

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
        xml_text = xml_path.read_text(encoding="utf-8")
        print(f"\nVerarbeite Datei: {xml_path.name}")
        print("  → Starte Annotation via OpenAI…", end="", flush=True)

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
                    print(f"Überspringe Ordner {page_dir}, "
                          f"{len(preproc)}/{len(all_xmls)} Dateien sind bereits vorverarbeitet ({ratio:.0%}).")
                    continue

            xml_files.extend(sorted(page_dir.glob("*.xml")))

    print(f"Starte LLM-Annotation für {len(xml_files)} Dateien…")
    for xml_path in tqdm(xml_files, unit="file"):
        process_file(xml_path, client)
if __name__ == "__main__":
    main()
