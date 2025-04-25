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
Du bekommst ein vollständiges XML aus einem historischen Transkriptionsworkflow.
Deine Aufgabe ist es, in jedem <TextLine> Element das darinstehende <Unicode> –
also den reinen Text – zu analysieren und alle Personen, Organisationen, Orte,
Daten (mit standardized when:DD.MM.YYYY) und Rollen zu finden.

Füge dann jedem <TextLine>-Tag ein Attribut

  custom="person {{offset:X; length:Y;}} organization {{offset:X; length:Y;}} place {{offset:X; length:Y;}} date {{offset:X; length:Y; when:DD.MM.YYYY;}} role {{offset:X; length:Y;}}"

ein, das alle Treffer im Unicode-Text auflistet. Liefere das komplette XML
zurück, unverändert außer deinen custom-Attributen.

FORMAT-BEISPIEL:
<TextLine id="r1_l5"
  custom="person {{offset:0; length:12;}} role {{offset:13; length:10;}}">
  <TextEquiv><Unicode>Herr Müller Vortragender</Unicode></TextEquiv>
</TextLine>

Hier ist das zu annotierende XML:
\"\"\"
{xml_content}
\"\"\"
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
            xml_files.extend(sorted(page_dir.glob("*.xml")))

    print(f"Starte LLM-Annotation für {len(xml_files)} Dateien…")
    for xml_path in tqdm(xml_files, unit="file"):
        process_file(xml_path, client)

if __name__ == "__main__":
    main()
