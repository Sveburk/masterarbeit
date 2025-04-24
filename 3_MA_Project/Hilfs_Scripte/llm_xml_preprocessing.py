#!/usr/bin/env python3
import os
import openai
from pathlib import Path
from tqdm import tqdm

# --- Kosten-Konstanten (GPT-4 Turbo, Stand 2024) ---
INPUT_COST_PER_1K = 0.01   # USD per 1K input tokens
OUTPUT_COST_PER_1K = 0.03  # USD per 1K output tokens

# --- Konfiguration: Pfade anpassen falls nötig ---
TRANSKRIBUS_DIR = Path("/Users/svenburkhardt/Desktop/Transkribus_test_In")
OUTPUT_DIR      = TRANSKRIBUS_DIR / "preprocessed"

# --- Hilfsfunktionen ---
def get_api_client() -> openai.OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Kein OPENAI_API_KEY in der Umgebung gefunden.")
    return openai.OpenAI(api_key=api_key)

def annotate_with_llm(xml_content: str,
                      client: openai.OpenAI,
                      model: str = "gpt-4",
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
    """Annotiert eine einzelne XML und speichert sie im OUTPUT_DIR."""
    try:
        xml_text = xml_path.read_text(encoding="utf-8")
        print(f"\nVerarbeite Datei: {xml_path.name}")
        print("  → Starte Annotation via OpenAI…", end="", flush=True)

        result    = annotate_with_llm(xml_text, client)
        annotated = result["annotated_xml"]
        meta      = result["llm_metadata"]

        print(" ✓ zurück, speichere…")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / f"{xml_path.stem}_enhanced_xml.xml"
        out_path.write_text(annotated, encoding="utf-8")

        print(f"    → Gespeichert unter: {out_path}")
        print(f"    • Model: {meta['model']}")
        print(f"    • Input-Tokens: {meta['input_tokens']}, Output-Tokens: {meta['output_tokens']}")
        print(f"    • Geschätzte Kosten: ${meta['cost_usd']}\n")

    except Exception as e:
        print(f"  ✗ Fehler bei {xml_path.name}: {e}")

# --- Hauptlogik ---
def get_transkribus():
    client = get_api_client()
    # jede siebenstellige Ordnernummer durchlaufen
    for seven in sorted(os.listdir(TRANSKRIBUS_DIR)):
        if not seven.isdigit():
            continue
        folder = TRANSKRIBUS_DIR / seven

        # nur Unterordner, die mit "Akte_" beginnen
        for sub in sorted(folder.iterdir()):
            if not sub.is_dir() or not sub.name.startswith("Akte_"):
                continue

            page_dir = sub / "page"
            if not page_dir.is_dir():
                continue

            # jede XML-Datei in /page/
            for xml_file in sorted(page_dir.iterdir()):
                if xml_file.suffix.lower() != ".xml":
                    continue
                process_file(xml_file, client)

def main():
    if not os.getenv("OPENAI_API_KEY"):
        print("Fehler: Kein OPENAI_API_KEY gesetzt.")
        return
    get_transkribus()

if __name__ == "__main__":
    main()
