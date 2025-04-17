import os
import json
import time
import csv
from typing import List, Dict

# Conditional import for openai
try:
    import openai
except ImportError:
    openai = None

# Kosten-Konstanten (GPT-4 Turbo, Stand 2024)
INPUT_COST_PER_1K = 0.01  # USD
OUTPUT_COST_PER_1K = 0.03  # USD

def enrich_document_with_llm(json_data: dict, client: any, model="gpt-4", temperature=0.0) -> Dict:
    prompt = f""" 
    Temperatur: 0,4  
    Du bekommst ein vollständiges JSON-Dokument aus einem historischen Transkriptionsworkflow.  
    Deine Aufgabe ist es, folgende Felder **zu ergänzen oder zu korrigieren**, **wenn sie erkennbar sind**:

    - `author` → Wer hat den Text verfasst? Suche nach Grußformeln wie "Deine...", "Mit freundlichen Grüßen..." usw.  
    - `recipient` → An wen ist der Text gerichtet? Analysiere das Adressfeld und Anrede.  
    - `creation_date` → Nutze Datumsangaben im Text oder Datumsformat wie „28.V.1941“.  
    - `creation_place` → Oft steht der Ort vor dem Datum, z. B. „München, 28. Mai 1941“.  
         Falls Adressen erwähnt sind, extrahiere sie und gib sie als strukturierte Felder zurück:\n
         author_address und recipient_address, inklusive Straße, Hausnummer, Postleitzahl, Ort, ggf. Zimmer/Stube/Militärinfo (wie Einheiten,Felpostnummern/FPN).\n
    - `content_tags_in_german` → Themen oder Gefühle im Text, z. B. Liebe, Krieg, Trauer, Hoffnung etc.  
    - `mentioned_persons`, `mentioned_organizations`, `mentioned_places` → Wenn nötig, **Dubletten entfernen**, falsch erkannte Personen (wie „des“) aussortieren.

    ⚠️ Besondere Regeln:
    - **„Laufenburg (Baden) Rhina“** oder ähnliche Kombinationen sind **in der Regel ein Ortsname** und sollen als solcher unter `mentioned_places` geführt werden.
    - **„Männerchor Murg“** oder ähnliche Begriffe sind **in der Regel eine Organisation**, meist ein Verein, und sollen unter `mentioned_organizations` erfasst werden – **nicht als Ort**. Murg kann aber in diesem Fall als location annotiert werden.
    ⚠️ Niemals verändern oder ergänzen:
    - ID-Felder (wie `geonames_id`, `wikidata_id`, `nodegoat_id` in `mentioned_places`)
    - Diese stammen aus einer externen Datenbank und dürfen nur übernommen, aber **nicht verändert** werden.

    Wenn ein Feld **nicht eindeutig bestimmbar ist**, verwende `""`.

    Gib das vollständige JSON inklusive aller Felder zurück, so wie im Original – aber angereichert mit deinen Ergänzungen.
 
    hier ist das Dokument
    {json.dumps(json_data, ensure_ascii=False, indent=2)}
    """
    

    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        messages=[{"role": "user", "content": prompt}]
    )

    output = response.choices[0].message.content
    input_tokens = response.usage.prompt_tokens
    output_tokens = response.usage.completion_tokens

    try:
        enriched_data = json.loads(output)
    except Exception as e:
        print("Fehler beim Parsen der LLM-Antwort:", e)
        enriched_data = json_data.copy()
        enriched_data["llm_metadata"] = {
            "error": f"Parsing error: {str(e)}",
            "raw_llm_output": output[:500]  # optional: erste 500 Zeichen speichern
        }

    enriched_data["llm_metadata"] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round((input_tokens / 1000 * INPUT_COST_PER_1K) + (output_tokens / 1000 * OUTPUT_COST_PER_1K), 4),
        "model": model
    }

    return enriched_data


def load_json_files(input_dir: str) -> List[str]:
    return [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith(".json") and not f.endswith("_enriched.json")
    ]

def save_enriched_json(original_path: str, enriched_data: dict):
    dir_name = os.path.dirname(original_path)
    base_name = os.path.basename(original_path).replace(".json", "_enriched.json")
    output_path = os.path.join(dir_name, base_name)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched_data, f, ensure_ascii=False, indent=2)

    print(f"Enriched JSON gespeichert: {output_path}")

def log_enrichment(csv_path: str, file: str, input_tokens: int, output_tokens: int, cost_usd: float):
    exists = os.path.exists(csv_path)
    with open(csv_path, mode="a", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        if not exists:
            writer.writerow(["filename", "input_tokens", "output_tokens", "cost_usd"])
        writer.writerow([file, input_tokens, output_tokens, f"{cost_usd:.4f}"])

def run_enrichment_on_directory(input_dir: str, api_key: str, model="gpt-4"):
    if openai is None:
        print("OpenAI library not installed. Skipping enrichment.")
        return
        
    client = openai.OpenAI(api_key=api_key)
    json_files = load_json_files(input_dir)

    total_in, total_out, total_cost = 0, 0, 0.0
    print(f"Starte LLM-Enrichment für {len(json_files)} JSON-Dateien...")

    csv_log_path = os.path.join(input_dir, "llm_enrichment_log.csv")

    for path in json_files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 🛡️ Original-IDs sichern
        original_place_ids = [
            (p.get("name", ""), p.get("nodegoat_id", ""), p.get("geonames_id", ""), p.get("wikidata_id", ""))
            for p in data.get("mentioned_places", [])
        ]
        original_person_ids = [
            (p.get("forename", ""), p.get("familyname", ""), p.get("nodegoat_id", ""))
            for p in data.get("mentioned_persons", [])
        ]

        # Prüfe, ob relevante Felder fehlen (IDs dürfen ruhig fehlen)
        missing_fields = any([
            not data.get("recipient"),
            not data.get("author"),
            not data.get("creation_date"),
            not data.get("content_tags_in_german")
        ])

        # ✨ Immer durchlaufen, aber nur bei fehlenden Feldern API-Aufruf starten
        if missing_fields:
            enriched = enrich_document_with_llm(data, client, model=model)

            # 🔒 Stelle sicher, dass keine IDs überschrieben wurden
            for p in enriched.get("mentioned_places", []):
                for name, node_id, geo_id, wiki_id in original_place_ids:
                    if p.get("name") == name:
                        p["nodegoat_id"] = node_id
                        p["geonames_id"] = geo_id
                        p["wikidata_id"] = wiki_id

            for p in enriched.get("mentioned_persons", []):
                for fn, ln, node_id in original_person_ids:
                    if p.get("forename") == fn and p.get("familyname") == ln:
                        p["nodegoat_id"] = node_id

            save_enriched_json(path, enriched)

            meta = enriched.get("llm_metadata", {})
            input_tokens = meta.get("input_tokens", 0)
            output_tokens = meta.get("output_tokens", 0)
            cost = meta.get("cost_usd", 0.0)

            log_enrichment(csv_log_path, os.path.basename(path), input_tokens, output_tokens, cost)

            total_in += input_tokens
            total_out += output_tokens
            total_cost += cost
        else:
            print(f"[SKIP] {os.path.basename(path)} hat bereits alle relevanten Felder.")

    print("\n--- Zusammenfassung ---")
    print(f"Input-Tokens: {total_in}")
    print(f"Output-Tokens: {total_out}")
    print(f"Geschätzte Kosten: ${round(total_cost, 4)}")
