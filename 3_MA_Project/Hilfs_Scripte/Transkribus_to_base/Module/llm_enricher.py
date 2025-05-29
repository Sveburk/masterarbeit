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

    - `author` → Wer hat den Text verfasst? Suche nach Grußformeln wie "Deine...", "Mit freundlichen Grüßen..." usw.  Wenn du das ausfüllst, ergänze in confidence	"OpenAI"

    - `recipient` → An wen ist der Text gerichtet? Analysiere das Adressfeld und Anrede. Wenn du das ausfüllst, ergänze in confidence	"OpenAI"
    - `creation_date` → Nutze Datumsangaben im Text oder Datumsformat wie „28.V.1941“.  
    - `creation_place` → Oft steht der Ort vor dem Datum, z. B. „München, 28. Mai 1941“.  
         Falls Adressen erwähnt sind, extrahiere sie und gib sie als strukturierte Felder zurück:\n
         author_address und recipient_address, inklusive Straße, Hausnummer, Postleitzahl, Ort, ggf. Zimmer/Stube/Militärinfo (wie Einheiten,Felpostnummern/FPN).\n
    - `content_tags_in_german` → Themen oder Gefühle im Text, z. B. Liebe, Krieg, Trauer, Hoffnung etc.  
    - `mentioned_persons`, `mentioned_organizations`, `mentioned_places` → Wenn nötig, **Dubletten entfernen**, falsch erkannte Personen (wie „des“) aussortieren.
    - `mentioned_persons` → erkenne, ob in dem content_transcription eine rolle für die Person genannt wird und ergänze sie

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

def merge_lists(orig_list: List[dict], enriched_list: List[dict], key_fields: List[str]) -> List[dict]:
    result = orig_list.copy()
    for item in enriched_list:
        # prüfe, ob item nach key_fields schon existiert
        if not any(all(o.get(k) == item.get(k) for k in key_fields) for o in orig_list):
            result.append(item)
    return result

def merge_original_and_enriched(orig: dict, enriched: dict) -> dict:
    merged = orig.copy()
    for k, v in enriched.items():
        if k not in merged or merged.get(k) in (None, "", [], {}):
            # fehlt ganz oder leer: übernehmen
            merged[k] = v
        else:
            # Spezialfall Listen: union
            if isinstance(v, list) and isinstance(merged.get(k), list):
                if k == "mentioned_places":
                    merged[k] = merge_lists(merged[k], v, key_fields=["name","nodegoat_id"])
                elif k == "mentioned_persons":
                    merged[k] = merge_lists(merged[k], v, key_fields=["forename","familyname","nodegoat_id"])
                elif k == "mentioned_organizations":
                    merged[k] = merge_lists(merged[k], v, key_fields=["name","nodegoat_id"])
                else:
                    # für alle anderen Listen einfach ersetzen, wenn orig leer
                    pass
            # alle anderen Felder belassen wir, weil orig nicht leer war
    return merged

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
    csv_log_path = os.path.join(input_dir, "llm_enrichment_log.csv")

    print(f"Starte LLM-Enrichment für {len(json_files)} Dateien…")
    for path in json_files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # nur, wenn wir wirklich etwas ergänzen müssen
        missing = any([
            not data.get("recipient"),
            not data.get("author"),
            not data.get("creation_date"),
            not data.get("content_tags_in_german")
        ])
        if not missing:
            print(f"[SKIP] {os.path.basename(path)}: alles bereits vorhanden.")
            continue

        enriched = enrich_document_with_llm(data, client, model=model)

        # IDs sichern und nach Merge wieder einsetzen
        place_ids = { (p["name"], p["nodegoat_id"]) for p in data.get("mentioned_places", []) }
        person_ids = { (p["forename"],p["familyname"],p["nodegoat_id"]) for p in data.get("mentioned_persons", []) }

        merged = merge_original_and_enriched(data, enriched)

        # IDs zurückschreiben
        for p in merged.get("mentioned_places", []):
            key = (p.get("name"), p.get("nodegoat_id"))
            if key not in place_ids:
                # neu ergänzte behalten wie vom LLM
                continue
            # nichts zu tun, existierende sind schon korrekt

        for p in merged.get("mentioned_persons", []):
            key = (p.get("forename"),p.get("familyname"),p.get("nodegoat_id"))
            if key not in person_ids:
                continue

        save_enriched_json(path, merged)

        meta = merged.get("llm_metadata", {})
        i_tok = meta.get("input_tokens", 0)
        o_tok = meta.get("output_tokens", 0)
        cost = meta.get("cost_usd", 0.0)
        log_enrichment(csv_log_path, os.path.basename(path), i_tok, o_tok, cost)

        total_in += i_tok
        total_out += o_tok
        total_cost += cost

    print("\n--- Zusammenfassung ---")
    print(f"Input-Tokens: {total_in}")
    print(f"Output-Tokens: {total_out}")
    print(f"Geschätzte Kosten: ${round(total_cost, 4)}")
