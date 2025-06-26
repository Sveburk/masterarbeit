from typing import Dict, List, Any
from pathlib import Path
import json
import unicodedata
import re
from Module.document_schemas import Place


def normalize_key(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s]", "", text)
    return text.lower().strip()


def load_json(path: Path) -> Dict[str, Any]:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                # Fallback: Liste in dict umwandeln (kompatibel zur alten Struktur)
                converted = {}
                for entry in loaded:
                    name = entry.get("original") or entry.get("Name")
                    if not name:
                        continue
                    key = normalize_key(name)
                    converted[key] = {
                        "original": name,
                        "tag": entry.get("tag", entry.get("Tag", "")),
                        "akten": entry.get("akten", [entry.get("Akte", "")]),
                        "grund": entry.get("grund", entry.get("Grund", ""))
                    }
                return converted
            return loaded  # war schon dict
    return {}


def save_json(data: Dict[str, Any], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def log_unmatched_entities(
    document_id: str,
    custom_data: Dict[str, List[Dict[str, Any]]],
    final_persons: List[Dict[str, Any]],
    final_places: List[Place],
    final_roles: List[Dict[str, Any]],
    unmatched_dir: Path
):
    """
    Legt deduplizierte unmatched-Logs an:
    - unmatched_persons.json
    - unmatched_places.json
    - unmatched_roles.json
    - unmatched_organisations.json
    - unmatched_events.json
    """
    unmatched_dir.mkdir(parents=True, exist_ok=True)

    target_file_map = {
        "person": "unmatched_person.json",
        "place": "unmatched_place.json",
        "role": "unmatched_role.json",
        "organisation": "unmatched_organisation.json",
        "event": "unmatched_event.json"
    }

    def log_entry(entity: Dict[str, Any], key_text: str, tag: str, reason: str, target_filename: str):

        key = normalize_key(key_text)
        if not key or tag not in target_file_map:
            return
        path = unmatched_dir / target_filename
        data = load_json(path)
        if key not in data:
            data[key] = {
                "original": key_text.strip(),
                "tag": tag,
                "akten": [document_id],
                "grund": reason
            }
        else:
            if document_id not in data[key]["akten"]:
                data[key]["akten"].append(document_id)
        save_json(data, path)

    # Personen
    for raw in custom_data.get("persons", []):
        fn = raw.get("forename", "").strip()
        ln = raw.get("familyname", "").strip()
        name = f"{fn} {ln}".strip() or raw.get("role", "") or raw.get("raw_token", "")
        if not any(
            fn == p.get("forename", "") and ln == p.get("familyname", "")
            for p in final_persons
        ):
            log_entry(raw, name, "person", "nicht in mentioned_persons übernommen", "unmatched_person.json")



    # Orte
    for place in custom_data.get("places", []):
        name = place.get("name", "").strip()
        if name and not any(name == (p.name if isinstance(p, Place) else p.get("name", "")) for p in final_places):
            log_entry(place, name, "place", "nicht in mentioned_places übernommen", "unmatched_places.json")

    # Rollen
    for role in final_roles:
        raw = role.get("raw", "").strip()
        # definiere 'name' hier als Fallback auf raw
        name = raw
        # wenn es eine Rolle gibt und sie nicht in final_persons landet
        if raw and not any(raw.lower() == p.get("role", "").lower() for p in final_persons):
            log_entry(
                role,
                name,
                "role",
                "nicht in Personenrolle übernommen",
                "unmatched_role.json"   # <— passt zum target_file_map
            )


    # Organisationen
    for org in custom_data.get("organisations", []):
        name = org.get("name", "").strip()
        if name:
            log_entry(org, name, "organisation", "nicht als associated_organisation übernommen", "unmatched_organisations.json")

    # Events
    for evt in custom_data.get("events", []):
        name = evt.get("name", "").strip()
        if name:
            log_entry(evt, name, "event", "nicht in mentioned_events übernommen", "unmatched_events.json")

    print("[UNMATCHED] Alle unmatched-Einträge wurden protokolliert.")
