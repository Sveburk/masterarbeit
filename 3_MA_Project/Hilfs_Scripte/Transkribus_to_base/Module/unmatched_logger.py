from typing import Dict, List, Any
from pathlib import Path
import json
from Module.document_schemas import Place

def log_unmatched_entities(document_id: str,
                           custom_data: Dict[str, List[Dict[str, Any]]],
                           final_persons: List[Dict[str, Any]],
                           final_places: List[Place],
                           final_roles: List[Dict[str, Any]],
                           unmatched_path: Path):
    """
    Führt ein zentrales unmatched.json nach Namen als Schlüssel.
    Jeder Name wird nur einmal gespeichert, mit Liste aller Akten, in denen er unmatched auftrat.
    """
    def person_name(person: Dict[str, Any]) -> str:
        """Generiert einen konsistenten Namen (für Schlüsselzwecke)."""
        fn = person.get("forename", "").strip()
        ln = person.get("familyname", "").strip()
        full = f"{fn} {ln}".strip()
        return full if full else fn or ln or person.get("role", "")
    def is_in_final(person: Dict[str, Any]) -> bool:
        fn = person.get("forename", "").strip()
        ln = person.get("familyname", "").strip()
        return any(
            p.get("forename", "").strip() == fn and p.get("familyname", "").strip() == ln
            for p in final_persons
        )


    def is_dropped(name, pool, key="name"):
        return not any(name == (getattr(p, key, None) if not isinstance(p, dict) else p.get(key)) for p in pool)

    def add_or_update_entry(entry_dict, name: str, tag: str, reason: str):
        if name not in entry_dict:
            entry_dict[name] = {
                "tag": tag,
                "akten": [document_id],
                "grund": reason
            }
        else:
            if document_id not in entry_dict[name]["akten"]:
                entry_dict[name]["akten"].append(document_id)

    # Vorhandene Datei laden
    unmatched_path.parent.mkdir(parents=True, exist_ok=True)
    if unmatched_path.exists():
        with open(unmatched_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                # Alte Struktur gefunden: konvertieren (vorsichtiger Fallback)
                unmatched_data = {}
                for entry in loaded:
                    name = entry.get("Name")
                    if not name:
                        continue
                    tag = entry.get("Tag", "")
                    grund = entry.get("Grund", "")
                    akte = entry.get("Akte", "")
                    unmatched_data[name] = {
                        "tag": tag,
                        "akten": [akte] if akte else [],
                        "grund": grund
                    }
            else:
                unmatched_data = loaded  # korrektes dict
    else:
        unmatched_data = {}

    for place in custom_data.get("places", []):
        name = place.get("name")
        if name and is_dropped(name, final_places):
            add_or_update_entry(unmatched_data, name, "place", "nicht in mentioned_places aufgenommen")

    # Rollen
    for role in final_roles:
        raw = role.get("raw", "").strip()
        if not raw:
            continue
        matched = any(
            raw.lower() in (p.get("role", "").lower())
            for p in final_persons
        )
        if not matched:
            add_or_update_entry(unmatched_data, raw, "role", "nicht in Personenrolle übernommen")

    # Speichern
    with open(unmatched_path, "w", encoding="utf-8") as f:
        json.dump(unmatched_data, f, ensure_ascii=False, indent=2)

    print(f"[UNMATCHED] Unmatched-Log aktualisiert: {len(unmatched_data)} Einträge.")

def log_unmatched_person(person_data: dict, document_id: str, reason: str, tag: str = "person"):
    from pathlib import Path
    import os, json

    unmatched_path = Path("unmatched/unmatched_persons.json")
    unmatched_path.parent.mkdir(parents=True, exist_ok=True)

    name = (
        person_data.get("raw_token") or
        f"{person_data.get('forename', '').strip()} {person_data.get('familyname', '').strip()}"
    ).strip()

    if not name:
        name = person_data.get("role", "") or "Unbekannt"

    if unmatched_path.exists():
        with open(unmatched_path, "r", encoding="utf-8") as f:
            unmatched_data = json.load(f)
    else:
        unmatched_data = {}

    if name not in unmatched_data:
        unmatched_data[name] = {
            "tag": tag,
            "akten": [document_id],
            "grund": reason
        }
    else:
        if document_id not in unmatched_data[name]["akten"]:
            unmatched_data[name]["akten"].append(document_id)

    with open(unmatched_path, "w", encoding="utf-8") as f:
        json.dump(unmatched_data, f, ensure_ascii=False, indent=2)

    print(f"[UNMATCHED] Einzelperson geloggt: {name} ({reason})")
