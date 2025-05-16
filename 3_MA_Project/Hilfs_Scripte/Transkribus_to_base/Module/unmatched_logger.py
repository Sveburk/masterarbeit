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
    Schreibt ein zentrales JSON namens "unmatched.json" mit allen gedroppten Personen, Orten und Rollen,
    die einen relevanten XML-Tag haben, aber nicht in die finale JSON-Struktur übernommen wurden.
    """
    def is_dropped(name, pool, key="name"):
        return not any(name == (getattr(p, key, None) if not isinstance(p, dict) else p.get(key)) for p in pool)

    unmatched_entries = []

    # Personen prüfen (nur wenn ein name enthalten ist)
    for person in custom_data.get("persons", []):
        fn, ln = person.get("forename", ""), person.get("familyname", "")
        name = f"{fn} {ln}".strip() if fn or ln else ""
        if not name:
            continue

        is_unmatched = all(
            not (p.get("forename") == fn and p.get("familyname") == ln)
            for p in final_persons
        )
        if is_unmatched:
            unmatched_entries.append({
                "Akte": document_id,
                "Name": name,
                "Context": "",
                "Tag": "person",
                "Grund": "nicht in mentioned_persons aufgenommen"
            })

    # Orte prüfen
    for place in custom_data.get("places", []):
        name = place.get("name")
        if not name:
            continue
        if is_dropped(name, final_places):
            unmatched_entries.append({
                "Akte": document_id,
                "Name": name,
                "Context": "",
                "Tag": "place",
                "Grund": "nicht in mentioned_places aufgenommen"
            })

    # Rollen prüfen
    for role in final_roles:
        raw = role.get("raw")
        if not raw:
            continue
        role_matched = False
        for p in final_persons:
            role_in_p = p.get("role", "")
            if raw.lower() in role_in_p.lower():
                role_matched = True
                break
        if not role_matched:
            unmatched_entries.append({
                "Akte": document_id,
                "Name": raw,
                "Context": "",
                "Tag": "role",
                "Grund": "nicht in Personenrolle übernommen"
            })

    # Schreibe Datei
    if unmatched_entries:
        unmatched_path.parent.mkdir(parents=True, exist_ok=True)
        if unmatched_path.exists():
            with open(unmatched_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        else:
            existing = []

        combined = existing + unmatched_entries

        with open(unmatched_path, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2)

        print(f"[UNMATCHED] {len(unmatched_entries)} neue Einträge in unmatched.json ergänzt")