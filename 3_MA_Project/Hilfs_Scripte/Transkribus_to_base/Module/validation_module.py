import re
from typing import Dict, List
from .document_schemas import BaseDocument, Person, Place


def validate_extended(doc: BaseDocument) -> Dict[str, List[str]]:
    errors = {}

    # Pflichtfelder prüfen
    if not doc.creation_date or not re.match(
        r"^\d{4}\.\d{2}\.\d{2}$", doc.creation_date
    ):
        errors.setdefault("creation_date", []).append(
            "Fehlend oder ungültiges Format (YYYY.MM.DD)"
        )

    if not doc.creation_place.strip():
        errors.setdefault("creation_place", []).append(
            "Fehlender Entstehungsort"
        )

    if not doc.document_type:
        errors.setdefault("document_type", []).append(
            "Kein Dokumenttyp angegeben"
        )

    # Strukturprüfungen
    if doc.document_type in ["Brief", "Postkarte"]:
        if not any(p.is_valid() for p in doc.recipients):
            errors.setdefault("recipients", []).append(
                "Empfänger fehlt oder ist ungültig"
            )
    if not doc.mentioned_places:
        errors.setdefault("mentioned_places", []).append(
            "Keine Orte angegeben"
        )

    for i, place in enumerate(doc.mentioned_places):
        if not place.geonames_id:
            errors.setdefault(f"mentioned_places[{i}]", []).append(
                "Geonames-ID fehlt"
            )
        if not place.nodegoat_id:
            errors.setdefault(f"mentioned_places[{i}]", []).append(
                "Nodegoat-ID fehlt"
            )

    for i, person in enumerate(doc.mentioned_persons):
        if person.forename.lower() in [
            "des",
            "herrn",
            "frau",
            "vereinsführer",
        ] or person.familyname.lower() in ["des", "herrn"]:
            errors.setdefault(f"mentioned_persons[{i}]", []).append(
                f"Möglicher Fehlname: {person.forename} {person.familyname}"
            )

    return errors


from collections import Counter


def generate_validation_summary(validation_error_list):
    total = len(validation_error_list)
    with_errors = sum(1 for e in validation_error_list if e["errors"])
    without_errors = total - with_errors

    print("\nValidierungsübersicht:")
    print(f"- {total} Dateien verarbeitet")
    print(f"- {without_errors} ohne Fehler")
    print(f"- {with_errors} mit Fehlern")

    error_counter = Counter()
    # for entry in validation_error_list:
    #     for field, messages in entry["errors"].items():
    #         for msg in messages:
    #             if "recipients" in field:
    #                 error_counter["recipients fehlt"] += 1
    #             elif "creation_date" in field:
    #                 error_counter["creation_date ungültig"] += 1
    #             elif "geonames_id" in field:
    #                 error_counter["geonames_id fehlt"] += 1
    #             elif "nodegoat_id" in field:
    #                 error_counter["nodegoat_id fehlt"] += 1
    #             elif "creation_place" in field:
    #                 error_counter["creation_place fehlt"] += 1
    #             elif "mentioned_persons" in field and "Möglicher Fehlname" in msg:
    #                 error_counter["person möglicherweise falsch"] += 1
    #             else:
    #                 error_counter[msg] += 1

    if error_counter:
        print("- Häufigste Fehler:")
        for msg, count in error_counter.most_common(10):
            print(f"  - {msg}: {count}×")


# ==== Beispiel-Testfälle ====
# if __name__ == "__main__":
#     doc = BaseDocument(
#         object_type="Dokument",
#         attributes={},
#         content_transcription="Test",
#         mentioned_persons=[
#             Person(forename="des", familyname=""),
#             Person(forename="Otto", familyname="Bollinger")
#         ],
#         mentioned_organizations=[],
#         mentioned_places=[
#             Place(name="Murg", geonames_id="", nodegoat_id="", type=""),
#             Place(name="Laufenburg", geonames_id="6555918", nodegoat_id="ng123", type="")
#         ],
#         mentioned_dates=["1941.05.28"],
#         content_tags_in_german=[],
#         author=Person(),
#         recipients=Person(),
#         creation_date="1941.5.28",  # falsch formatiert
#         creation_place="",
#         document_type="Brief",
#         document_format=""
#     )

#     errors = validate_extended(doc)
#     for field, issues in errors.items():
#         for issue in issues:
#             print(f"[{field}] {issue}")
