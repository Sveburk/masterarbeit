import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from Module.place_matcher import PlaceMatcher
from Module.date_matcher import extract_custom_date
from Module.organization_matcher import match_organization_from_text, KNOWN_ORGS
from Module.person_matcher import match_person, KNOWN_PERSONS
from Module.document_schemas import Event, Place, Organization, Person
from Module.Assigned_Roles_Module import NAME_RE

def extract_name_with_spacy(name: str) -> (str, str):
    parts = name.strip().split()
    if len(parts) >= 2:
        return " ".join(parts[:-1]), parts[-1]
    return parts[0], ""

def extract_events_from_xml(xml_path: str, place_matcher: PlaceMatcher) -> List[Event]:
    def build_event(block: List[str], line_obj) -> Event:
        full_text = "\n".join(block).strip()
        print(f"[DEBUG] FINAL EVENT TEXT:\n{full_text}\n----")

        # Orte extrahieren
        places = []
        for token in full_text.split():
            token_clean = token.strip(".,;:-()\"").lower()
            if len(token_clean) < 3:
                continue
            if token_clean in {"an", "am", "mit", "und", "des", "der", "die", "dem", "den", "vom", "zum", "beim"}:
                continue
            if token_clean[0].islower():
                continue
            match = place_matcher.match_place(token)
            if match:
                places.append(Place(
                    name=match["data"].get("name", ""),
                    type="",
                    alternate_place_name=match["data"].get("alternate_place_name", ""),
                    geonames_id=match["data"].get("geonames_id", ""),
                    wikidata_id=match["data"].get("wikidata_id", ""),
                    nodegoat_id=match["data"].get("nodegoat_id", "")
                ))

        # Datum (aus erster Zeile bzw. Blockquelle)
        dates = extract_custom_date(line_obj, namespace)

        # Organisationen
        org_matches = match_organization_from_text(full_text, KNOWN_ORGS)
        organizations = [Organization.from_dict(o) for o in org_matches]

        # Personen
        persons = []
        name_candidates = NAME_RE.findall(full_text)
        for name in name_candidates:
            forename, familyname = extract_name_with_spacy(name)
            if forename or familyname:
                match, score = match_person(
                    {"forename": forename, "familyname": familyname},
                    KNOWN_PERSONS
                )
                if match:
                    persons.append(Person.from_dict({
                        **match,
                        "match_score": score,
                        "confidence": "from_event_text"
                    }))

        return Event(
            name=full_text.split("\n")[0].strip(),
            description=full_text,
            location=", ".join([p.name for p in places]),
            date=dates[0]["date"] if dates else "",
            involved_places=places,
            involved_organizations=organizations,
            involved_persons=persons,
            dates=dates
        )

    def is_continuation(prev_line: str, current_line: str, xml_line: ET.Element) -> bool:
        if prev_line.endswith("-"):
            return True
        if current_line and current_line[0].islower():
            return True
        if not extract_custom_date(xml_line, namespace):
            return True
        continuation_keywords = {"des", "der", "am", "vom", "zum", "beim"}
        if current_line.strip().split()[0].lower() in continuation_keywords:
            return True
        return False

    tree = ET.parse(xml_path)
    root = tree.getroot()
    namespace = {'ns': root.tag.split('}')[0].strip('{')}
    events: List[Event] = []

    buffer: List[str] = []
    line_obj = None

    for text_line in root.findall('.//ns:TextLine', namespace):
        custom_attr = text_line.attrib.get('custom', '')
        unicode_el = text_line.find('./ns:TextEquiv/ns:Unicode', namespace)
        line_text = unicode_el.text.strip() if unicode_el is not None and unicode_el.text else ""

        is_event_line = "event" in custom_attr.lower()

        if is_event_line:
            if not buffer:
                buffer.append(line_text)
                line_obj = text_line
            else:
                prev = buffer[-1]
                if is_continuation(prev, line_text, text_line):
                    buffer.append(line_text)
                else:
                    events.append(build_event(buffer, line_obj))
                    buffer = [line_text]
                    line_obj = text_line
        else:
            if buffer:
                events.append(build_event(buffer, line_obj))
                buffer = []
                line_obj = None

    if buffer:
        events.append(build_event(buffer, line_obj))

    return events
