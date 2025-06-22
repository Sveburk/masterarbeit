import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from Module.place_matcher import PlaceMatcher
from Module.date_matcher import extract_custom_date
from Module.organization_matcher import match_organization_from_text, KNOWN_ORGS
from Module.person_matcher import match_person, KNOWN_PERSONS
from Module.document_schemas import Event, Place, Organization, Person
from Module.Assigned_Roles_Module import NAME_RE
import re

def extract_name_with_spacy(name: str) -> (str, str):
    parts = name.strip().split()
    if len(parts) >= 2:
        return " ".join(parts[:-1]), parts[-1]
    return parts[0], ""

def extract_events_from_xml(xml_path: str, place_matcher: PlaceMatcher) -> List[Event]:
    def build_event(block: List[str], line_obj, namespaces: Dict[str,str]) -> Optional[Event]:
        full_text = "\n".join(block).strip()
        date_token_re = re.compile(r'^\d{1,2}\.\d{1,2}\.?$')        #reges für mögliche Daten
        print(f"[DEBUG] FINAL EVENT TEXT:\n{full_text}\n----")

        # Regex für einfache Datumstokens wie "15.3", "15.03" oder "15.03."
        date_token_re = re.compile(r'^\d{1,2}\.\d{1,2}\.?$')
        date_tokens: List[str] = []

        # Orte extrahieren
        places = []
        for token in full_text.split():
            token_clean = token.strip(".,;:-()\"").lower()
            if date_token_re.match(token_clean):
                date_tokens.append(token_clean)
                continue
            if len(token_clean) < 3:
                continue
            if token_clean in {"an", "am", "mit", "und", "des", "der", "die", "dem", "den", "vom", "zum", "beim"}:
                continue
            if token_clean[0].islower():
                continue

            match_list = place_matcher.match_place(token)
            # ───── Schutz gegen leere Trefferliste ─────
            if not match_list:
                # kein Treffer für dieses Token → nächstes Token
                continue
            # ───────────────────────────────────────────

            # Nimm besten Treffer
            # Ortstreffer holen und in Liste zwingen
            match_list = place_matcher.match_place(token)
            # kein Treffer → überspringen
            if not match_list:
                continue
            # Falls kein Listentyp (z.B. einzelnes Dict), in Liste packen
            if not isinstance(match_list, (list, tuple)):
                match_list = [match_list]
            # Nimm jetzt den ersten Treffer
            best_match = match_list[0]
            places.append(Place(
                name=best_match["data"].get("name", ""),
                type="",
                alternate_place_name=best_match["data"].get("alternate_place_name", ""),
                geonames_id=best_match["data"].get("geonames_id", ""),
                wikidata_id=best_match["data"].get("wikidata_id", ""),
                nodegoat_id=best_match["data"].get("nodegoat_id", "")
            ))
        if not places:
            return None

        # Datum (aus erster Zeile bzw. Blockquelle)
        dates = extract_custom_date(line_obj, namespaces)
        # Falls kein Datum extrahiert wurde, aber ein Token wie "15.3." gefunden,
        # dann dieses als Event-Datum verwenden
        if not dates and date_tokens:
            dates = [{"date": date_tokens[0]}]

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

    def is_continuation(prev_line: str, current_line: str, xml_line: ET.Element, namespaces: Dict[str,str]) -> bool:
        if prev_line.endswith("-"):
            return True
        if current_line and current_line[0].islower():
            return True
        if not extract_custom_date(xml_line, namespaces):
            return True
        continuation_keywords = {"des", "der", "am", "vom", "zum", "beim"}
        if current_line.strip().split()[0].lower() in continuation_keywords:
            return True
        return False

    namespaces = {}
    for event, elem in ET.iterparse(xml_path, events=['start-ns']):
        prefix, uri = elem
        # lege Standard-Namespace auf 'ns', falls prefix == ''
        namespaces[prefix or 'ns'] = uri

    tree = ET.parse(xml_path)
    root = tree.getroot()
    events: List[Event] = []

    buffer: List[str] = []
    line_obj = None

    for text_line in root.findall('.//ns:TextLine', namespaces):
        custom_attr = text_line.attrib.get('custom', '')
        unicode_el = text_line.find('./ns:TextEquiv/ns:Unicode', namespaces)
        line_text = unicode_el.text.strip() if unicode_el is not None and unicode_el.text else ""

        is_event_line = "event" in custom_attr.lower()

        if buffer:
            evt = None
            # Prüfe, ob die aktuelle Zeile zur vorherigen passt (z. B. Fortsetzung)
            if is_event_line or is_continuation(buffer[-1], line_text, text_line, namespaces):
                buffer.append(line_text)
            else:
                # Bisherigen Block abschließen
                evt = build_event(buffer, line_obj, namespaces)
            if evt is not None:
                events.append(evt)
                buffer = [line_text] if is_event_line else []
                line_obj = text_line if is_event_line else None
        else:
            if is_event_line:
                buffer = [line_text]
                line_obj = text_line

    if buffer:
        # Letzten Block abschließen
        last_evt = build_event(buffer, line_obj, namespaces)
        if last_evt is not None:
            events.append(last_evt)
    return events
