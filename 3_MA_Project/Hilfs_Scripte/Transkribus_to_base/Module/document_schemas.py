"""
Schemas für die Strukturierung der Dokumentdaten aus dem Männerchor Murg Korpus.

Dieses Modul definiert die Basis-Schema-Klassen für verschiedene Dokumenttypen,
die im Projekt verwendet werden. Es unterstützt die Datenvalidierung und -strukturierung
für die Weiterverarbeitung der extrahierten Informationen.
"""

import json
from typing import Dict, List, Optional, Union, Any
import re
from datetime import datetime

# Im Person-Constructor in document_schemas.py
class Person:
    def __init__(
        self,
        anrede: str = "",
        forename: str = "",
        alternate_name: str = "",
        familyname: str = "",
        title: str = "",
        role: str = "",
        associated_place: str = "",
        associated_organisation: str = "",
        nodegoat_id: str = "",
        match_score: Optional[float] = None,
        confidence: str = ""
    ):
        # alle Parameter auch zuweisen!
        self.anrede = anrede
        self.forename = forename
        self.alternate_name = alternate_name
        self.familyname = familyname
        self.title = title
        self.role = role
        self.associated_place = associated_place
        self.associated_organisation = associated_organisation
        self.nodegoat_id = nodegoat_id
        self.match_score = match_score
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anrede": self.anrede,
            "forename": self.forename,
            "alternate_name": self.alternate_name,
            "familyname": self.familyname,
            "title": self.title,
            "role": self.role,
            "associated_place": self.associated_place,
            "associated_organisation": self.associated_organisation,
            "nodegoat_id": self.nodegoat_id,
            "match_score": self.match_score,
            "confidence": self.confidence
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Person':
        # nur eine from_dict, die *alle* Felder übernimmt
        return cls(
            anrede=data.get("anrede", ""),
            forename=data.get("forename", ""),
            alternate_name=data.get("alternate_name", ""),
            familyname=data.get("familyname", ""),
            title=data.get("title", ""),
            role=data.get("role", ""),
            associated_place=data.get("associated_place", ""),
            associated_organisation=data.get("associated_organisation", ""),
            nodegoat_id=data.get("nodegoat_id", ""),
            match_score=data.get("match_score"),
            confidence=data.get("confidence", "")
        )

    def is_valid(self) -> bool:
        """Mindestens Vor- oder Nachname muss vorhanden sein."""
        return bool(self.forename.strip() or self.familyname.strip())

    def __init__(
        self,
        anrede: str = "",
        forename: str = "",
        alternate_name: str = "",
        familyname: str = "",
        title: str = "",
        role: str = "",
        associated_place: str = "",
        associated_organisation: str = "",
        nodegoat_id: str = "",
        match_score: Optional[float] = None,
        confidence: str = ""
    ):
        # alle Parameter auch zuweisen!
        self.anrede = anrede
        self.forename = forename
        self.alternate_name = alternate_name
        self.familyname = familyname
        self.title = title
        self.role = role
        self.associated_place = associated_place
        self.associated_organisation = associated_organisation
        self.nodegoat_id = nodegoat_id
        self.match_score = match_score
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anrede": self.anrede,
            "forename": self.forename,
            "alternate_name": self.alternate_name,
            "familyname": self.familyname,
            "title": self.title,
            "role": self.role,
            "associated_place": self.associated_place,
            "associated_organisation": self.associated_organisation,
            "nodegoat_id": self.nodegoat_id,
            "match_score": self.match_score,
            "confidence": self.confidence
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Person':
        # nur eine from_dict, die *alle* Felder übernimmt
        return cls(
            anrede=data.get("anrede", ""),
            forename=data.get("forename", ""),
            alternate_name=data.get("alternate_name", ""),
            familyname=data.get("familyname", ""),
            title=data.get("title", ""),
            role=data.get("role", ""),
            associated_place=data.get("associated_place", ""),
            associated_organisation=data.get("associated_organisation", ""),
            nodegoat_id=data.get("nodegoat_id", ""),
            match_score=data.get("match_score"),
            confidence=data.get("confidence", "")
        )

    def is_valid(self) -> bool:
        """Mindestens Vor- oder Nachname muss vorhanden sein."""
        return bool(self.forename.strip() or self.familyname.strip())

    def __init__(
        self,
        anrede: str = "",
        forename: str = "",
        alternate_name: str = "",
        familyname: str = "",
        title: str = "",
        role: str = "",
        associated_place: str = "",
        associated_organisation: str = "",
        nodegoat_id: str = "",
        match_score: Optional[float] = None,
        confidence: str = ""
    ):
        # alle Parameter auch zuweisen!
        self.anrede = anrede
        self.forename = forename
        self.alternate_name = alternate_name
        self.familyname = familyname
        self.title = title
        self.role = role
        self.associated_place = associated_place
        self.associated_organisation = associated_organisation
        self.nodegoat_id = nodegoat_id
        self.match_score = match_score
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anrede": self.anrede,
            "forename": self.forename,
            "alternate_name": self.alternate_name,
            "familyname": self.familyname,
            "title": self.title,
            "role": self.role,
            "associated_place": self.associated_place,
            "associated_organisation": self.associated_organisation,
            "nodegoat_id": self.nodegoat_id,
            "match_score": self.match_score,
            "confidence": self.confidence
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Person':
        # nur eine from_dict, die *alle* Felder übernimmt
        return cls(
            anrede=data.get("anrede", ""),
            forename=data.get("forename", ""),
            alternate_name=data.get("alternate_name", ""),
            familyname=data.get("familyname", ""),
            title=data.get("title", ""),
            role=data.get("role", ""),
            associated_place=data.get("associated_place", ""),
            associated_organisation=data.get("associated_organisation", ""),
            nodegoat_id=data.get("nodegoat_id", ""),
            match_score=data.get("match_score"),
            confidence=data.get("confidence", "")
        )

    def is_valid(self) -> bool:
        """Mindestens Vor- oder Nachname muss vorhanden sein."""
        return bool(self.forename.strip() or self.familyname.strip())

class Organization:
    """Repräsentiert eine Organisation, angereichert mit Nodegoat-ID, Alternativnamen, Match-Score und Confidence."""

    def __init__(
        self,
        name: str = "",
        type: str = "",
        nodegoat_id: str = "",
        alternate_names: List[str] = None,
        feldpostnummer: str = "",
        match_score: Optional[float] = None,
        confidence: str = ""
    ):
        self.name = name
        self.type = type
        self.nodegoat_id = nodegoat_id
        self.alternate_names = alternate_names or []
        self.feldpostnummer = feldpostnummer
        self.match_score = match_score
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "name": self.name,
            "type": self.type,
            "nodegoat_id": self.nodegoat_id,
            "alternate_names": self.alternate_names,
            "feldpostnummer": self.feldpostnummer
        }
        if self.match_score is not None:
            d["match_score"] = self.match_score
        if self.confidence:
            d["confidence"] = self.confidence
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Organization':
        return cls(
            name=data.get("name", ""),
            type=data.get("type", ""),
            nodegoat_id=data.get("nodegoat_id", ""),
            alternate_names=data.get("alternate_names", []),
            feldpostnummer=data.get("feldpostnummer", ""),
            match_score=data.get("match_score"),
            confidence=data.get("confidence", "")
        )

    def is_valid(self) -> bool:
        """Prüft, ob die Organisationsdaten gültig sind."""
        return bool(self.name.strip())

class Place:
    """Repräsentiert einen Ort."""
    
    def __init__(self, name: str = "", type: str = "",
                 alternate_place_name: str = "", geonames_id: str = "", wikidata_id: str = "", nodegoat_id: str = ""):
        self.name = name
        self.type = type
        self.alternate_place_name = alternate_place_name
        self.geonames_id = geonames_id
        self.wikidata_id = wikidata_id
        self.nodegoat_id = nodegoat_id
    
    def to_dict(self) -> Dict[str, str]:
        """Konvertiert Ortsobjekt in ein Dictionary."""
        return {
            "name": self.name,
            "type": self.type,
            "alternate_place_name": self.alternate_place_name,
            "geonames_id": self.geonames_id,
            "wikidata_id": self.wikidata_id,
            "nodegoat_id": self.nodegoat_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'Place':
        """Erstellt ein Ortsobjekt aus einem Dictionary."""
        return cls(
            name=data.get("name", ""),
            type=data.get("type", ""),
            alternate_place_name=data.get("alternate_place_name", data.get("alternate_name", "")), 
            geonames_id=data.get("geonames_id", ""),
            wikidata_id=data.get("wikidata_id", ""),
            nodegoat_id=data.get("nodegoat_id", "")
        )
    
    def is_valid(self) -> bool:
        """Prüft, ob die Ortsdaten gültig sind."""
        return bool(self.name.strip())

class Event:
    """Repräsentiert ein Ereignis."""
    
    def __init__(self, name: str = "", date: str = "", location: str = "", description: str = ""):
        self.name = name
        self.date = date
        self.location = location
        self.description = description
    
    def to_dict(self) -> Dict[str, str]:
        """Konvertiert Ereignisobjekt in ein Dictionary."""
        return {
            "name": self.name,
            "date": self.date,
            "location": self.location,
            "description": self.description
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'Event':
        """Erstellt ein Ereignisobjekt aus einem Dictionary."""
        return cls(
            name=data.get("name", ""),
            date=data.get("date", ""),
            location=data.get("location", ""),
            description=data.get("description", "")
        )
    
    def is_valid(self) -> bool:
        """Prüft, ob die Ereignisdaten gültig sind."""
        return bool(self.name.strip())

class BaseDocument:
    """Basis-Dokumentenklasse, die gemeinsame Attribute für alle Dokumente definiert."""
    
    # Listen der gültigen Dokumenttypen und -formate
    VALID_DOCUMENT_TYPES = [
        "Brief", "Protokoll", "Postkarte", "Rechnung",
        "Regierungsdokument", "Karte", "Noten", "Zeitungsartikel",
        "Liste", "Website", "Notizzettel", "Offerte"
    ]
    
    VALID_DOCUMENT_FORMATS = ["Handschrift", "Maschinell", "mitUnterschrift", "Bild"]
    
    def __init__(self, 
                 object_type: str = "Dokument",
                 attributes: Dict[str, str] = None,
                 author: Union[Person, Dict[str, str]] = None,
                 recipient: Union[Person, Dict[str, str]] = None,
                 mentioned_persons: List[Union[Person, Dict[str, str]]] = None,
                 mentioned_organizations: List[Union[Organization, Dict[str, str]]] = None,
                 mentioned_events: List[Union[Event, Dict[str, str]]] = None,
                 creation_date: str = "",
                 creation_place: str = "",
                 mentioned_dates: List[str] = None,
                 mentioned_places: List[Union[Place, Dict[str, str]]] = None,
                 content_tags_in_german: List[str] = None,
                 content_transcription: str = "",
                 document_type: str = "",
                 document_format: str = ""):
        
        self.object_type = object_type
        self.attributes = attributes or {}
        
        # Konvertieren der Person-Objekte oder Dictionaries
        self.author = Person.from_dict(author) if isinstance(author, dict) else author or Person()
        self.recipient = Person.from_dict(recipient) if isinstance(recipient, dict) else recipient or Person()
        
        # Konvertieren der Listen von Objekten oder Dictionaries
        self.mentioned_persons = []
        if mentioned_persons:
            for person in mentioned_persons:
                if isinstance(person, dict):
                    self.mentioned_persons.append(Person.from_dict(person))
                else:
                    self.mentioned_persons.append(person)
        
        self.mentioned_organizations = []
        if mentioned_organizations:
            for org in mentioned_organizations:
                if isinstance(org, dict):
                    self.mentioned_organizations.append(Organization.from_dict(org))
                else:
                    self.mentioned_organizations.append(org)
        
        self.mentioned_events = []
        if mentioned_events:
            for event in mentioned_events:
                if isinstance(event, dict):
                    self.mentioned_events.append(Event.from_dict(event))
                else:
                    self.mentioned_events.append(event)
        
        self.mentioned_places = []
        if mentioned_places:
            for place in mentioned_places:
                if isinstance(place, dict):
                    self.mentioned_places.append(Place.from_dict(place))
                else:
                    self.mentioned_places.append(place)
        
        self.creation_date = creation_date
        self.creation_place = creation_place
        self.mentioned_dates = mentioned_dates or []
        self.content_tags_in_german = content_tags_in_german or []
        self.content_transcription = content_transcription
        self.document_type = document_type
        self.document_format = document_format
    
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert das Dokument in ein Dictionary."""
        return {
            "object_type": self.object_type,
            "attributes": self.attributes,
            "author": self.author.to_dict(),
            "recipient": self.recipient.to_dict(),
            "mentioned_persons": [person.to_dict() for person in self.mentioned_persons],
            "mentioned_organizations": [org.to_dict() for org in self.mentioned_organizations],
            "mentioned_events": [event.to_dict() for event in self.mentioned_events],
            "creation_date": self.creation_date,
            "creation_place": self.creation_place,
            "mentioned_dates": self.mentioned_dates,
            "mentioned_places": [place.to_dict() for place in self.mentioned_places],
            "content_tags_in_german": self.content_tags_in_german,
            "content_transcription": self.content_transcription,
            "document_type": self.document_type,
            "document_format": self.document_format
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseDocument':
        """Erstellt ein Dokumentobjekt aus einem Dictionary."""
        return cls(
            object_type=data.get("object_type", "Dokument"),
            attributes=data.get("attributes", {}),
            author=data.get("author", {}),
            recipient=data.get("recipient", {}),
            mentioned_persons=data.get("mentioned_persons", []),
            mentioned_organizations=data.get("mentioned_organizations", []),
            mentioned_events=data.get("mentioned_events", []),
            creation_date=data.get("creation_date", ""),
            creation_place=data.get("creation_place", ""),
            mentioned_dates=data.get("mentioned_dates", []),
            mentioned_places=data.get("mentioned_places", []),
            content_tags_in_german=data.get("content_tags_in_german", []),
            content_transcription=data.get("content_transcription", ""),
            document_type=data.get("document_type", ""),
            document_format=data.get("document_format", "")
        )
    
    def to_json(self, indent: int = 4) -> str:
        """Konvertiert das Dokument in einen JSON-String."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BaseDocument':
        """Erstellt ein Dokumentobjekt aus einem JSON-String."""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def validate(self) -> Dict[str, List[str]]:
        """
        Validiert das Dokument und gibt Fehler zurück.
        
        Returns:
            Dict[str, List[str]]: Dictionary mit Feldnamen als Schlüssel 
                                 und Listen von Fehlermeldungen als Werte
        """
        errors = {}
        
        # Validiere Dokumenttyp
        if self.document_type and self.document_type not in self.VALID_DOCUMENT_TYPES:
            errors["document_type"] = [f"Ungültiger Dokumenttyp: {self.document_type}. Muss einer der folgenden sein: {', '.join(self.VALID_DOCUMENT_TYPES)}"]
        
        # Validiere Dokumentformat
        if self.document_format and self.document_format not in self.VALID_DOCUMENT_FORMATS:
            errors["document_format"] = [f"Ungültiges Dokumentformat: {self.document_format}. Muss einer der folgenden sein: {', '.join(self.VALID_DOCUMENT_FORMATS)}"]
        
        # Validiere Datum (falls vorhanden)
        if self.creation_date and not self._is_valid_date(self.creation_date):
            errors["creation_date"] = [f"Ungültiges Datum: {self.creation_date}. Format sollte YYYY.MM.DD sein"]
        
        # Validiere erwähnte Daten
        for i, date in enumerate(self.mentioned_dates):
            if not self._is_valid_date(date):
                if "mentioned_dates" not in errors:
                    errors["mentioned_dates"] = []
                errors["mentioned_dates"].append(f"Ungültiges Datum an Index {i}: {date}. Format sollte YYYY.MM.DD sein")
        
        # Validiere Empfänger für Briefe und Postkarten
        if not self.recipient.is_valid() and self.document_type in ["Brief", "Postkarte"]:
            errors["recipient"] = ["Empfänger muss für Briefe und Postkarten angegeben werden"]
        
        # Validiere erwähnte Personen
        for i, person in enumerate(self.mentioned_persons):
            if not person.is_valid():
                if "mentioned_persons" not in errors:
                    errors["mentioned_persons"] = []
                errors["mentioned_persons"].append(f"Ungültige Person an Index {i}: muss mindestens Vor- oder Nachnamen haben")
        
        # Validiere erwähnte Organisationen
        for i, org in enumerate(self.mentioned_organizations):
            if not org.is_valid():
                if "mentioned_organizations" not in errors:
                    errors["mentioned_organizations"] = []
                errors["mentioned_organizations"].append(f"Ungültige Organisation an Index {i}: muss einen Namen haben")
        
        # Validiere erwähnte Ereignisse
        for i, event in enumerate(self.mentioned_events):
            if not event.is_valid():
                if "mentioned_events" not in errors:
                    errors["mentioned_events"] = []
                errors["mentioned_events"].append(f"Ungültiges Ereignis an Index {i}: muss einen Namen haben")
        
        # Validiere erwähnte Orte
        for i, place in enumerate(self.mentioned_places):
            if not place.is_valid():
                if "mentioned_places" not in errors:
                    errors["mentioned_places"] = []
                errors["mentioned_places"].append(f"Ungültiger Ort an Index {i}: muss einen Namen haben")
        
        return errors
    
    def is_valid(self) -> bool:
        """Prüft, ob das Dokument gültig ist."""
        return len(self.validate()) == 0
    
    def _is_valid_date(self, date_str: str) -> bool:
        """
        Prüft, ob ein Datum im Format YYYY.MM.DD gültig ist.
        Akzeptiert auch Teilangaben wie YYYY oder YYYY.MM.
        """
        if not date_str:
            return True  # Leere Daten sind erlaubt
        
        # Prüfe Datumsformate: YYYY.MM.DD, YYYY.MM, YYYY
        patterns = [
            r"^\d{4}\.\d{2}\.\d{2}$",  # YYYY.MM.DD
            r"^\d{4}\.\d{2}$",          # YYYY.MM
            r"^\d{4}$"                  # YYYY
        ]
        
        # Wenn eines der Muster passt, ist das Format korrekt
        for pattern in patterns:
            if re.match(pattern, date_str):
                return True
        
        return False


# Beispiele für spezifische Dokumenttypen (können erweitert werden)

class Brief(BaseDocument):
    """Repräsentiert einen Brief mit zusätzlichen brief-spezifischen Attributen."""
    
    def __init__(self, greeting: str = "", closing: str = "", **kwargs):
        super().__init__(**kwargs)
        self.document_type = "Brief"
        self.greeting = greeting
        self.closing = closing
    
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert den Brief in ein Dictionary."""
        data = super().to_dict()
        data["greeting"] = self.greeting
        data["closing"] = self.closing
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Brief':
        """Erstellt ein Brief-Objekt aus einem Dictionary."""
        brief = cls(
            greeting=data.get("greeting", ""),
            closing=data.get("closing", "")
        )
        for key, value in data.items():
            if key not in ["greeting", "closing"]:
                setattr(brief, key, value)
        return brief

class Postkarte(BaseDocument):
    """Repräsentiert eine Postkarte mit zusätzlichen postkarten-spezifischen Attributen."""
    
    def __init__(self, postmark: str = "", postmark_date: str = "", **kwargs):
        super().__init__(**kwargs)
        self.document_type = "Postkarte"
        self.postmark = postmark
        self.postmark_date = postmark_date
    
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert die Postkarte in ein Dictionary."""
        data = super().to_dict()
        data["postmark"] = self.postmark
        data["postmark_date"] = self.postmark_date
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Postkarte':
        """Erstellt ein Postkarte-Objekt aus einem Dictionary."""
        postkarte = cls(
            postmark=data.get("postmark", ""),
            postmark_date=data.get("postmark_date", "")
        )
        for key, value in data.items():
            if key not in ["postmark", "postmark_date"]:
                setattr(postkarte, key, value)
        return postkarte

class Protokoll(BaseDocument):
    """Repräsentiert ein Protokoll mit zusätzlichen protokoll-spezifischen Attributen."""
    
    def __init__(self, meeting_type: str = "", attendees: List[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.document_type = "Protokoll"
        self.meeting_type = meeting_type
        self.attendees = attendees or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert das Protokoll in ein Dictionary."""
        data = super().to_dict()
        data["meeting_type"] = self.meeting_type
        data["attendees"] = self.attendees
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Protokoll':
        """Erstellt ein Protokoll-Objekt aus einem Dictionary."""
        protokoll = cls(
            meeting_type=data.get("meeting_type", ""),
            attendees=data.get("attendees", [])
        )
        for key, value in data.items():
            if key not in ["meeting_type", "attendees"]:
                setattr(protokoll, key, value)
        return protokoll

# Fabrik-Funktion, um das richtige Dokumentobjekt basierend auf dem Typ zu erstellen
def create_document(data: Dict[str, Any]) -> BaseDocument:
    """
    Erstellt das passende Dokumentobjekt basierend auf dem Dokumenttyp im Dictionary.
    
    Args:
        data: Dictionary mit Dokumentdaten
        
    Returns:
        Ein spezialisiertes Dokumentobjekt oder ein BaseDocument-Objekt
    """
    document_type = data.get("document_type", "")
    
    if document_type == "Brief":
        return Brief.from_dict(data)
    elif document_type == "Postkarte":
        return Postkarte.from_dict(data)
    elif document_type == "Protokoll":
        return Protokoll.from_dict(data)
    else:
        return BaseDocument.from_dict(data)