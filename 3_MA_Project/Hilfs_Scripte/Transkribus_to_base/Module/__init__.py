# ==== document_schemas.py ====
from .document_schemas import BaseDocument, Person, Place, Event, Organization

# ==== assigned_roles_module.py ====
from .Assigned_Roles_Module import assign_roles_to_known_persons

# ==== type_matcher.py ====
from .type_matcher import get_document_type

# ==== person_matcher.py ====
from .person_matcher import (
    match_person, KNOWN_PERSONS, deduplicate_persons,
    normalize_name, normalize_name_with_title,
    fuzzy_match_name, load_known_persons_from_csv,
)
# ==== place_matcher.py ====
from .place_matcher import PlaceMatcher


# ==== validation_module.py ====
from .validation_module import validate_extended, generate_validation_summary


__all__ = [
    # document_schemas
    "BaseDocument", "Person", "Place", "Event", "Organization",

    # person_matcher
    "match_person", "KNOWN_PERSONS", "deduplicate_persons",
    "normalize_name", "normalize_name_with_title",
    "fuzzy_match_name", "load_known_persons_from_csv",

    # type_matcher
    "get_document_type",

    # assigned_roles_module
    "assign_roles_to_known_persons",

    #validation_module
    "validate_extended", "generate_validation_summary"
]
