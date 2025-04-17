# ==== document_schemas.py ====
from .document_schemas import BaseDocument, Person, Place, Event, Organization

# ==== assigned_roles_module.py ====
from .Assigned_Roles_Module import assign_roles_to_known_persons

# ==== type_matcher.py ====
from .type_matcher import get_document_type

# ==== person_matcher.py ====
from .person_matcher import (
    match_person, KNOWN_PERSONS, deduplicate_persons,
    normalize_name, load_known_persons_from_csv,get_best_match_info, fuzzy_match_name, 
)
import inspect
print(">>> match_person loaded from:", inspect.getfile(match_person))

# === Organization_matcher.py ====
from .organization_matcher import load_organizations_from_csv, match_organization_from_text

# ==== place_matcher.py ====
from .place_matcher import PlaceMatcher

# ==== validation_module.py ====
from .validation_module import validate_extended, generate_validation_summary

# ==== llm_enricher.py ====
from . import llm_enricher


__all__ = [
    # document_schemas
    "BaseDocument", "Person", "Place", "Event", "Organization",

    # person_matcher
    "match_person", "KNOWN_PERSONS", "deduplicate_persons",
    "normalize_name", "load_known_persons_from_csv", "get_best_match_info", "fuzzy_match_name",

    # type_matcher
    "get_document_type",

    # assigned_roles_module
    "assign_roles_to_known_persons",

    # validation_module
    "validate_extended", "generate_validation_summary",

    # organization_matcher
    "load_organizations_from_csv", "match_organization_from_text",

    # llm_enricher
    "llm_enricher"
]