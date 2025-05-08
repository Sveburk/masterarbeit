# File: Module/__init__.py




# --- Enrich Pipeline ---
from .enrich_pipeline import enrich_pipeline

# --- Document Schemas ---
from .document_schemas import BaseDocument, Person, Place, Event, Organization

# --- Person Matcher ---
from .person_matcher import (
    load_known_persons_from_csv,
    KNOWN_PERSONS,
    get_matching_thresholds,
    normalize_name_string,
    normalize_name,
    fuzzy_match_name,
    match_person,
    deduplicate_persons,
    get_best_match_info,
    extract_person_data,
    split_and_enrich_persons,
)

# --- Letter‑Metadata Matcher ---
from .letter_metadata_matcher import match_authors, match_recipients, resolve_llm_custom_authors_recipients

# --- Organization Matcher ---
from .organization_matcher import (
    load_organizations_from_csv,
    match_organization_from_text,
    match_organization_entities,
)

# --- Type Matcher ---
from .type_matcher import get_document_type

# --- Assigned Roles ---
from .Assigned_Roles_Module import ROLE_MAPPINGS_DE,KNOWN_ROLE_LIST, NAME_RE,assign_roles_to_known_persons,extract_standalone_roles,map_role_to_schema_entry, extract_role_in_token, process_text

# --- Place Matcher ---
from .place_matcher import PlaceMatcher, mentioned_places_from_custom_data

# --- Validation ---
from .validation_module import validate_extended, generate_validation_summary

# --- LLM Enricher ---
from .llm_enricher import run_enrichment_on_directory





__all__ = [

    # Pipeline
    "enrich_pipeline",

    # Schemas
    "BaseDocument", "Person", "Place", "Event", "Organization",

    # Person Matcher
    "load_known_persons_from_csv",
    "KNOWN_PERSONS",
    "get_matching_thresholds",
    "normalize_name_string",
    "normalize_name",
    "fuzzy_match_name",
    "match_person",
    "deduplicate_persons",
    "get_best_match_info",
    "extract_person_data",
    "split_and_enrich_persons",

    # Letter‑Metadata Matcher
    "match_authors",
    "match_recipients",
    "resolve_llm_custom_authors_recipients",
    

    # Organization Matcher
    "load_organizations_from_csv",
    "match_organization_from_text",
    "match_organization_entities",

    # Type Matcher
    "get_document_type",

    # Assigned Roles
    "assign_roles_to_known_persons",
    "extract_standalone_roles",
    "map_role_to_schema_entry",
    "extract_role_in_token",
    "process_text",
    

    # Place Matcher
    "PlaceMatcher", "mentioned_places_from_custom_data",

    # Validation
    "validate_extended", "generate_validation_summary",

    # LLM Enricher
    "run_enrichment_on_directory",
]
