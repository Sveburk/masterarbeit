# File: Module/__init__.py


# Event-Matcher
from Module.event_matcher import extract_events_from_xml


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
    deduplicate_and_group_persons, 
    count_mentions_in_transcript_contextual

)

# --- Letter‑Metadata Matcher ---
from .letter_metadata_matcher import (
    match_authors,
    match_recipients,
    extract_multiple_recipients_raw,
    resolve_llm_custom_authors_recipients,
    extract_authors_recipients_from_mentions,
    ensure_author_recipient_in_mentions,
    postprocess_roles,
    enrich_final_recipients,
    deduplicate_recipients,
    assign_sender_and_recipient_place,
    finalize_recipient_places,
    )

# --- Organization Matcher ---
from .organization_matcher import (
    load_organizations_from_csv,
    match_organization_from_text,
    match_organization_entities,
    match_organization_by_name,
)

# --- Type Matcher ---
from .type_matcher import get_document_type

# --- Assigned Roles ---
from .Assigned_Roles_Module import(
    ROLE_MAPPINGS_DE,
    KNOWN_ROLE_LIST,
    NAME_RE,
    assign_roles_to_known_persons,
    extract_standalone_roles,
    map_role_to_schema_entry, 
    extract_role_in_token, 
    process_text, 
    flatten_organisation_entry,
    )

# --- Place Matcher ---
from .place_matcher import(
    PlaceMatcher, 
    mentioned_places_from_custom_data, 
    extract_place_lines_from_xml)

# --- Validation ---
from .validation_module import validate_extended, generate_validation_summary

# --- LLM Enricher ---
from .llm_enricher import run_enrichment_on_directory

#--- unmatched-logger ---
from Module.unmatched_logger import (log_unmatched_entities)

# --- Date_matcher ---
from Module.date_matcher import (
    extract_date_from_custom,combine_dates,extract_custom_date)





__all__ = [
    #event-matcher
    "extract_events_from_xml",

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
    "deduplicate_and_group_persons", 
    "count_mentions_in_transcript_contextual",

    # Letter‑Metadata Matcher
    "match_authors",
    "match_recipients",
    "extract_multiple_recipients_raw",
    "resolve_llm_custom_authors_recipients",
    "extract_authors_recipients_from_mentions",
    "ensure_author_recipient_in_mentions",
    "postprocess_roles",
    "enrich_final_recipients",
    "deduplicate_recipients",
    "assign_sender_and_recipient_place",
    "finalize_recipient_places",
    "flatten_organisation_entry",   
    

    # Organization Matcher
    "load_organizations_from_csv",
    "match_organization_from_text",
    "match_organization_entities",
    "match_organization_by_name",
    

    # Type Matcher
    "get_document_type",

    # Assigned Roles
    "assign_roles_to_known_persons",
    "extract_standalone_roles",
    "map_role_to_schema_entry",
    "extract_role_in_token",
    "process_text",

    # Place Matcher
    "PlaceMatcher", 
    "mentioned_places_from_custom_data",
    "extract_place_lines_from_xml",

    # Validation
    "validate_extended", "generate_validation_summary",

    # LLM Enricher
    "run_enrichment_on_directory",

    #unmatched-logger
    "log_unmatched_entities",

    #Date_matcher
    "combine_dates",
    "extract_date_from_custom",
    "extract_custom_date"
]
