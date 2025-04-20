"""
Module: Module/enrich_pipeline.py

Core enrichment functions decoupled from specific matcher classes.
"""

def merge_entity(base: dict, enrich: dict, overwrite: set = ()) -> dict:
    """
    Merges enrich into base dict:
    - Keys in overwrite always replaced.
    - Otherwise only set if base has no value (None, empty string, empty list/dict).
    """
    result = base.copy()
    for k, v in enrich.items():
        if k in overwrite:
            result[k] = v
        else:
            old = result.get(k)
            if old in (None, "", [], {}):
                result[k] = v
    return result


def enrich_pipeline(
    doc_entities,
    match_person_fn,
    match_org_fn,    # expects (name, org_list)
    org_list,        # <-- neuer Parameter
    get_type_fn,
    assign_roles_fn,
    place_matcher):
    """
    Applies enrichment steps on each entity dict in doc_entities.
    Steps:
      1) Person matching via match_person_fn
      2) Organization matching via match_org_fn
      3) Type detection via get_type_fn
      4) Role assignment via assign_roles_fn
      5) Place matching via place_matcher.match_place
    Returns a new list of merged dicts.
    """
    enriched_list = []
    for ent in doc_entities:
        merged = ent.copy()
        # 1) Person matching
        # assume ent contains person fields or 'text'
        person_input = extract_person_data(ent) if 'forename' not in ent else ent
        match, score = match_person_fn(person_input)
        if match and score >= 70:
            pm = {
                "forename": match.get("forename", ""),
                "familyname": match.get("familyname", ""),
                "nodegoat_id": match.get("id", "")
            }
        else:
            pm = {}
        merged = merge_entity(merged, pm)

        # 2) Organization matching
        org_info = match_org_fn(merged.get("text", merged.get("name", "")),
                                org_list) or {}
        merged = merge_entity(merged, org_info)

        # 3) Type detection
        type_val = get_type_fn(merged.get("id", ""), merged.get("page", ""))
        merged = merge_entity(merged, {"type": type_val})

        # 4) Role assignment
        roles_list = assign_roles_fn([merged], merged.get("text", ""))
        if roles_list:
            merged = merge_entity(merged, roles_list[0])

        # 5) Place matching
        places = merged.get("associated_places", [])
        merged["associated_places"] = [place_matcher.match_place(p) for p in places]

        enriched_list.append(merged)
    return enriched_list
