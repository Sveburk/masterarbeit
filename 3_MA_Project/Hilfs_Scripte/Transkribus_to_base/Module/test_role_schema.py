#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Einfache Tests zur Überprüfung der role_schema-Funktion
"""

import sys
from pathlib import Path

# Füge das Modul-Verzeichnis zum Pfad hinzu
script_dir = Path(__file__).parent
sys.path.append(str(script_dir.parent))

# Importiere die benötigten Module
from Module.document_schemas import Person
from Module.Assigned_Roles_Module import normalize_and_match_role, map_role_to_schema_entry


def test_person_with_role_schema():
    """Test der Person-Klasse mit role_schema Parameter"""
    
    print("=== Test: Person mit role_schema ===")
    
    # Test 1: Person mit role aber ohne role_schema erstellen
    person1 = Person(
        forename="Max",
        familyname="Mustermann",
        role="Malermeister"
    )
    print(f"Person1 erstellt: {person1.forename} {person1.familyname}, Rolle: {person1.role}")
    print(f"Person1.role_schema initial: {person1.role_schema}")
    
    # Test 2: Person mit role und role_schema erstellen
    person2 = Person(
        forename="Maria",
        familyname="Musterfrau",
        role="Schriftführerin",
        role_schema="SCHRIFTFÜHRER"
    )
    print(f"Person2 erstellt: {person2.forename} {person2.familyname}, Rolle: {person2.role}")
    print(f"Person2.role_schema initial: {person2.role_schema}")
    
    # Test 3: Verhalten der to_dict-Methode
    print("\n=== Test: to_dict() ===")
    dict1 = person1.to_dict()
    print(f"Person1 to_dict(): role_schema = {dict1.get('role_schema')}")
    
    dict2 = person2.to_dict()
    print(f"Person2 to_dict(): role_schema = {dict2.get('role_schema')}")
    
    # Test 4: from_dict mit und ohne role_schema
    print("\n=== Test: from_dict() ===")
    test_dict1 = {
        "forename": "Hans",
        "familyname": "Schmidt",
        "role": "Vorsitzender"
    }
    person3 = Person.from_dict(test_dict1)
    print(f"Person3 from_dict ohne role_schema: {person3.role_schema}")
    
    test_dict2 = {
        "forename": "Anna",
        "familyname": "Müller",
        "role": "Kassiererin",
        "role_schema": "KASSIERER"
    }
    person4 = Person.from_dict(test_dict2)
    print(f"Person4 from_dict mit role_schema: {person4.role_schema}")
    
    # Test 5: Direkter Test der role_schema-Mapping-Funktion
    print("\n=== Test: map_role_to_schema_entry ===")
    rollen = ["Malermeister", "Vorstandsmitglied", "Schriftführer", "Vereinsführer", "Kassenwart"]
    for rolle in rollen:
        normalisiert = normalize_and_match_role(rolle)
        schema = map_role_to_schema_entry(normalisiert or rolle)
        print(f"Rolle: {rolle} -> Normalisiert: {normalisiert} -> Schema: {schema}")


if __name__ == "__main__":
    test_person_with_role_schema()