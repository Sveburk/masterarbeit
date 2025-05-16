from typing import List, Dict, Union
from collections import defaultdict
import re
import xml.etree.ElementTree as ET


def parse_custom_attributes(attr_str: str) -> Dict[str, str]:
    result = {}
    for part in attr_str.split(";"):
        part = part.strip()
        if not part:
            continue
        key_value = part.split(":", 1)
        if len(key_value) == 2:
            key, value = key_value
            result[key.strip()] = value.strip()
    return result


def normalize_to_ddmmyyyy(date_str: str) -> Union[str, None]:
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", date_str):
        day, month, year = date_str.split(".")
    elif re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        year, month, day = date_str.split("-")
    elif re.match(r"^\d{1,2}\.\d{1,2}\.\d{2}$", date_str):
        day, month, year = date_str.split(".")
        year = "19" + year if int(year) >= 30 else "20" + year
    else:
        return None
    return f"{int(day):02d}.{int(month):02d}.{year}"


def extract_date_from_custom(custom_attr: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    dates = []
    for pattern in [r"date\s+\{([^}]+)\}", r"date\s*{([^}]+)}"]:
        for match in re.finditer(pattern, custom_attr):
            date_data = parse_custom_attributes(match.group(1))
            if "when" in date_data:
                date_str = date_data["when"]
                print(f"[DEBUG] Erkanntes Datum: {date_str}")

                if re.match(r"^\d{2}/\d{2}\.\d{2}\.\d{4}$", date_str):
                    tag1, rest = date_str.split("/")
                    tag2, month, year = rest.split(".")
                    from_date = f"{int(tag1):02d}.{int(month):02d}.{year}"
                    to_date = f"{int(tag2):02d}.{int(month):02d}.{year}"
                    dates.append({
                        "date_range": {"from": from_date, "to": to_date},
                        "original": date_str
                    })
                else:
                    normalized = normalize_to_ddmmyyyy(date_str)
                    if normalized:
                        dates.append({"date": normalized})
    return dates


def extract_custom_date(root: ET.Element, ns: Dict[str, str]) -> List[str]:
    """
    Extrahiert alle 'custom'-Attribute mit 'date {…}' aus <TextLine>-Elementen,
    dedupliziert normierte Strings.
    """
    seen = set()
    custom_attrs = []

    for line in root.findall(".//ns:TextLine", ns):
        custom = line.get("custom", "").strip()
        norm_custom = re.sub(r"\s+", "", custom.strip())  # Alle Leerzeichen entfernen
        if "date" in norm_custom and norm_custom not in seen:
            seen.add(norm_custom)
            custom_attrs.append(norm_custom)

    return custom_attrs

def combine_dates(all_custom_attrs: List[str]) -> List[Dict[str, object]]:
    single_counter = defaultdict(int)
    range_counter = defaultdict(int)

    for attr in set(all_custom_attrs):  # einmalige Strings
        for entry in extract_date_from_custom(attr):
            if "date" in entry:
                key = entry["date"]
                single_counter[key] += 1
            elif "date_range" in entry:
                key = (
                    entry["date_range"]["from"],
                    entry["date_range"]["to"],
                    entry["original"]
                )
                range_counter[key] += 1

    result: List[Dict[str, object]] = []
    for date, count in sorted(single_counter.items()):
        result.append({"date": date, "count": count})
    for (from_d, to_d, orig), count in sorted(range_counter.items()):
        result.append({
            "date_range": {"from": from_d, "to": to_d},
            "original": orig,
            "count": count
        })
    return result
