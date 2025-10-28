#!/usr/bin/env python3
"""
Extract all postcodes from property listings and save a sorted list.

Scans rightmove-output/properties/ for property files, extracts postcodes, and writes
a sorted list to config/property_listing_postcodes.txt.
"""

import json
from pathlib import Path
from typing import Set

PROPERTIES_DIR = Path("rightmove-output/properties")
CONFIG_DIR = Path("config")
PROPERTY_POSTCODES_FILE = CONFIG_DIR / "property_listing_postcodes.txt"


def extract_postcodes() -> Set[str]:
    """Extract all postcodes from property files."""
    postcodes = set()

    if not PROPERTIES_DIR.exists():
        return postcodes

    # Find all property files (e.g., rightmove_167666876-0.json)
    for file in PROPERTIES_DIR.glob("*_*-*.json"):
        try:
            with open(file, "r") as f:
                data = json.load(f)
                postcode = data.get("postcode")
                if postcode:
                    postcodes.add(postcode)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file}: {e}")

    return postcodes


def main():
    """Extract postcodes and save to file."""
    postcodes = extract_postcodes()

    if not postcodes:
        print("No postcodes found")
        return

    CONFIG_DIR.mkdir(exist_ok=True)

    sorted_postcodes = sorted(postcodes)

    with open(PROPERTY_POSTCODES_FILE, "w") as f:
        for postcode in sorted_postcodes:
            f.write(f"{postcode}\n")

    print(f"Extracted {len(sorted_postcodes)} unique postcodes")
    print(f"Saved to {PROPERTY_POSTCODES_FILE}")


if __name__ == "__main__":
    main()
