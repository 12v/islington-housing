#!/usr/bin/env python3
"""
Scrape the next postcode from the property listing postcodes list.

Maintains state in config/registry_scraper_state.json to track which postcode
was last scraped. On each run, scrapes the next postcode in the list, cycling
back to the beginning when reaching the end.
"""

import asyncio
import json
from pathlib import Path
from scrapers.registry_scraper import scrape_postcode

CONFIG_DIR = Path("config")
POSTCODES_FILE = CONFIG_DIR / "property_listing_postcodes.txt"
STATE_FILE = CONFIG_DIR / "registry_scraper_state.json"


def load_postcodes() -> list[str]:
    """Load postcodes from file."""
    if not POSTCODES_FILE.exists():
        print(f"Postcodes file not found: {POSTCODES_FILE}")
        return []

    with open(POSTCODES_FILE) as f:
        return [line.strip() for line in f if line.strip()]


def load_state() -> dict:
    """Load current state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_postcode": None}


def save_state(state: dict):
    """Save state to file."""
    CONFIG_DIR.mkdir(exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_next_postcode(postcodes: list[str]) -> str:
    """Get the next postcode to scrape."""
    if not postcodes:
        raise ValueError("No postcodes available")

    state = load_state()
    last_postcode = state.get("last_postcode")

    # If no last postcode, start with first
    if last_postcode is None:
        return postcodes[0]

    # Find last postcode in list
    try:
        current_index = postcodes.index(last_postcode)
    except ValueError:
        # Last postcode no longer in list, start over
        return postcodes[0]

    # Return next postcode, cycling back to start if at end
    next_index = (current_index + 1) % len(postcodes)
    return postcodes[next_index]


async def main():
    postcodes = load_postcodes()

    if not postcodes:
        print("No postcodes to scrape")
        return

    next_postcode = get_next_postcode(postcodes)

    print(f"Scraping postcode: {next_postcode}")

    await scrape_postcode(next_postcode)

    # Save state after successful scrape
    state = {"last_postcode": next_postcode}
    save_state(state)

    print(f"State updated: {next_postcode}")


if __name__ == "__main__":
    asyncio.run(main())
