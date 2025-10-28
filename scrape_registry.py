#!/usr/bin/env python3
"""
Scrape the next 5 postcodes from the property listing postcodes list.

Maintains state in config/registry_scraper_state.json to track which postcode
was last scraped. On each run, scrapes the next 5 postcodes in the list, with
a 1-minute delay between each, cycling back to the beginning when reaching the end.
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


def get_next_postcodes(postcodes: list[str], count: int = 5) -> list[str]:
    """Get the next N postcodes to scrape."""
    if not postcodes:
        raise ValueError("No postcodes available")

    state = load_state()
    last_postcode = state.get("last_postcode")

    # Find starting index
    if last_postcode is None:
        start_index = 0
    else:
        try:
            start_index = (postcodes.index(last_postcode) + 1) % len(postcodes)
        except ValueError:
            # Last postcode no longer in list, start over
            start_index = 0

    # Get next N postcodes, wrapping around if necessary
    next_postcodes = []
    for i in range(count):
        index = (start_index + i) % len(postcodes)
        next_postcodes.append(postcodes[index])

    return next_postcodes


async def main():
    postcodes = load_postcodes()

    if not postcodes:
        print("No postcodes to scrape")
        return

    next_postcodes = get_next_postcodes(postcodes, count=5)

    for i, postcode in enumerate(next_postcodes):
        print(f"Scraping postcode {i + 1}/5: {postcode}")
        await scrape_postcode(postcode)

        # Save state after each scrape
        state = {"last_postcode": postcode}
        save_state(state)

        # Wait 1 minute before next postcode (except after last one)
        if i < len(next_postcodes) - 1:
            print("Waiting 1 minute before next postcode...")
            await asyncio.sleep(60)

    print("Completed scraping 5 postcodes")


if __name__ == "__main__":
    asyncio.run(main())
