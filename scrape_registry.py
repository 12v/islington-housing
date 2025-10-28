#!/usr/bin/env python3
"""
Scrape Islington property register for a given postcode.

Usage: python scrape_registry.py <postcode>
Example: python scrape_registry.py "N19 4JN"
"""

import asyncio
import logging
import sys
from scrapers.registry_scraper import scrape_postcode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


async def main():
    if len(sys.argv) < 2:
        print("Usage: python scrape_registry.py <postcode>")
        print('Example: python scrape_registry.py "N19 4JN"')
        sys.exit(1)

    postcode = sys.argv[1]

    output_dir = await scrape_postcode(postcode)

    print(f"\n{'='*70}")
    print(f"Register Search Results for {postcode}")
    print(f"{'='*70}")
    print(f"Output saved to: {output_dir}/")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
