#!/usr/bin/env python3
"""
Run RightMove scraper for all Islington postcodes.
Usage: python scrape_all.py [--with-photos]
"""

import asyncio
import json
import logging
import sys
from scrapers.rightmove import RightMoveScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    with open("config/postcodes.json", "r") as f:
        config = json.load(f)

    postcodes = config["postcodes"]

    logger.info(f"Starting RightMove scrape for {len(postcodes)} postcodes")

    scraper = RightMoveScraper()
    results = await scraper.run(postcodes)

    # Summary
    total_props = sum(r["total_properties"] for r in results)
    logger.info(f"\n{'='*70}")
    logger.info(f"Scrape completed!")
    logger.info(f"Total properties found: {total_props}")
    logger.info(f"Results saved to output/properties/")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(main())
