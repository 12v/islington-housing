#!/usr/bin/env python3
"""Test scraper with a few known postcodes."""

import asyncio
import logging
from scrapers.rightmove import RightMoveScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    test_postcodes = ["N19 3NR", "E8 2BB", "EC1A 1AA"]
    logger.info(f"Starting test scrape for {len(test_postcodes)} postcodes")

    scraper = RightMoveScraper()
    results = await scraper.run(test_postcodes)

    total_props = sum(r["total_properties"] for r in results)
    logger.info(f"\n{'='*70}")
    logger.info(f"Test scrape completed!")
    logger.info(f"Total properties found: {total_props}")
    logger.info(f"Results saved to output/properties/")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(main())
