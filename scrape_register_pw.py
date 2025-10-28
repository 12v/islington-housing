#!/usr/bin/env python3
"""
Lightweight Playwright-based scraper for Islington property register.

Saves detailed property information to register-output/ directory.

Usage: python scrape_register_pw.py [postcode]
Example: python scrape_register_pw.py "N19 4JN"
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from playwright.async_api import async_playwright, Page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("register-output")
OUTPUT_DIR.mkdir(exist_ok=True)


async def search_register(postcode: str, page: Page) -> List[Dict[str, Any]]:
    """Search Islington register for a postcode using Playwright."""
    logger.info(f"Searching register for: {postcode}")

    # Navigate to register
    logger.info("Loading register page...")
    await page.goto(
        "https://propertylicensing.islington.gov.uk/public-register",
        wait_until="domcontentloaded",
        timeout=15000,
    )

    # Wait for JS to render
    await page.wait_for_timeout(1000)

    # Find and fill postcode input
    logger.info(f"Entering postcode: {postcode}")
    try:
        await page.fill('input[placeholder*="postcode" i]', postcode, timeout=5000)
    except Exception as e:
        logger.warning(f"Could not fill postcode input: {e}")
        inputs = await page.query_selector_all("input[type='search'], input[type='text']")
        if inputs:
            await inputs[0].fill(postcode)
            logger.info("Filled first input field")
        else:
            logger.error("No input fields found")
            return []

    await asyncio.sleep(0.5)

    # Submit search
    try:
        await page.press('input[placeholder*="postcode" i]', "Enter", timeout=5000)
    except Exception:
        inputs = await page.query_selector_all("input[type='search'], input[type='text']")
        if inputs:
            await inputs[0].press("Enter")
    logger.info("Submitted search...")

    # Wait for results
    await page.wait_for_timeout(2000)

    # Extract properties with links
    properties = await extract_properties(page, postcode)
    logger.info(f"Found {len(properties)} properties on search results page")

    return properties


async def extract_properties(page: Page, postcode: str) -> List[Dict[str, Any]]:
    """Extract property links from the search results page."""
    properties = []

    try:
        # Find all property links in h2 tags
        h2_links = await page.query_selector_all("h2 a")
        if h2_links:
            logger.info(f"Found {len(h2_links)} property links")
            for link in h2_links:
                text = await link.text_content()
                href = await link.get_attribute("href")
                if text and href:
                    properties.append({
                        "address": text.strip(),
                        "postcode": postcode,
                        "detail_url": href,
                    })

    except Exception as e:
        logger.error(f"Error extracting properties: {e}")

    return properties


async def fetch_property_details(property_url: str, page: Page) -> Dict[str, Any]:
    """Fetch detailed information about a property by following its link."""
    logger.info(f"Fetching property details from: {property_url}")

    try:
        # Click the link to navigate (maintains session context better than goto)
        # Extract just the path part for the selector
        path_only = property_url.split("?")[0]
        link = await page.query_selector(f"a[href*='{path_only}']")

        if link:
            # Click and wait for navigation
            async with page.expect_navigation(timeout=15000):
                await link.click()
        else:
            # Fallback: direct navigation (may have session issues)
            await page.goto(
                f"https://propertylicensing.islington.gov.uk{property_url}",
                wait_until="domcontentloaded",
                timeout=15000,
            )

        await page.wait_for_timeout(1000)

        details = {}

        # Extract address from <h1> within the main content area
        h1_selector = "div.grid-row div.column-full h1.heading-large"
        h1 = await page.query_selector(h1_selector)
        if h1:
            h1_text = (await h1.text_content()).strip()
            details["address"] = h1_text
            logger.debug(f"Extracted address: {h1_text}")

        # Extract licence number from <h2>
        h2_selector = "div.grid-row div.column-full h2.heading-medium"
        h2 = await page.query_selector(h2_selector)
        if h2:
            h2_text = (await h2.text_content()).strip()
            # Format: "Licence number ISL-403549725326"
            if "ISL-" in h2_text:
                licence_num = h2_text.replace("Licence number", "").strip()
                details["licence_number"] = licence_num
                logger.debug(f"Extracted licence number: {licence_num}")

        # Extract all p elements containing span.bold within the property details div
        p_elements = await page.query_selector_all("div.grid-row div.column-full > div > p")

        for p in p_elements:
            try:
                # Get the full text content
                full_text = await p.text_content()

                # Split by line breaks to get label and value
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]

                if len(lines) >= 2:
                    label = lines[0]
                    value = lines[1]

                    # Map to our desired field names
                    if label == "Licence type":
                        details["licence_type"] = value
                    elif label == "Licence reference number":
                        details["licence_number"] = value
                    elif label == "Year built":
                        details["year_built"] = value
                    elif label == "Property description":
                        details["property_description"] = value
                    elif label == "Licence holder name":
                        details["licence_holder_name"] = value
                    elif label == "Licence holder address":
                        details["licence_holder_address"] = value
                    elif label == "UPRN":
                        details["uprn"] = value
                    elif label == "Licence start date":
                        details["licence_start_date"] = value
                    elif label == "Licence end date":
                        details["licence_end_date"] = value

                    logger.debug(f"Extracted {label}: {value}")
            except Exception as e:
                logger.debug(f"Error extracting field: {e}")

        # Look for "Additional details" link and click it
        additional_link = await page.query_selector("a:has-text('Additional details'), a[href*='additional']")
        if additional_link:
            logger.info("Found 'Additional details' link, clicking...")
            try:
                # Click the link and wait for navigation
                await additional_link.click()
                await page.wait_for_timeout(1500)

                # Extract all additional details
                additional_fields = {}
                p_elements = await page.query_selector_all("div.grid-row div.column-full > div > p")

                for p in p_elements:
                    try:
                        full_text = await p.text_content()
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]

                        if len(lines) >= 2:
                            label = lines[0]
                            value = lines[1]

                            # Store all fields from additional page
                            # Convert label to snake_case key
                            key = label.lower().replace(" ", "_").replace(".", "").replace("-", "_")
                            additional_fields[key] = value
                            logger.debug(f"Extracted additional {label}: {value}")

                    except Exception as e:
                        logger.debug(f"Error extracting additional field: {e}")

                if additional_fields:
                    details["additional_details"] = additional_fields
                    logger.info(f"Fetched {len(additional_fields)} additional detail fields")

            except Exception as e:
                logger.warning(f"Could not fetch additional details: {e}")

        return details

    except Exception as e:
        logger.error(f"Error fetching property details: {e}")
        return {"error": str(e)}


async def save_results(postcode: str, properties: List[Dict[str, Any]]):
    """Save results to register-output directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    postcode_dir = OUTPUT_DIR / postcode.replace(" ", "-")
    postcode_dir.mkdir(exist_ok=True)

    # Save summary
    summary = {
        "postcode": postcode,
        "timestamp": timestamp,
        "total_properties": len(properties),
        "properties": [
            {
                "address": p.get("address"),
                "licence_number": p.get("details", {}).get("licence_number"),
            }
            for p in properties
        ],
    }

    summary_file = postcode_dir / "summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved summary to {summary_file}")

    # Save individual properties
    for prop in properties:
        # Get license number from details
        license_num = prop.get("details", {}).get("licence_number", "unknown")

        # Remove detail_url from saved data
        prop_to_save = {
            "address": prop.get("address"),
            "postcode": prop.get("postcode"),
            "details": prop.get("details", {}),
        }

        prop_file = postcode_dir / f"{license_num}.json"
        with open(prop_file, "w") as f:
            json.dump(prop_to_save, f, indent=2)

    return postcode_dir


async def main():
    """Run the scraper."""
    postcode = sys.argv[1] if len(sys.argv) > 1 else "N19 4JN"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Search for properties
            properties = await search_register(postcode, page)

            # Fetch details for each property
            logger.info(f"Fetching details for {len(properties)} properties...")
            for prop in properties:
                details = await fetch_property_details(prop["detail_url"], page)
                prop["details"] = details
                await asyncio.sleep(0.5)  # Be respectful with requests

            # Save results
            output_dir = await save_results(postcode, properties)

            # Print summary
            print(f"\n{'='*70}")
            print(f"Register Search Results for {postcode}")
            print(f"{'='*70}")
            print(f"Properties found: {len(properties)}")
            print(f"Output saved to: {output_dir}/")

            if properties:
                print("\nProperties:")
                for i, prop in enumerate(properties, 1):
                    print(f"  {i}. {prop['address']}")

            print(f"{'='*70}\n")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
