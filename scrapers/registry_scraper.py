import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("register-output")
OUTPUT_DIR.mkdir(exist_ok=True)


async def search_register(postcode: str, page: Page) -> List[Dict[str, Any]]:
    """Search Islington register for a postcode using Playwright."""
    logger.info(f"Searching register for: {postcode}")

    logger.info("Loading register page...")
    await page.goto(
        "https://propertylicensing.islington.gov.uk/public-register",
        wait_until="domcontentloaded",
        timeout=15000,
    )

    await page.wait_for_timeout(1000)

    logger.info(f"Entering postcode: {postcode}")
    await page.fill('input#search_query', postcode, timeout=5000)
    await page.wait_for_timeout(500)
    await page.press('input#search_query', "Enter", timeout=5000)
    logger.info("Submitted search...")

    await page.wait_for_timeout(2000)

    properties = await extract_properties(page, postcode)
    logger.info(f"Found {len(properties)} properties on search results page")

    return properties


async def extract_properties(page: Page, postcode: str) -> List[Dict[str, Any]]:
    """Extract property links from the search results page."""
    properties = []

    try:
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
        path_only = property_url.split("?")[0]
        link = await page.query_selector(f"a[href*='{path_only}']")

        if link:
            async with page.expect_navigation(timeout=15000):
                await link.click()
        else:
            await page.goto(
                f"https://propertylicensing.islington.gov.uk{property_url}",
                wait_until="domcontentloaded",
                timeout=15000,
            )

        await page.wait_for_timeout(1000)

        details = {}

        h1_selector = "div.grid-row div.column-full h1.heading-large"
        h1 = await page.query_selector(h1_selector)
        if h1:
            h1_text = (await h1.text_content()).strip()
            details["address"] = h1_text
            logger.debug(f"Extracted address: {h1_text}")

        h2_selector = "div.grid-row div.column-full h2.heading-medium"
        h2 = await page.query_selector(h2_selector)
        if h2:
            h2_text = (await h2.text_content()).strip()
            if "ISL-" in h2_text:
                licence_num = h2_text.replace("Licence number", "").strip()
                details["licence_number"] = licence_num
                logger.debug(f"Extracted licence number: {licence_num}")

        p_elements = await page.query_selector_all("div.grid-row div.column-full > div > p")

        for p in p_elements:
            try:
                full_text = await p.text_content()
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]

                if len(lines) >= 2:
                    label = lines[0]
                    value = lines[1]

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

        additional_link = await page.query_selector("a:has-text('Additional details'), a[href*='additional']")
        if additional_link:
            logger.info("Found 'Additional details' link, clicking...")
            try:
                await additional_link.click()
                await page.wait_for_timeout(1500)

                additional_fields = {}
                p_elements = await page.query_selector_all("div.grid-row div.column-full > div > p")

                for p in p_elements:
                    try:
                        full_text = await p.text_content()
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]

                        if len(lines) >= 2:
                            label = lines[0]
                            value = lines[1]

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


def find_next_version(base_path: Path, license_num: str) -> int:
    """Find the next version number for a license file."""
    version = 0
    while (base_path / f"{license_num}_{version}.json").exists():
        version += 1
    return version


def data_changed(new_data: Dict[str, Any], old_data: Dict[str, Any]) -> bool:
    """Check if data has changed, ignoring scraped_at field."""
    new_copy = {k: v for k, v in new_data.items() if k != "scraped_at"}
    old_copy = {k: v for k, v in old_data.items() if k != "scraped_at"}
    return new_copy != old_copy


async def save_results(properties: List[Dict[str, Any]]) -> Path:
    """Save results to register-output directory with versioning."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    for prop in properties:
        license_num = prop.get("details", {}).get("licence_number", "unknown")

        prop_to_save = {
            "address": prop.get("address"),
            "postcode": prop.get("postcode"),
            "details": prop.get("details", {}),
            "scraped_at": datetime.now().isoformat(),
        }

        # Check if any version exists
        version = 0
        latest_file = OUTPUT_DIR / f"{license_num}_{version}.json"

        # Find the latest version
        while (OUTPUT_DIR / f"{license_num}_{version}.json").exists():
            version += 1

        # If files exist, check the most recent one
        if version > 0:
            latest_version = version - 1
            latest_file = OUTPUT_DIR / f"{license_num}_{latest_version}.json"

            with open(latest_file, "r") as f:
                old_data = json.load(f)

            if data_changed(prop_to_save, old_data):
                # Data changed, create new version
                new_file = OUTPUT_DIR / f"{license_num}_{version}.json"
                with open(new_file, "w") as f:
                    json.dump(prop_to_save, f, indent=2)
                logger.info(f"Data changed, saved new version: {license_num}_{version}.json")
            else:
                # Data unchanged, just update scraped_at in latest version
                old_data["scraped_at"] = prop_to_save["scraped_at"]
                with open(latest_file, "w") as f:
                    json.dump(old_data, f, indent=2)
                logger.info(f"No changes, updated scraped_at: {license_num}_{latest_version}.json")
        else:
            # First version
            new_file = OUTPUT_DIR / f"{license_num}_0.json"
            with open(new_file, "w") as f:
                json.dump(prop_to_save, f, indent=2)
            logger.info(f"Saved new license: {license_num}_0.json")

    return OUTPUT_DIR


async def scrape_postcode(postcode: str, headless: bool = True) -> Path:
    """Main scraping function for a single postcode."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()

        try:
            properties = await search_register(postcode, page)

            logger.info(f"Fetching details for {len(properties)} properties...")
            for prop in properties:
                details = await fetch_property_details(prop["detail_url"], page)
                prop["details"] = details
                await page.wait_for_timeout(500)

            output_dir = await save_results(properties)

            return output_dir

        finally:
            await browser.close()
