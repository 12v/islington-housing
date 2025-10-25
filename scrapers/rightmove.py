import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin
import aiohttp
from playwright.async_api import async_playwright, Page, BrowserContext

logger = logging.getLogger(__name__)


class RightMoveScraper:
    """Scraper for RightMove rental listings."""

    BASE_URL = "https://www.rightmove.co.uk"

    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session: Optional[aiohttp.ClientSession] = None
        self.context: Optional[BrowserContext] = None
        self.browser = None
        self.playwright = None

    async def initialize(self):
        """Initialize browser and session."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.firefox.launch(
            headless=True,
            args=["--no-sandbox"]
        )
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
        )
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close browser and session."""
        if self.session:
            await self.session.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def scrape_postcode(self, postcode: str, location_code: str = "1676") -> Dict[str, Any]:
        """Scrape listings for a specific postcode.

        Args:
            postcode: UK postcode (e.g., "N19")
            location_code: RightMove location code (use OUTCODE for outward code search)
        """
        logger.info(f"Starting RightMove scrape for postcode: {postcode}")

        properties = []
        page = None

        try:
            page = await self.context.new_page()

            # Use the improved URL format that returns proper results
            # OUTCODE searches by postcode outward code (e.g., N19, N1)
            search_url = (
                f"{self.BASE_URL}/property-to-rent/find.html?"
                f"searchLocation={postcode}&"
                f"useLocationIdentifier=true&"
                f"locationIdentifier=OUTCODE%5E{location_code}&"
                f"radius=0.0&"
                f"_includeLetAgreed=on"
            )
            logger.info(f"Navigating to: {search_url}")

            await page.goto(search_url, wait_until="networkidle")
            await asyncio.sleep(2)

            # Extract property listings from the rendered page
            properties = await self._extract_properties(page)
            logger.info(f"Found {len(properties)} unique properties on page 1")

            # Handle pagination - scrape up to 3 pages
            page_num = 1
            while await self._has_next_page(page) and page_num < 3:
                page_num += 1
                logger.info(f"Moving to page {page_num}...")
                try:
                    await self._go_to_next_page(page)
                    await asyncio.sleep(2)
                    page_properties = await self._extract_properties(page)
                    logger.info(f"Found {len(page_properties)} unique properties on page {page_num}")
                    properties.extend(page_properties)
                except Exception as e:
                    logger.warning(f"Error scraping page {page_num}: {e}")
                    break

        except Exception as e:
            logger.error(f"Error scraping RightMove for {postcode}: {e}", exc_info=True)
        finally:
            if page:
                await page.close()

        result = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "rightmove",
            "postcode_filter": postcode,
            "properties": properties
        }

        return result

    async def _extract_properties(self, page: Page) -> List[Dict[str, Any]]:
        """Extract property data from the search results page."""
        properties = []

        try:
            # Get all property links on the page
            property_links = await page.query_selector_all("a[href*='/properties/']")
            logger.info(f"Found {len(property_links)} property links on page")

            # Track unique property IDs to avoid duplicates
            seen_ids: set = set()

            # Extract data from each link
            for idx, link in enumerate(property_links):
                try:
                    href = await link.get_attribute("href")
                    if not href or "/properties/" not in href:
                        continue

                    # Clean the URL - remove fragment and query string
                    property_url = urljoin(self.BASE_URL, href.split("#")[0])
                    property_id = self._extract_property_id(property_url)

                    if not property_id:
                        continue

                    # Skip duplicates
                    if property_id in seen_ids:
                        continue
                    seen_ids.add(property_id)

                    # Extract property card data
                    card_data = await link.evaluate("""el => {
                        let card = el.closest('div[class*="propertyCard"]');
                        if (!card) card = el.closest('article');
                        if (!card) card = el.parentElement;

                        return {
                            text: card?.innerText || '',
                            innerHTML: card?.innerHTML ? card.innerHTML.substring(0, 2000) : ''
                        }
                    }""")

                    listing_text = card_data["text"]

                    # Parse the listing text to extract structured data
                    structured_data = self._parse_listing_text(listing_text)

                    prop = {
                        "property_id": property_id,
                        "property_url": property_url,
                        **structured_data  # Include parsed fields
                    }

                    properties.append(prop)
                    logger.debug(f"Extracted property {property_id}")

                except Exception as e:
                    logger.warning(f"Error processing link {idx}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error extracting properties: {e}")

        return properties

    def _parse_listing_text(self, text: str) -> Dict[str, Any]:
        """Parse property listing text to extract structured data."""
        data = {}

        if not text:
            return data

        lines = text.split('\n')

        # Extract price (usually in format "£X,XXX pcm")
        for line in lines:
            if "£" in line and "pcm" in line:
                parts = line.split("|")
                for part in parts:
                    if "£" in part and "pcm" in part:
                        data["price"] = part.strip()
                        break
                break

        # Extract location and basic details from the text
        text_upper = text.upper()
        if "FEATURED" in text_upper:
            data["is_featured"] = True
        if "PROMOTED" in text_upper or "SPONSORED" in text_upper:
            data["is_promoted"] = True

        # Store full listing text
        data["listing_text"] = text[:500]

        return data

    def _extract_property_id(self, url: str) -> str:
        """Extract property ID from RightMove URL."""
        try:
            if "/properties/" in url:
                # Extract the part after /properties/
                parts = url.split("/properties/")
                if len(parts) > 1:
                    # Remove query string and fragment
                    prop_part = parts[1].split("?")[0].split("#")[0].split("/")[0]
                    if prop_part.isdigit():
                        return prop_part
        except:
            pass
        return ""

    async def _has_next_page(self, page: Page) -> bool:
        """Check if there's a next page link in the page header."""
        try:
            # RightMove uses <link rel="next"> in the page head for pagination
            # This is the semantic way to specify next page in HTML
            next_link = await page.query_selector("link[rel='next']")
            if next_link:
                href = await next_link.get_attribute("href")
                if href:
                    logger.debug(f"Found next page link: {href}")
                    return True
            return False
        except Exception as e:
            logger.warning(f"Error checking for next page: {e}")
            return False

    async def _go_to_next_page(self, page: Page):
        """Navigate to next page using the link[rel='next'] href."""
        try:
            next_link = await page.query_selector("link[rel='next']")
            if next_link:
                href = await next_link.get_attribute("href")
                if href:
                    next_url = urljoin(self.BASE_URL, href)
                    logger.debug(f"Navigating to next page: {next_url}")
                    await page.goto(next_url, wait_until="networkidle")
                    await asyncio.sleep(1)
                    return
            logger.warning("Could not find next page link")
        except Exception as e:
            logger.warning(f"Error navigating to next page: {e}")

    async def run(self, postcodes: List[str], location_codes: List[str] = None) -> List[Dict[str, Any]]:
        """Run scraper for multiple postcodes.

        Args:
            postcodes: List of postcodes (e.g., ["N19", "N1"])
            location_codes: List of location codes (default will use standard codes)
        """
        await self.initialize()
        results = []

        try:
            for idx, postcode in enumerate(postcodes):
                location_code = location_codes[idx] if location_codes and idx < len(location_codes) else "1676"
                result = await self.scrape_postcode(postcode, location_code)
                results.append(result)

                # Save result to file
                output_file = self.output_dir / f"rightmove_{postcode}.json"
                with open(output_file, "w") as f:
                    json.dump(result, f, indent=2)
                logger.info(f"Saved results to {output_file}")
        finally:
            await self.close()

        return results


async def main():
    """Test the scraper."""
    logging.basicConfig(level=logging.INFO)

    scraper = RightMoveScraper()
    results = await scraper.run(["N19"], ["1676"])

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
