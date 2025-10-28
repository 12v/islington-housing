import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin
import asyncio
import aiohttp

logger = logging.getLogger(__name__)


class RightMoveScraper:
    """Scraper for RightMove rental listings using HTTP requests."""

    BASE_URL = "https://www.rightmove.co.uk"

    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session: Optional[aiohttp.ClientSession] = None
        self.postcode_location_map: Dict[str, str] = {}
        self._load_postcode_mapping()

    def _load_postcode_mapping(self):
        """Load postcode to RightMove location ID mapping from JSON."""
        try:
            mapping_file = (
                Path(__file__).parent.parent
                / "config"
                / "postcode_location_mapping.json"
            )
            if mapping_file.exists():
                with open(mapping_file, "r") as f:
                    data = json.load(f)
                    self.postcode_location_map = data.get("postcodes", {})
                    logger.info(
                        f"Loaded {len(self.postcode_location_map)} postcode mappings"
                    )
            else:
                logger.warning(f"Postcode mapping file not found: {mapping_file}")
        except Exception as e:
            logger.error(f"Error loading postcode mapping: {e}")

    async def initialize(self):
        """Initialize HTTP session."""
        self.session = aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()

    async def scrape_postcode(self, postcode: str) -> Dict[str, Any]:
        """Scrape listings for a specific full postcode.

        Args:
            postcode: UK full postcode (e.g., "N19 3RU", "EC1A 1AA")

        Returns:
            Dictionary with scraped_at, source, postcode, and properties list
        """
        if " " not in postcode:
            raise ValueError(
                f"Must use full postcode format (e.g., 'N19 3RU'), not outcode '{postcode}'"
            )

        logger.info(f"Starting RightMove scrape for postcode: {postcode}")

        properties = []
        scraped_at = datetime.now(timezone.utc).isoformat()

        try:
            # Get location code for this postcode
            location_code = self.postcode_location_map.get(postcode)

            if not location_code:
                raise ValueError(
                    f"Postcode '{postcode}' not found in Islington postcode mapping"
                )

            # Fetch first page
            search_url = (
                f"{self.BASE_URL}/property-to-rent/find.html?"
                f"locationIdentifier=POSTCODE%5E{location_code}"
            )
            logger.info(f"Fetching: {search_url}")

            properties = await self._fetch_page(search_url)
            logger.info(f"Found {len(properties)} properties on page 1")

        except Exception as e:
            logger.error(f"Error scraping RightMove for {postcode}: {e}", exc_info=True)

        result = {
            "source": "rightmove",
            "scraped_at": scraped_at,
            "postcode": postcode,
            "total_properties": len(properties),
            "properties": properties,
        }

        return result

    async def _fetch_page(self, url: str) -> List[Dict[str, Any]]:
        """Fetch a search page and extract property data."""
        try:
            async with self.session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    logger.error(f"HTTP {response.status} for {url}")
                    return []

                html = await response.text()
                return self._extract_properties_from_html(html)

        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return []

    def _extract_properties_from_html(self, html: str) -> List[Dict[str, Any]]:
        """Extract property data from __NEXT_DATA__ JSON in HTML."""
        properties = []

        try:
            # Extract __NEXT_DATA__ script
            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                html,
                re.DOTALL,
            )

            if not match:
                logger.warning("Could not find __NEXT_DATA__ in page")
                return properties

            json_str = match.group(1)
            data = json.loads(json_str)

            # Navigate to searchResults.properties
            search_results = (
                data.get("props", {}).get("pageProps", {}).get("searchResults", {})
            )

            if not search_results:
                logger.warning("No searchResults in __NEXT_DATA__")
                return properties

            props_list = search_results.get("properties", [])
            logger.info(f"Extracted {len(props_list)} properties from JSON")

            # Store full property objects
            # Filter out featured properties (premium is OK)
            for prop in props_list:
                if prop.get("featuredProperty"):
                    logger.debug(f"Skipping featured property {prop.get('id')}")
                    continue
                properties.append(prop)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing __NEXT_DATA__ JSON: {e}")
        except Exception as e:
            logger.error(f"Error extracting properties: {e}")

        return properties

    def _get_next_version(self, property_id: int) -> int:
        """Get the next version number for a property.

        Returns:
            Next version number (0 for first, 1 for second, etc.)
        """
        props_dir = self.output_dir / "properties"
        pattern = f"rightmove_{property_id}-*.json"
        existing_files = sorted(props_dir.glob(pattern))

        if not existing_files:
            return 0  # First pass

        # Extract version numbers from existing files
        versions = []
        for f in existing_files:
            match = re.search(r'-(\d+)\.json$', f.name)
            if match:
                versions.append(int(match.group(1)))

        return max(versions) + 1 if versions else 0

    def _has_changed(self, property_id: int, core_prop_data: Dict[str, Any]) -> bool:
        """Check if property core data has changed compared to latest version.

        Compares only the core property data, ignoring timestamp fields.

        Args:
            property_id: Property ID
            core_prop_data: Property data without scraped_at/source/postcode

        Returns:
            True if property is new or has changed, False if identical to latest.
        """
        props_dir = self.output_dir / "properties"
        pattern = f"rightmove_{property_id}-*.json"
        existing_files = sorted(props_dir.glob(pattern))

        if not existing_files:
            return True  # New property

        latest_file = existing_files[-1]

        try:
            with open(latest_file, "r") as f:
                latest_prop_json = json.load(f)

            # Extract just the core property data (everything except metadata and timestamps)
            ignore_fields = {"source", "scraped_at", "postcode", "updateDate"}
            latest_core = {
                k: v
                for k, v in latest_prop_json.items()
                if k not in ignore_fields
            }
            new_core = {
                k: v
                for k, v in core_prop_data.items()
                if k not in ignore_fields
            }

            # Compare JSON content (normalize by sorting keys)
            latest_str = json.dumps(latest_core, sort_keys=True, default=str)
            new_str = json.dumps(new_core, sort_keys=True, default=str)

            return latest_str != new_str
        except Exception as e:
            logger.warning(f"Error comparing property {property_id}: {e}")
            return True  # Default to saving on error

    async def download_photos(self, properties: List[Dict[str, Any]]):
        """Download new photos for properties, skipping existing ones."""
        photo_dir = self.output_dir / "photos"
        photo_dir.mkdir(parents=True, exist_ok=True)

        total_downloaded = 0

        for prop in properties:
            property_id = prop.get("id")
            if not property_id:
                continue

            images = prop.get("images", [])
            if not images:
                continue

            prop_photo_dir = photo_dir / "rightmove" / str(property_id)
            prop_photo_dir.mkdir(parents=True, exist_ok=True)

            for idx, image in enumerate(images):
                src_url = image.get("srcUrl") or image.get("url")
                if not src_url:
                    continue

                # Ensure full URL
                if not src_url.startswith("http"):
                    src_url = urljoin(self.BASE_URL, src_url)

                photo_path = prop_photo_dir / f"photo-{idx}.jpg"

                # Skip if photo already exists
                if photo_path.exists():
                    logger.debug(
                        f"Photo {idx} already exists for property {property_id}, skipping"
                    )
                    continue

                try:
                    await self._download_file(src_url, photo_path)
                    total_downloaded += 1
                    logger.debug(f"Downloaded photo {idx} for property {property_id}")
                except Exception as e:
                    logger.warning(
                        f"Error downloading photo {idx} for property {property_id}: {e}"
                    )

        logger.info(f"Downloaded {total_downloaded} new photos")

    async def _download_file(self, url: str, file_path: Path):
        """Download a file from URL."""
        async with self.session.get(
            url, timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            if response.status == 200:
                with open(file_path, "wb") as f:
                    f.write(await response.read())
            else:
                logger.warning(f"HTTP {response.status} downloading {url}")

    async def run(self, postcodes: List[str]) -> List[Dict[str, Any]]:
        """Run scraper for multiple postcodes.

        Args:
            postcodes: List of full postcodes (e.g., ["N19 3NR", "N19 3AA"])
            download_photos: Whether to download photos (default True)
        """
        await self.initialize()
        results = []

        try:
            for postcode in postcodes:
                result = await self.scrape_postcode(postcode)

                if result["properties"]:
                    logger.info(
                        f"Downloading photos for {len(result['properties'])} properties..."
                    )
                    await self.download_photos(result["properties"])

                results.append(result)

                # Save each property as a separate JSON file with versioning
                props_dir = self.output_dir / "properties"
                props_dir.mkdir(parents=True, exist_ok=True)

                saved_count = 0
                for prop in result["properties"]:
                    property_id = prop.get("id")
                    if not property_id:
                        continue

                    # Check if property data has changed compared to latest version
                    if not self._has_changed(property_id, prop):
                        logger.debug(
                            f"Property {property_id} unchanged, skipping new version"
                        )
                        continue

                    # Get next version number and create versioned filename
                    next_version = self._get_next_version(property_id)

                    # Create property JSON with source, scraped_at, postcode at root level
                    # Exclude photos_local from output
                    prop_data = {k: v for k, v in prop.items() if k != "photos_local"}
                    prop_json = {
                        "source": result["source"],
                        "scraped_at": result["scraped_at"],
                        "postcode": result["postcode"],
                        **prop_data,
                    }

                    prop_file = (
                        props_dir / f"rightmove_{property_id}-{next_version}.json"
                    )
                    with open(prop_file, "w") as f:
                        json.dump(prop_json, f, indent=2)
                    logger.debug(f"Saved property {property_id} v{next_version} to {prop_file}")
                    saved_count += 1

                logger.info(
                    f"Saved {saved_count} property file versions for {postcode}"
                )

                # Add small delay between postcodes
                await asyncio.sleep(2)

        finally:
            await self.close()

        return results


async def main():
    """Test the scraper."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RightMoveScraper()
    results = await scraper.run(["N19 3NR"])

    # Print summary
    if results:
        result = results[0]
        print(f"\n{'='*70}")
        print(f"Source: {result['source']}")
        print(f"Scraped: {result['scraped_at']}")
        print(f"Postcode: {result['postcode']}")
        print(f"Total properties: {result['total_properties']}")
        print(f"{'='*70}\n")

        if result["properties"]:
            first = result["properties"][0]
            print(f"First property sample:")
            print(
                json.dumps(
                    {
                        "id": first.get("id"),
                        "displayAddress": first.get("displayAddress"),
                        "bedrooms": first.get("bedrooms"),
                        "bathrooms": first.get("bathrooms"),
                        "propertySubType": first.get("propertySubType"),
                        "price": first.get("price"),
                        "images_count": len(first.get("images", [])),
                        "photos_local_count": len(first.get("photos_local", [])),
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
