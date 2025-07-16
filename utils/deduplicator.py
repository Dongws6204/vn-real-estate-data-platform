import logging
from typing import List, Dict, Optional
from utils.mongodb import MongoDBClient

logger = logging.getLogger(__name__)


class Deduplicator:
    def __init__(self, db_client: MongoDBClient):
        """
        Deduplicator is responsible for identifying new, updatable, or duplicate listings
        based on their source_id and content.
        """
        self.db_client = db_client

    def classify_listings(self, source: str, listings: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Classify listings into new, update, and duplicate based on source_id and content.

        Args:
            source (str): Source name (e.g., 'raovat321').
            listings (List[Dict]): Listings scraped from the source.

        Returns:
            Dict[str, List[Dict]]: {
                'new': [...],
                'update': [...],
                'duplicate': [...]
            }
        """
        new, to_update, duplicates = [], [], []

        # Step 1: Extract all source_ids
        source_ids = [item['source_id'] for item in listings if 'source_id' in item]
        existing_ids = self.db_client.get_existing_ids(source, source_ids)

        for listing in listings:
            source_id = listing.get('source_id')

            if not source_id:
                logger.warning(f"[{source}] Listing without source_id skipped.")
                continue

            if source_id in existing_ids:
                # TODO: Add meaningful comparison for updates here
                duplicates.append(listing)
            else:
                new.append(listing)

        logger.info(
            f"[{source}] Classification Summary - New: {len(new)}, Update: {len(to_update)}, Duplicate: {len(duplicates)}"
        )

        return {
            'new': new,
            'update': to_update,
            'duplicate': duplicates
        }

    def _needs_update(self, existing: Dict, incoming: Dict) -> bool:
        """
        Check whether a listing needs updating based on field differences.
        (Placeholder – to be implemented.)
        """
        return False

    def _check_cross_source_duplicate(self, listing: Dict) -> Optional[Dict]:
        """
        Check for cross-source duplicates.
        (Placeholder – to be implemented.)
        """
        return None
