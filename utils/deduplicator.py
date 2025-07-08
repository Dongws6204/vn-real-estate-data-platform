import logging

from utils.mongodb import MongoDBClient

logger = logging.getLogger(__name__)

class Deduplicator:
    def __init__(self, db_client: MongoDBClient):
        # ...
        self.db_client = db_client

    def filter_new_listings(self, source: str, listings: list[dict]) -> list[dict]:
        if not listings:
            return []

        incoming_ids = [listing['source_id'] for listing in listings if 'source_id' in listing]
        
        listings_with_id = len(incoming_ids)
        listings_without_id = len(listings) - listings_with_id
        
        if listings_without_id > 0:
            logger.warning(f"[{source}] {listings_without_id} listings were found without a 'source_id' and will be skipped in deduplication check.")
            
        if not incoming_ids:
            return [] 
            
        existing_ids_set = self.db_client.get_existing_ids(source, incoming_ids)
        
        new_listings = [
            listing for listing in listings 
            if listing.get('source_id') and listing.get('source_id') not in existing_ids_set
        ]
        
        logger.info(
            f"[{source}] Deduplication summary: "
            f"Incoming={len(listings)}, "
            f"Existing={len(existing_ids_set)}, "
            f"New={len(new_listings)}"
        )
        
        return new_listings

    # def find_cross_source_duplicates(self, listing_data):
    #     # Logic deduplicatie 2
    #     pass