import time
from typing import Dict, List, Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError
from config.mongodb_config import MONGODB_CONFIG
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

# Create logs directory if it doesn't exist
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with UTF-8 encoding
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Create file handler with UTF-8 encoding
file_handler = RotatingFileHandler(
    os.path.join(log_directory, 'mongodb.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'  # Specify UTF-8 encoding
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

class MongoDBClient:
    def __init__(self):
        try:
            self.client = MongoClient(MONGODB_CONFIG['uri'])
            self.db = self.client[MONGODB_CONFIG['database']]
            # Test connection
            self.client.server_info()
            logger.info("Successfully connected to MongoDB")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}", exc_info=True)
            raise
        
    def get_collection(self, source: str):
        try:
            if source not in MONGODB_CONFIG['collections']:
                raise ValueError(f"Invalid source: {source}")
            return self.db[MONGODB_CONFIG['collections'][source]]
        except Exception as e:
            logger.error(f"Error getting collection for source {source}: {str(e)}", exc_info=True)
            raise

    def bulk_upsert_listings(self, source: str, listings: List[Dict]) -> bool:
        if not listings:
            logger.warning("No listings provided for bulk upsert")
            return False

        try:
            collection = self.get_collection(source)
            
            # Validate and clean listings before creating operations
            valid_listings = []
            for idx, listing in enumerate(listings):
                try:
                    if self._validate_listing(listing):
                        # Remove created_at if it exists in the listing
                        created_at = listing.pop('created_at', listing['crawled_at'])
                        # Clean and sanitize data before insertion
                        cleaned_listing = self._clean_listing_data(listing)
                        valid_listings.append({
                            'listing': cleaned_listing,
                            'created_at': created_at
                        })
                    else:
                        logger.warning(f"Invalid listing at index {idx}: {repr(listing.get('source_id', 'unknown'))}")
                except Exception as e:
                    logger.error(f"Error processing listing at index {idx}: {repr(str(e))}")
                    continue

            if not valid_listings:
                logger.error("No valid listings to process")
                return False

            # Create operations in smaller batches
            BATCH_SIZE = 100
            success_count = 0
            error_count = 0

            for i in range(0, len(valid_listings), BATCH_SIZE):
                batch = valid_listings[i:i + BATCH_SIZE]
                batch_operations = []

                for item in batch:
                    try:
                        listing = item['listing']
                        created_at = item['created_at']
                        
                        operation = UpdateOne(
                            {
                                'source': source,
                                'source_id': listing['source_id']
                            },
                            {
                                '$set': listing,
                                '$setOnInsert': {'created_at': created_at}
                            },
                            upsert=True
                        )
                        batch_operations.append(operation)
                    except Exception as e:
                        logger.error(f"Error creating operation for listing {repr(listing.get('source_id', 'unknown'))}: {repr(str(e))}")
                        error_count += 1
                        continue

                if batch_operations:
                    try:
                        result = collection.bulk_write(batch_operations, ordered=False)
                        success_count += result.upserted_count + result.modified_count
                        logger.info(
                            f"Batch {i//BATCH_SIZE + 1} results: "
                            f"Inserted: {result.upserted_count}, "
                            f"Modified: {result.modified_count}, "
                            f"Matched: {result.matched_count}"
                        )
                    except BulkWriteError as bwe:
                        error_details = bwe.details
                        logger.error(f"Bulk write error in batch {i//BATCH_SIZE + 1}: {repr(error_details)}")
                        
                        if 'writeErrors' in error_details:
                            for error in error_details['writeErrors']:
                                error_index = error.get('index', 'unknown')
                                error_code = error.get('code', 'unknown')
                                error_msg = error.get('errmsg', 'unknown')
                                
                                # Get the problematic document
                                if error_index != 'unknown':
                                    actual_index = i + error_index
                                    if actual_index < len(valid_listings):
                                        problem_doc = valid_listings[actual_index]
                                        logger.error(
                                            f"Error at index {error_index} (total index {actual_index}): "
                                            f"Code: {error_code}, "
                                            f"Message: {repr(error_msg)}, "
                                            f"Document: {repr(problem_doc)}"
                                        )
                                
                                error_count += 1
                                
                        # Try to count successful operations from partial success
                        if 'nInserted' in error_details:
                            success_count += error_details['nInserted']
                        if 'nModified' in error_details:
                            success_count += error_details['nModified']
                        continue
                        
                    except Exception as e:
                        logger.error(f"Unexpected error in batch {i//BATCH_SIZE + 1}: {repr(str(e))}")
                        error_count += len(batch_operations)
                        continue

            logger.info(f"Bulk upsert completed. Success: {success_count}, Errors: {error_count}")
            return success_count > 0

        except Exception as e:
            logger.error(f"Fatal error in bulk upsert: {repr(str(e))}", exc_info=True)
            return False

    def _clean_listing_data(self, listing: Dict) -> Dict:
        """Clean and sanitize listing data before insertion"""
        try:
            cleaned = {}
            for key, value in listing.items():
                # Handle string values
                if isinstance(value, str):
                    cleaned[key] = value.strip()
                # Handle nested dictionaries
                elif isinstance(value, dict):
                    cleaned[key] = self._clean_listing_data(value)
                # Handle lists
                elif isinstance(value, list):
                    cleaned[key] = [
                        self._clean_listing_data(item) if isinstance(item, dict)
                        else item.strip() if isinstance(item, str)
                        else item
                        for item in value
                    ]
                # Handle other types
                else:
                    cleaned[key] = value

            return cleaned
        except Exception as e:
            logger.error(f"Error cleaning listing data: {repr(str(e))}")
            return listing  # Return original if cleaning fails

    def get_uncrawled_listings(self, source: str, limit: int = 100) -> List[Dict]:
        try:
            collection = self.get_collection(source)
            results = list(collection.find(
                {'source': source, 'crawled': False},
                {'url': 1, 'source_id': 1}
            ).limit(limit))
            
            logger.info(f"Found {len(results)} uncrawled listings for source: {source}")
            return results
            
        except Exception as e:
            logger.error(f"Error getting uncrawled listings: {repr(str(e))}", exc_info=True)
            return []

    def update_listing_detail(self, source: str, source_id: str, detail_data: Dict) -> bool:
        try:
            if not self._validate_detail_data(detail_data):
                logger.error(f"Invalid detail data for listing {repr(source_id)}")
                return False

            collection = self.get_collection(source)
            result = collection.update_one(
                {'source': source, 'source_id': source_id},
                {'$set': detail_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Successfully updated listing detail for {repr(source_id)}")
                return True
            else:
                logger.warning(f"No listing found to update for {repr(source_id)}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating listing detail for {repr(source_id)}: {repr(str(e))}", exc_info=True)
            return False

    def _validate_listing(self, listing: Dict) -> bool:
        """Validate listing data before insertion"""
        try:
            if not isinstance(listing, dict):
                logger.error(f"Listing is not a dictionary: {type(listing)}")
                return False

            required_fields = ['source', 'source_id', 'crawled_at', 'url']
            
            for field in required_fields:
                if field not in listing:
                    logger.error(f"Missing required field: {field}")
                    return False
                if listing[field] is None:
                    logger.error(f"Required field is None: {field}")
                    return False
                if isinstance(listing[field], str) and not listing[field].strip():
                    logger.error(f"Required field is empty string: {field}")
                    return False

            return True
        except Exception as e:
            logger.error(f"Error validating listing: {repr(str(e))}")
            return False

    def _validate_detail_data(self, detail_data: Dict) -> bool:
        """Validate detail data before update"""
        return bool(detail_data) and isinstance(detail_data, dict)
    
    #deduplicate
    def get_existing_ids(self, source: str, source_ids: list[str]) -> set[str]:
        """
        Checks a list of source_ids against the database and returns a set of IDs that already exist.
        """
        collection = self.get_collection(source)  

        if collection is None:
            logger.warning(f"Collection for source '{source}' not found. Cannot check for existing IDs.")
            return set()
        
        logger.debug(f"[{source}] Checking for {len(source_ids)} existing IDs in the database...")
        start_time = time.time()

        try:
            query = {'source_id': {'$in': source_ids}}
            projection = {'source_id': 1, '_id': 0}
            
            cursor = collection.find(query, projection)
            
            existing_ids = {item['source_id'] for item in cursor}
            
            duration = time.time() - start_time
            logger.debug(f"[{source}] Found {len(existing_ids)} existing IDs in {duration:.2f} seconds.")
            
            return existing_ids
        except Exception as e:
            logger.error(f"[{source}] An error occurred while fetching existing IDs: {e}")
            return set()
        
    def close(self):
        try:
            self.client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {repr(str(e))}", exc_info=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()