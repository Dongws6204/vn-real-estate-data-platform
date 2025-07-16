import argparse
import time
from datetime import datetime
from config.scraper_config import SCRAPER_CONFIG
from utils import deduplicator
from utils.mongodb import MongoDBClient
from utils.data_exporter import DataExporter
from utils.monitoring import ScraperMonitor
from utils.proxy_manager import ProxyManager
from utils.rate_limiter import RateLimiter
from utils.data_validator import PropertyDataValidator
from utils.deduplicator import Deduplicator

from scrapers.raovat321.listing_scraper import RaoVat321ListingScraper
from scrapers.raovat321.detail_scraper import RaoVat321DetailScraper

from scrapers.nhadat24h.listing_scraper import Nhadat24hListingScraper
from scrapers.nhadat24h.detail_scraper import Nhadat24hDetailScraper

from scrapers.cafeland.listing_scraper import CafelandListingScraper
from scrapers.cafeland.detail_scraper import CafelandDetailScraper

from config.logging_config import setup_logger

logger = setup_logger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Property Scraper')
    parser.add_argument('--source', choices=[
            'raovat321',
            'nhadat24h',
            'cafeland',
            'batdongsan',
            'nhadat247',
            'all'
        ],
        default='all', help='Source to scrape')
    parser.add_argument('--max-pages', type=int, default=10,
        help='Maximum number of pages to scrape')
    parser.add_argument('--export-format', choices=['csv', 'json', 'excel'],
        default='csv', help='Export format')
    parser.add_argument('--use-proxies', action='store_true',
        help='Use proxy rotation')
    return parser.parse_args()

def get_scraper_for_source(source: str, config: dict):
    """Factory function to get appropriate scrapers for a source"""
    scrapers = {
        'raovat321': (RaoVat321ListingScraper, RaoVat321DetailScraper),
        'nhadat24h': (Nhadat24hListingScraper, Nhadat24hDetailScraper),
        'cafeland': (CafelandListingScraper, CafelandDetailScraper),
        # Add other scrapers here
    }
    
    if source not in scrapers:
        raise ValueError(f"No scraper implemented for source: {source}")
        
    listing_scraper_class, detail_scraper_class = scrapers[source]
    return listing_scraper_class(config), detail_scraper_class(config)

def main():
    args = parse_arguments()
    
    # Initialize components
    db_client = MongoDBClient()
    data_exporter = DataExporter()
    monitor = ScraperMonitor()
    validator = PropertyDataValidator()
    rate_limiter = RateLimiter()
    
    deduplicator = Deduplicator(db_client)
    
    # Initialize proxy manager if requested
    proxy_manager = ProxyManager() if args.use_proxies else None
    
    sources = [
        'raovat321',
        'nhadat24h',
        'cafeland',
        'batdongsan',
        'nhadat247',
    ] if args.source == 'all' else [args.source]
    
    try:
        for source in sources:
            logger.info(f"Starting scraper for {source}")
            
            # Get source-specific configuration
            if source not in SCRAPER_CONFIG:
                logger.error(f"No configuration found for source: {source}")
                continue
                
            # Update config with command line arguments
            config = SCRAPER_CONFIG[source].copy()
            config['max_pages'] = min(args.max_pages, config['max_pages'])
            
            try:
                # Get appropriate scrapers for the source
                listing_scraper, detail_scraper = get_scraper_for_source(source, config)
                
                # Scrape listings
                listings = listing_scraper.scrape()
                logger.info(f"Found {len(listings)} listings for {source}")
                # Deduplicate listings - level 1
                new_listings = deduplicator.classify_listings(source, listings)['new']
                
                # Validate and clean listings
                valid_listings = []

                for listing in new_listings:
                    is_valid, errors = validator.validate_listing(listing)
                    if is_valid:
                        cleaned_listing = validator.clean_data(listing)
                        valid_listings.append(cleaned_listing)
                    else:
                        logger.warning(f"Invalid listing: {errors}")
                
                logger.info(f"Valid listings: {len(valid_listings)}")
                
                # Store listings in database
                if valid_listings:
                    db_client.bulk_upsert_listings(source, valid_listings)
                
                # Scrape details for each listing
                for listing in valid_listings:
                    try:
                        # Apply rate limiting
                        rate_limiter.wait(source)
                        
                        # Get proxy if enabled
                        proxy = proxy_manager.get_proxy() if proxy_manager else None
                        
                        # Scrape details
                        details = detail_scraper.get_detail(listing['url'])
                        if details:
                            db_client.update_listing_detail(source, listing['source_id'], details)
                            monitor.record_item_scraped()
                            
                    except Exception as e:
                        logger.error(f"Error scraping details for {listing['url']}: {str(e)}")
                        monitor.record_request(False, str(e))
                        continue
                
                # Export data
                all_listings = list(db_client.get_collection(source).find({}))
                if all_listings:
                    if args.export_format == 'csv':
                        export_path = data_exporter.export_to_csv(all_listings)
                    elif args.export_format == 'json':
                        export_path = data_exporter.export_to_json(all_listings)
                    else:
                        export_path = data_exporter.export_to_excel(all_listings)
                        
                    logger.info(f"Data exported to {export_path}")
                
            except Exception as e:
                logger.error(f"Error processing source {source}: {str(e)}")
                continue
            
        # Print final statistics
        summary = monitor.get_summary()
        logger.info("Scraping completed. Summary:")
        logger.info(f"Total items scraped: {summary['items_scraped']}")
        logger.info(f"Success rate: {summary['success_rate']:.2%}")
        logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
    finally:
        db_client.close()

if __name__ == "__main__":
    main()