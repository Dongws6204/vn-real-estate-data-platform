import argparse
import os
import time
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

from scrapers.nhadat247.detail_scraper import NhaDat247DetailScraper
from scrapers.nhadat247.listing_scraper import NhaDat247ListingScraper

# Load variables from .env file
load_dotenv()

from config.scraper_config import SCRAPER_CONFIG
from scrapers.batdongsan.detail_scraper import BatDongSanDetailScraper
from scrapers.batdongsan.listing_scraper import BatDongSanListingScraper
from utils.data_exporter import DataExporter
from utils.monitoring import ScraperMonitor
from utils.proxy_manager import ProxyManager
from utils.rate_limiter import RateLimiter
from utils.data_validator import PropertyDataValidator

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
            'batdongsanvn',
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
        'batdongsanvn': (BatDongSanListingScraper, BatDongSanDetailScraper),
        'nhadat247': (NhaDat247ListingScraper, NhaDat247DetailScraper),
        # Add other scrapers here
    }
    
    if source not in scrapers:
        raise ValueError(f"No scraper implemented for source: {source}")
        
    listing_scraper_class, detail_scraper_class = scrapers[source]
    return listing_scraper_class(config), detail_scraper_class(config)

def main():
    args = parse_arguments()
    
    # Kafka Producer Config
    EH_NAMESPACE = os.getenv("EH_NAMESPACE", "default-namespace.servicebus.windows.net:9093")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "default-topic")
    CONNECTION_STRING = os.getenv("EH_CONNECTION_STRING")
    
    conf = {
        'bootstrap.servers': EH_NAMESPACE,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': CONNECTION_STRING,
        'client.id': 'vn-real-estate-scraper',
        'acks': 1
    }
    producer = Producer(conf)

    def delivery_callback(err, msg):
        if err: logger.error(f"Message delivery failed: {err}")
        else: logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] at {msg.offset()}")
    
    # Initialize components
    # deduplicator = Deduplicator()
    data_exporter = DataExporter()
    monitor = ScraperMonitor()
    validator = PropertyDataValidator()
    rate_limiter = RateLimiter()
    
    
    proxy_manager = None
    if args.use_proxies:
        your_proxy_list = [
    "47.251.70.179:1080",
    "47.91.88.100:1080",
    "47.254.47.61:8080",
    "47.252.1.180:3128",
    "103.152.112.145:80",
    "203.89.126.250:80",
    "162.223.94.164:80",
    "64.225.4.85:9991",
    "134.209.29.120:3128",
    "167.71.5.83:8080",
    "45.79.27.210:44554",
    "45.79.142.211:3128",
    "45.79.158.235:44554",
    "192.46.208.26:8080",
    "143.198.182.218:80",
    "139.59.1.14:80",
    "159.203.61.169:3128",
    "161.35.70.249:8080",
    "172.67.182.2:80",
    "172.67.182.8:80",
    "172.67.182.14:80",
    "172.67.182.25:80",
    "172.67.182.28:80",
    "172.67.182.32:80",
    "172.67.182.34:80",
    "172.67.182.35:80",
    "172.67.182.36:80",
    "172.67.182.38:80",
    "172.67.182.40:80",
    "172.67.182.41:80",
    "172.67.182.45:80",
    "172.67.182.47:80",
    "172.67.182.48:80",
    "172.67.182.49:80",
    "172.67.182.51:80",
    "172.67.182.52:80",
    "172.67.182.53:80",
    "172.67.182.55:80",
    "172.67.182.56:80",
    "172.67.182.58:80",
    "172.67.182.59:80",
    "172.67.182.60:80",
    "172.67.182.61:80",
    "172.67.182.62:80",
    "172.67.182.63:80",
    "172.67.182.64:80",
    "172.67.182.67:80",
    "172.67.182.68:80",
    "172.67.182.69:80",
    "172.67.182.71:80",
    "172.67.182.72:80",
    "172.67.182.76:80",
    "172.67.182.77:80",
    "172.67.182.78:80",
    "172.67.182.79:80",
    "172.67.182.83:80",
    "172.67.182.84:80",
    "172.67.182.85:80",
    "172.67.182.88:80",
    "172.67.182.92:80",
    "172.67.182.93:80",
    "172.67.182.94:80",
    "172.67.182.96:80",
    "172.67.182.97:80",
    "172.67.182.98:80",
    "172.67.182.99:80",
    "172.67.182.100:80",
    "172.67.182.102:80",
    "172.67.182.103:80",
    "172.67.182.104:80",
    "172.67.182.105:80",
    "172.67.182.106:80",
    "172.67.182.107:80",
    "172.67.182.109:80",
    "172.67.182.111:80",
    "172.67.182.113:80",
    "172.67.182.114:80",
    "172.67.182.115:80",
    "172.67.182.116:80",
    "172.67.182.117:80",
    "172.67.182.118:80",
    "172.67.182.119:80",
    "172.67.182.120:80",
    "172.67.182.121:80",
    "172.67.182.124:80",
    "172.67.182.125:80",
    "172.67.182.126:80",
    "172.67.182.127:80",
    "172.67.182.128:80",
    "172.67.182.130:80",
    "172.67.182.131:80",
    "172.67.182.132:80",
    "172.67.182.134:80",
    "172.67.182.136:80",
    "172.67.182.137:80",
    "172.67.182.138:80",
    "172.67.182.139:80",
    "172.67.182.140:80",
    "172.67.182.141:80",
    "172.67.182.142:80",
    "172.67.182.143:80",
    "172.67.182.144:80",
    "172.67.182.145:80",
    "172.67.182.146:80",
    "172.67.182.147:80",
    "172.67.182.149:80",
    "172.67.182.150:80",
]

        # Khởi tạo và để nó tự kiểm tra proxy nào sống
        proxy_manager = ProxyManager(proxy_list=your_proxy_list)

    
    sources = [
        'raovat321',
        'nhadat24h',
        'cafeland',
        'batdongsanvn',
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

                valid_listings = []

                for listing in listings:
                    is_valid, errors = validator.validate_listing(listing)
                    if is_valid:
                        cleaned_listing = validator.clean_data(listing)
                        valid_listings.append(cleaned_listing)
                    else:
                        logger.warning(f"Invalid listing: {errors}")
                
                logger.info(f"Valid listings: {len(valid_listings)}")
                
                if source == 'batdongsanvn': 
                    logger.info("Detected 'batdongsanvn' source. Running specialized batch detail scraping.")
            
                    try:
                        # Gọi hàm xử lý hàng loạt của detail scraper
                        full_listings = detail_scraper.scrape_details_in_batch(valid_listings)
                        
                        if full_listings:
                            # Send all results to Kafka
                            for l in full_listings:
                                key = l.get('source_id', str(uuid.uuid4())).encode('utf-8') if l.get('source_id') else None
                                producer.produce(
                                    topic=TOPIC_NAME,
                                    key=key,
                                    value=json.dumps(l, default=str, ensure_ascii=False).encode('utf-8'),
                                    callback=delivery_callback
                                )
                                producer.poll(0)
                        
                            logger.info(f"Successfully sent {len(full_listings)} items to Kafka for {source}.")
                            for _ in full_listings:
                                monitor.record_item_scraped()
                            
                            producer.flush()
                        
                    except Exception as e:
                        logger.error(f"Error during batch processing for {source}: {e}", exc_info=True)
                else:
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
                                listing.update(details)
                                key = listing.get('source_id', str(uuid.uuid4())).encode('utf-8') if listing.get('source_id') else None
                                producer.produce(
                                    topic=TOPIC_NAME,
                                    key=key,
                                    value=json.dumps(listing, default=str, ensure_ascii=False).encode('utf-8'),
                                    callback=delivery_callback
                                )
                                producer.poll(0)
                                monitor.record_item_scraped()
                                
                            time.sleep(2)
                                
                        except Exception as e:
                            logger.error(f"Error scraping details for {listing['url']}: {str(e)}")
                            monitor.record_request(False, str(e))
                            continue
                            
                    producer.flush()
                
                # Cleanup (Dọn dẹp Selenium Driver nếu có khởi tạo)
                if hasattr(listing_scraper, 'close'):
                    listing_scraper.close()
                if hasattr(detail_scraper, 'close'):
                    detail_scraper.close()
                    
                # Export data
                # if valid_listings:
                #     if args.export_format == 'csv':
                #         export_path = data_exporter.export_to_csv(valid_listings)
                #     elif args.export_format == 'json':
                #         export_path = data_exporter.export_to_json(valid_listings)
                #     else:
                #         export_path = data_exporter.export_to_excel(valid_listings)
                        
                #     logger.info(f"Data exported to {export_path}")
                
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

if __name__ == "__main__":
    main()