from typing import Dict, List
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, normalize_price, extract_number

# Create logs directory if it doesn't exist
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Create file handler
file_handler = RotatingFileHandler(
    os.path.join(log_directory, 'raovat321_scraper.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

class RaoVat321ListingScraper(ListingScraper):
    def scrape(self) -> List[Dict]:
        listings = []
        page = 1
        
        try:
            while page <= self.config['max_pages']:
                url = f"{self.config['base_url']}?page={page}"
                logger.info(f"Scraping page {page}: {url}")
                
                soup = self.get_page(url)
                if not soup:
                    logger.error(f"Failed to get page {page}")
                    break
                    
                page_listings = self.get_listings(soup)
                if not page_listings:
                    logger.info(f"No listings found on page {page}")
                    break
                    
                listings.extend(page_listings)
                logger.info(f"Found {len(page_listings)} listings on page {page}")
                
                page += 1
                
        except Exception as e:
            logger.error(f"Error in scrape method: {str(e)}", exc_info=True)
            
        logger.info(f"Total listings scraped: {len(listings)}")
        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        try:
            listing_elements = soup.select('.w-full.my-6 div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')
            
            if not listing_elements:
                logger.warning("No listing elements found using primary selector")
                # Fallback selector
                listing_elements = soup.select('div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')
            
            logger.info(f"Found {len(listing_elements)} listing elements")
            
            for element in listing_elements:
                try:
                    listing_data = self.process_listing(element)
                    if listing_data:
                        listings.append(listing_data)
                    else:
                        logger.warning("Failed to process listing element")
                except Exception as e:
                    logger.error(f"Error processing listing: {str(e)}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error in get_listings: {str(e)}", exc_info=True)
            
        return listings

    def process_listing(self, element) -> Dict:
        try:
            # Extract basic information
            main_link = element.select_one('a[href^="/bat-dong-san"]')
            img_element = element.select_one('img.w-full.h-24.md\\:h-auto.object-cover')
            title_container = element.select_one('a.title.md\\:flex.space-x-1.items-center')
            
            if not main_link or not title_container:
                logger.warning("Missing required elements in listing")
                return None
            
            # Build URL
            url = main_link['href']
            if url.startswith('/'):
                url = f"{self.config['base_url'].split('/bat-dong-san')[0]}{url}"
            
            # Extract other information
            is_vip = bool(element.select_one('img[src*="vip.gif"]'))
            price_element = element.select_one('span.text-red-600.font-bold.text-base')
            time_element = element.select_one('span.text-date')
            
            # Get categories and location
            category_container = element.select_one('div.block.md\\:flex.mt-1.text-sm.text-category')
            if category_container:
                category_links = category_container.select('span:first-child a.category-link')
                location_links = category_container.select('span.location a.category-link')
            else:
                logger.warning("Missing category container")
                category_links = []
                location_links = []
            
            # Process price
            price_text = clean_text(price_element.text) if price_element else ''
            if price_text.lower() == 'liên hệ':
                price_data = {
                    'amount': 0,
                    'currency': 'VND',
                    'unit': 'total',
                    'original_text': price_text
                }
            else:
                price_data = self._validate_price(price_text)
            
            # Build listing data
            listing_data = {
                'source': 'raovat321',
                'source_id': url.split('/')[-1],
                'title': clean_text(title_container.text),
                'url': url,
                'image_url': img_element['src'] if img_element and 'src' in img_element.attrs else '',
                'price': price_data,
                'posted_time': clean_text(time_element.text) if time_element else '',
                'categories': [clean_text(link.text) for link in category_links],
                'location': {
                    'district': clean_text(location_links[0].text) if len(location_links) > 0 else '',
                    'city': clean_text(location_links[1].text) if len(location_links) > 1 else '',
                },
                'is_vip': is_vip,
                'crawled': False,
                'crawled_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Add full address
            listing_data['location']['full_address'] = f"{listing_data['location']['district']}, {listing_data['location']['city']}".strip(', ')
            
            return listing_data
            
        except Exception as e:
            logger.error(f"Error parsing listing element: {str(e)}", exc_info=True)
            return None

    def _validate_price(self, price_text: str) -> Dict:
        """Helper method to validate and normalize price"""
        price_data = {
            'amount': 0,
            'currency': 'VND',
            'unit': 'total',
            'original_text': price_text
        }
        
        if not price_text:
            return price_data
            
        try:
            # Process price text
            clean_price = price_text.lower().replace(' ', '')
            
            # Handle monthly rent
            if '/tháng' in clean_price:
                clean_price = clean_price.replace('/tháng', '')
            
            # Extract numeric value
            amount = extract_number(clean_price)
            
            if amount is None:
                logger.warning(f"Could not extract numeric value from price: {price_text}")
                return price_data
                
            # Determine unit and adjust amount
            if 'tỷ' in clean_price:
                amount *= 1000000000
                price_data['unit'] = 'billion'
            elif 'triệu' in clean_price:
                amount *= 1000000
                price_data['unit'] = 'million'
            elif 'nghìn' in clean_price:
                amount *= 1000
                price_data['unit'] = 'thousand'
                
            price_data['amount'] = amount
            
            # Handle rental period
            if '/tháng' in price_text:
                price_data['period'] = 'monthly'
            
        except Exception as e:
            logger.error(f"Error parsing price {price_text}: {str(e)}", exc_info=True)
            
        return price_data