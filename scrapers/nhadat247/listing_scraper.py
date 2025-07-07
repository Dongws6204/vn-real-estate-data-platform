from typing import Dict, List
from bs4 import BeautifulSoup
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, extract_number, normalize_price
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class NhaDat247ListingScraper(ListingScraper):
    def scrape(self):
        listings = []
        page = 1
        
        while page <= self.config['max_pages']:
            url = f"{self.config['base_url']}/trang-{page}"
            soup = self.get_page(url)
            
            if not soup:
                break
                
            page_listings = self.get_listings(soup)
            if not page_listings:
                break
                
            listings.extend(page_listings)
            page += 1
            
        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        # NhaDat247 uses 'div.item' for listings
        listing_elements = soup.select('div.item')
        
        for element in listing_elements:
            try:
                listing_data = self.process_listing(element)
                if listing_data:
                    listings.append(listing_data)
            except Exception as e:
                logger.error(f"Error processing NhaDat247 listing: {str(e)}")
                
        return listings

    def process_listing(self, element) -> Dict:
        try:
            # Extract basic listing data
            link_element = element.select_one('h3.title a')
            price_element = element.select_one('span.price')
            area_element = element.select_one('span.area')
            address_element = element.select_one('div.address')
            
            # Process the URL
            url = link_element['href']
            if not url.startswith('http'):
                url = f"{self.config['base_url']}{url}"
                
            # Generate source_id from URL
            source_id = url.split('-')[-1].split('.')[0]
            
            return {
                'source': 'nhadat247',
                'source_id': source_id,
                'title': clean_text(link_element.text),
                'url': url,
                'price': normalize_price(price_element.text if price_element else ''),
                'raw_area': clean_text(area_element.text if area_element else ''),
                'address': clean_text(address_element.text if address_element else ''),
                'thumbnail': element.select_one('img.thumb')['src'],
                'crawled': False
            }
        except Exception as e:
            logger.error(f"Error parsing NhaDat247 listing element: {str(e)}")
            return None