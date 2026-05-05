from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from datetime import datetime
import time
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, extract_number
from ..base.web_driver import webDriverManager
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class CafelandListingScraper(ListingScraper):
    def scrape(self) -> List[Dict]:
        listings = []
        page = 1
        
        try:
            while page <= self.config['max_pages']:
                url = f"{self.config['listings_url']}page-{page}"
                logger.info(f"Scraping page {page}: {url}")
                
                # Dùng thuộc tính get_page của BaseScraper (mặc định lấy bằng curl_cffi tốc độ cao)
                soup = self.get_page(url, use_js=False)
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
            logger.error(f"Error in scrape method: {str(e)}")
            
        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        try:
            listing_elements = soup.find_all('div', class_='row-item')
            
            for element in listing_elements:
                try:
                    listing_data = self.process_listing(element)
                    if listing_data:
                        listings.append(listing_data)
                except Exception as e:
                    logger.error(f"Error processing listing: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error in get_listings: {str(e)}")
            
        return listings

    def process_listing(self, element) -> Dict:
        try:
            # Get title and URL
            title_elem = element.find('a', class_='realTitle')
            if not title_elem or 'href' not in title_elem.attrs:
                logger.warning("Missing title or URL")
                return None
                
            url = title_elem['href']
            # Extract source_id from URL (e.g., from "some-title-2261155.html")
            source_id = url.split('-')[-1].replace('.html', '')
            if not source_id.isdigit():
                logger.warning(f"Invalid source_id format: {source_id}")
                return None

            # Ensure URL is absolute
            if not url.startswith('http'):
                url = f"https://nhadat.cafeland.vn{url}"

            # Get price and area
            info_property = element.find('div', class_='info-property')
            price_elem = info_property.find('span', class_='reales-price') if info_property else None
            area_elem = info_property.find('span', class_='reales-area') if info_property else None
            
            # Get location
            location_elem = element.find('div', class_='info-location')
            
            # Get description
            desc_elem = element.find('div', class_='reales-preview')
            
            # Get contact info
            contact_elem = element.find('div', class_='profile-member')
            contact_name = contact_elem.find('a', class_='member-name') if contact_elem else None
            contact_time = contact_elem.find('div', class_='reals-update-time') if contact_elem else None
            
            # Get images
            images = []
            img_elements = element.find_all('img', class_='lazyload')
            for img in img_elements:
                if 'src' in img.attrs and img['src'].startswith('http'):
                    images.append(img['src'])

            # Build listing data
            listing_data = {
                'source': 'cafeland',
                'source_id': source_id,
                'url': url,
                'title': clean_text(title_elem.text) if title_elem else None,
                'price': self._parse_price(price_elem.text) if price_elem else None,
                'area': extract_number(area_elem.text) if area_elem else None,
                'location': clean_text(location_elem.text) if location_elem else None,
                'description': clean_text(desc_elem.text) if desc_elem else None,
                'contact_info': {
                    'name': clean_text(contact_name.text) if contact_name else None,
                    'update_time': clean_text(contact_time.text) if contact_time else None
                },
                'media': {
                    'images': images,
                    'videos': []
                },
                'is_vip': bool(element.find('div', class_='reals-typevip')),
                'is_verified': bool(element.find('div', class_='reals-uytin')),
                'crawled': False,
                'crawled_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }

            # Validate required fields
            required_fields = ['source', 'source_id', 'url']
            for field in required_fields:
                if not listing_data.get(field):
                    logger.warning(f"Missing required field: {field}")
                    return None

            # Validate URL format
            if not listing_data['url'].startswith('http'):
                logger.warning(f"Invalid URL format: {listing_data['url']}")
                return None
            
            return listing_data
            
        except Exception as e:
            logger.error(f"Error parsing listing element: {str(e)}")
            return None

    def _parse_price(self, price_text: str) -> Dict:
        """Helper method to parse price text into structured data"""
        price_data = {
            'amount': 0,
            'currency': 'VND',
            'unit': 'total',
            'original_text': price_text
        }
        
        try:
            if not price_text or 'thương lượng' in price_text.lower():
                return price_data
                
            clean_price = price_text.lower().replace(' ', '')
            
            # Extract numeric value
            amount = extract_number(clean_price)
            
            if amount is None:
                return price_data
                
            # Determine unit and adjust amount
            if 'tỷ' in clean_price:
                amount *= 1000000000 
                price_data['unit'] = 'billion'
            elif 'triệu' in clean_price:
                amount *= 1000000
                price_data['unit'] = 'million'
            
            price_data['amount'] = amount
            
        except Exception as e:
            logger.error(f"Error parsing price {price_text}: {str(e)}")
            
        return price_data