from typing import Dict, List
from bs4 import BeautifulSoup
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, extract_number, normalize_price
from ..base.web_driver import webDriverManager
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class NhaDat247ListingScraper(ListingScraper):
    
    def scrape(self) -> List[Dict]:
        listings = []
        max_pages = self.config.get('max_pages', 1)
        
        for page in range(1, max_pages + 1):
            url = self.config['listings_url'].format(page)
            logger.info(f"Opening url: {url}")
            
            # Sử dụng BaseScraper chung, fetch_fast vì trang có phân trang thường tĩnh, 
            # nếu gặp lỗi Cloudflare hoặc rống thì anh đổi use_js=True để kích hoạt Selenium nhé
            soup = self.get_page(url, use_js=False)
            
            if not soup:
                logger.warning(f"Could not load page {page}")
                break
                
            page_listings = self.get_listings(soup)
            if not page_listings:
                logger.info(f"No listings found on page {page}. Stopping.")
                break
                
            listings.extend(page_listings)
            
            # Tạo khoảng trễ để không bị khoá IP
            time.sleep(self.config.get('delay', 2))

        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        # Support full variations of listing cards depending on the category page
        listing_elements = soup.select('div.re__product-item, div.js__card, div.pr-container')
        logger.info(f"Found {len(listing_elements)} card elements to process.")
        
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
            # Match thẻ link lấy từ nhiều class clone có thể có của web
            link_element = element.select_one('a.js__product-link') or element.select_one('a.js__card-title')
            
            if not link_element:
                return None
                
            title_element = element.select_one('h3.re__card-title') or link_element.select_one('span')
            price_element = element.select_one('.re__card-config-price')
            area_element = element.select_one('.re__card-config-area')
            address_element = element.select_one('.re__card-location')
            
            # Thumbnail extraction from multiple class formats
            thumb_element = element.select_one('img') or element.select_one('.lazy')
            
            if address_element:
                # Lấy text của tất cả thẻ <a> bên trong, bỏ khoảng trắng thừa
                address_parts = [a.get_text(strip=True) for a in address_element.find_all('a')]
                
                # Ghép chúng lại bằng dấu phẩy và khoảng trắng
                full_address = ", ".join(address_parts)
            else:
                address_parts = []
                full_address = ''
            
            # Thumbnail ảnh web hay dùng lazy data-src trước khi thành src
            thumb_element = element.select_one('.lazy') or element.select_one('img')
            
            url = link_element.get('href', '')
            if url and not url.startswith('http'):
                url = f"{self.config['base_url'].rstrip('/')}{url if url.startswith('/') else '/' + url}"
                
            title_text = title_element.text if title_element else link_element.get('title', '')
            
            # Generate source_id from URL segment
            source_id = url.split('-')[-1].split('.')[0] if url else 'unknown'

            
            return {
                'source': 'nhadat247',
                'source_id': source_id,
                'title': clean_text(title_text),
                'url': url,
                'price': normalize_price(price_element.text if price_element else ''),
                'raw_area': clean_text(area_element.text if area_element else ''),
                'location': {
                            'address': full_address,
                            'district': address_parts[-2] if len(address_parts) >= 2 else '', # Quận/Huyện thường ở vị trí áp chót
                            'city': address_parts[-1] if len(address_parts) >= 1 else ''      # Tỉnh/Thành thường ở vị trí cuối cùng
                },
                'address': full_address,
                'thumbnail': thumb_element.get('data-src', thumb_element.get('src', '')) if thumb_element else '',
                'crawled': False,
                'crawled_at': datetime.now()
            }
        except Exception as e:
            logger.error(f"Error parsing NhaDat247 listing element: {str(e)}")
            return None