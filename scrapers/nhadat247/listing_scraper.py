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
        # Chạy Selenium duy nhất 1 lần thay vì gọi URL nhiều lần
        try:
            with webDriverManager(headless=True) as driver:
                url = self.config['base_url']
                logger.info(f"Opening url: {url}")
                driver.get(url)
                
                # Cuộn trang một chút để kích hoạt lazy-load ban đầu
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                
                # Đợi cho tin đăng đầu tiên xuất hiện
                # Bao gồm cả div.re__product-item (theo chuẩn mới) và .js__card / .pr-container 
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.re__product-item'))
                    )
                    logger.info("Listings loaded successfully on initial page nhadat247 load.")
                except Exception as e:
                    logger.warning(f"Timeout waiting for listings on first load {url}")

                max_pages = self.config.get('max_pages', 1)
                max_clicks = max_pages - 1
                
                for i in range(max_clicks):
                    try:
                        # Cuộn trang xuống cuối để tìm nút Mở rộng/Xem thêm
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 500);")
                        time.sleep(1)
                        
                        # Quét tìm nút "Mở rộng" / "Xem thêm" dựa vào text hoặc class
                        btn = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'mở rộng') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'xem thêm')] | //*[@class and contains(@class, 'btn-viewmore')]"))
                        )
                        
                        # Chắc chắn nút nằm trên khung nhìn màn hình
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                        time.sleep(1)
                        
                        # Click bằng javascript
                        driver.execute_script("arguments[0].click();", btn)
                        logger.info(f"Clicked 'Load more' {i+1}/{max_clicks} times")
                        
                        # Quan trọng: Wait sau mỗi lần click để DOM có thời gian render listings mới
                        time.sleep(3)
                        
                    except Exception as e:
                        logger.info(f"Not Found {i+1}: {e}")
                        break
                
                # Sau khi click mở rộng đủ số lần, lấy toàn bộ mã HTML HTML
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # Parse listings
                page_listings = self.get_listings(soup)
                if page_listings:
                    listings.extend(page_listings)
                    
        except Exception as e:
            logger.error(f"Selenium scraper process failed for {url}. Details: {str(e)}")

        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        # Chỉnh sửa chính xác selector của từng card listing thay vì container
        listing_elements = soup.select('div.re__product-item')
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
            if address_element:
                # Lấy text của tất cả thẻ <a> bên trong, bỏ khoảng trắng thừa
                address_parts = [a.get_text(strip=True) for a in address_element.find_all('a')]
                
                # Ghép chúng lại bằng dấu phẩy và khoảng trắng
                full_address = ", ".join(address_parts)
            else:
                full_address = ''
            
            # Thumbnail ảnh web hay dùng lazy data-src trước khi thành src
            thumb_element = element.select_one('img')
            
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