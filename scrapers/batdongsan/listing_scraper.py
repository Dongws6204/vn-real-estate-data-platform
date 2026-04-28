from datetime import datetime, timedelta
import random
import re
import time
from typing import Dict, List,  Optional
from bs4 import BeautifulSoup

from scrapers.base.web_driver import webDriverManager
from utils.proxy_manager import ProxyManager
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, extract_number, normalize_price
from config.logging_config import setup_logger

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

logger = setup_logger(__name__)

class BatDongSanListingScraper(ListingScraper):
    def scrape(self) -> List[Dict]:
        listings = []
        page = 1
        
        while page <= self.config.get('max_pages', 1):
            url = f"{self.config['base_url']}/p{page}"
            logger.info(f"--- [SCRAPE] Attempting to fetch page {page}: {url} ---")
            
            soup = self.get_page(url)
            
            if not soup:
                logger.error("[SCRAPE] FAILED: get_page() returned None. Stopping.")
                break

            # Lưu lại HTML của trang để kiểm tra
            try:
                html_content = soup.prettify()
                debug_filename = f"debug_page_{page}.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(html_content)
                logger.info(f"[SCRAPE] Saved page content for debugging to: {debug_filename}")
            except Exception as e:
                logger.warning(f"Could not save debug HTML file: {e}")

            if "Verify you are human" in soup.text or "challenges.cloudflare.com" in soup.text:
                logger.error("[SCRAPE] FAILED: Cloudflare challenge detected in page content. Stopping.")
                break

            wait_selector = '#product-lists-web .js__card.js__card-full-web'
            found_elements = soup.select(wait_selector) # Dùng select để đếm
            
            if not found_elements:
                logger.warning(f"[SCRAPE] FAILED: Selector '{wait_selector}' not found on page {page}. This could be the last page or a layout change. Stopping.")
                break
            
            logger.info(f"[SCRAPE] Found {len(found_elements)} potential listing items on page {page}.")
            
            page_listings = self.get_listings(soup)
            if not page_listings:
                logger.warning(f"[SCRAPE] FAILED: get_listings() returned an empty list even though elements were found. Check process_listing selectors.")
                break

            listings.extend(page_listings)
            page += 1
            time.sleep(random.uniform(2, 4))

        logger.info(f"--- [SCRAPE] Finished listing scraping. Total items found: {len(listings)} ---")
        return listings
    # def scrape(self) -> List[Dict]:
    #     listings = []
    #     page = 1
        
    #     while page <= self.config.get('max_pages', 1):
    #         url = f"{self.config['base_url']}/p{page}"
    #         logger.info(f"--- Scraping Listing Page {page}: {url} ---")
            
    #         # Bước 1: Dùng curl_cffi để lấy HTML
    #         soup = self.get_page(url)
            
    #         # Bước 2: Kiểm tra kết quả trả về
    #         if not soup:
    #             logger.warning(f"Failed to retrieve page {page}. Stopping.")
    #             break 

    #         if "Verify you are human" in soup.text or "challenges.cloudflare.com" in soup.text:
    #             logger.error(f"Cloudflare challenge detected on page {page}. Stopping scraper.")
    #             logger.error("Try using a proxy or a different impersonate version in curl_cffi.")
    #             break

    #         wait_selector = '#product-lists-web .js__card.js__card-full-web'
    #         if not soup.select_one(wait_selector):
    #             logger.info(f"No listings found on page {page}. This is likely the last page. Stopping.")
    #             break
                
    #         # Bước 5: Bóc tách dữ liệu
    #         page_listings = self.get_listings(soup)
    #         if not page_listings:
    #             # Dòng này gần như không bao giờ xảy ra vì đã check ở trên
    #             logger.info(f"get_listings returned an empty list for page {page}. Stopping.")
    #             break

    #         listings.extend(page_listings)
    #         page += 1
            
    #         # Nghỉ giữa các trang để không spam server
    #         sleep_time = random.uniform(2, 4)
    #         logger.info(f"Sleeping for {sleep_time:.2f} seconds before next page...")
    #         time.sleep(sleep_time)

    #     logger.info(f"Finished scraping listing pages. Total items found: {len(listings)}")
    #     return listings
    
    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        listing_elements = soup.select('#product-lists-web .js__card.js__card-full-web')
        logger.info(f"Found {len(listing_elements)} listing elements on this page.")
        
        for element in listing_elements:
            try:
                # Giờ chỉ cần truyền `element`
                listing_data = self.process_listing(element)
                if listing_data:
                    listings.append(listing_data)
            except Exception as e:
                logger.error(f"Error processing a listing item: {e}", exc_info=True)
                
        return listings
    
    def process_listing(self, element: BeautifulSoup) -> Optional[Dict]:
        prid = "unknown"
        try:
            product_div = element.select_one('a.js__product-link-for-product-id')
            if not product_div:
                logger.warning("Could not find product link element. Skipping this item.")
                return None

            prid = product_div.get("data-product-id", "unknown")
            
            url_path = product_div.get("href")
            if not url_path or not url_path.startswith('/'):
                logger.warning(f"[prid={prid}] Invalid or missing URL path. Skipping.")
                return None
                
            url = f"https://batdongsan.com.vn{url_path}"
            title = product_div.get("title", "N/A")
            
            img_source = []
            img_tags = product_div.select('div.re__card-image img')
            for img_tag in img_tags:
                    # Lấy src từ 'data-src' (cho lazy loading) hoặc 'src'
                    src = img_tag.get('data-src') or img_tag.get('src')
                    if src:
                        img_source.append(src)

            if img_source: 
                pass
            else: 
                logger.info(f"[prid={prid}] No images found for this listing.")

            card_info_div = product_div.select_one('div.re__card-info div.re__card-info-content')
            if not card_info_div:
                logger.warning(f"[prid={prid}] Could not find card-info-content div. Data might be incomplete.")
                # Có thể return None ở đây nếu thông tin này là bắt buộc
                # return None
        
            price_text = "Liên hệ"
            if card_info_div and (price_span := card_info_div.select_one('span.re__card-config-price')):
                price_text = price_span.get_text(strip=True)
                
            price_data = {'amount': 0, 'currency': 'VND', 'unit': 'total', 'original_text': price_text} if price_text.lower() == 'liên hệ' else self._validate_price(price_text)
            
            location_text = "N/A"
            location_dict = {'district': '', 'city': '', 'full_text': 'N/A'}

            if card_info_div and (location_div := card_info_div.select_one('div.re__card-location')):
                location_text = location_div.get_text(strip=True).strip('· ') # Bỏ dấu ·
                
                parts = [part.strip() for part in location_text.split(',')]
                
                if len(parts) >= 2:
                    location_dict = {
                        'district': parts[0],
                        'city': parts[1],
                        'full_text': location_text
                    }
                elif len(parts) == 1:
                    location_dict = {
                        'district': parts[0],
                        'city': '',
                        'full_text': location_text
                    }
            posted_time_str = None
            published_info_div = element.select_one('.re__card-published-info')
            if published_info_div:
                time_div = published_info_div.select_one('.card-user-info--date-time')
                if time_div:
                    posted_time_str = time_div.get_text(strip=True)
            else:
                logger.warning(f"[prid={prid}] Could not find '.re__card-published-info'. Posted time is missing.")

            # --- BƯỚC 4: Tạo kết quả ---
            listing_data = {
                "source": "batdongsanvn",
                # "source_id": url.split('/')[-1],
                "source_id": prid, 
                "title": clean_text(title),
                "url": url,
                'image_url': img_source, # giữ tên key cũ của bạn
                'price': price_data,
                'posted_time': posted_time_str,
                'categories': '',
                'location': location_dict,
                'is_vip': True, # Cần logic để xác định
                'crawled_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }

            logger.info(f"[prid={prid}] Successfully processed listing.")
            return listing_data

        except Exception as e:
            logger.error(f"[prid={prid}] FATAL ERROR in process_listing: {e}", exc_info=True)
            return None
        
        
    def _validate_price(self, price_text: str) -> Dict:
        price_data = {
            'amount': 0, 'currency': 'VND', 'unit': 'total', 'original_text': price_text
        }
        if not price_text: return price_data
        try:
            clean_price = price_text.lower().replace(' ', '')
            if '/tháng' in clean_price: clean_price = clean_price.replace('/tháng', '')
            amount = extract_number(clean_price)
            if amount is None: return price_data
            if 'tỷ' in clean_price:
                amount *= 1_000_000_000
                price_data['unit'] = 'billion'
            elif 'triệu' in clean_price:
                amount *= 1_000_000
                price_data['unit'] = 'million'
            elif 'nghìn' in clean_price:
                amount *= 1_000
                price_data['unit'] = 'thousand'
            price_data['amount'] = amount
            if '/tháng' in price_text: price_data['period'] = 'monthly'
        except Exception as e:
            logger.error(f"Lỗi phân tích giá {price_text}: {str(e)}", exc_info=True)
        return price_data
    
    def _normalize_date(self, date_str: str) -> Optional[datetime]:
        """Normalize Vietnamese date strings like 'Đăng hôm nay', 'Đăng 3 ngày trước' etc. to datetime object."""

        date_str = date_str.strip().lower()
        today = datetime.today()

        if "hôm nay" in date_str:
            return today

        if "hôm qua" in date_str:
            return today - timedelta(days=1)

        # Match: "Đăng X ngày trước"
        match_days = re.search(r"đăng\s*(\d+)\s*ngày", date_str)
        if match_days:
            days_ago = int(match_days.group(1))
            return today - timedelta(days=days_ago)

        # Match: "Đăng X tuần trước"
        match_weeks = re.search(r"đăng\s*(\d+)\s*tuần", date_str)
        if match_weeks:
            weeks_ago = int(match_weeks.group(1))
            return today - timedelta(weeks=weeks_ago)

        # Fallback: Try common formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        return None