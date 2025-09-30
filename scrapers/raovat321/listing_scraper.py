import time
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

from scrapers.base.web_driver import webDriverManager

from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, normalize_price, extract_number

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException,  ElementClickInterceptedException

# Logging setup
log_directory = 'logs'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

file_handler = RotatingFileHandler(
    os.path.join(log_directory, 'raovat321_scraper.log'),
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(console_handler)
logger.addHandler(file_handler)


class RaoVat321ListingScraper(ListingScraper):
    def _get_contact_raovat_logic(self, driver: WebDriver, url: str) -> Dict[str, Optional[str]]:
        contact_info = {'author_name': None, 'contact_phone': None}
        wait = WebDriverWait(driver, 10)

        try: 
            contact_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.contact-box')))
            try: 
                author_element = contact_box.find_element(By.XPATH, ".//div[contains(text(), 'Người liên hệ:')]")
                contact_info['author_name'] = author_element.text.replace('Người liên hệ:', '').strip()
            except NoSuchElementException:
                logger.warning(f"No found: {url}")

            try:
                phone_link_selector = (By.CSS_SELECTOR, "a.bg-success") # Dùng CSS nhanh hơn
                show_button = wait.until(EC.element_to_be_clickable(phone_link_selector))
                
                initial_text = show_button.text
                driver.execute_script("arguments[0].click();", show_button)

                wait.until(lambda d: d.find_element(*phone_link_selector).text != initial_text)

                phone_text = driver.find_element(*phone_link_selector).text.strip()
                
                if phone_text and '***' not in phone_text:
                    contact_info['contact_phone'] = phone_text
                else:
                    logger.warning(f" {url}")

            except (TimeoutException, ElementClickInterceptedException, NoSuchElementException) as e:
                logger.warning(f"Erorr number phone {url}: {type(e).__name__}")
            
        except TimeoutException:
            logger.error(f"No found 'div.contact-box' in {url}")
        
        return contact_info
    

    def scrape(self) -> List[Dict]:
        listings = []
        page = 1

        with webDriverManager(headless=True) as driver:
            logger.info("Starting.")
            try:
                while page <= self.config['max_pages']:
                    url = f"{self.config['base_url']}?page={page}"
                    logger.info(f"Scraping {page}: {url}")

                    soup = self.get_page(url)
                    if not soup:
                        logger.error(f"No get Info {page}")
                        break

                    # Hàm get_listings giờ sẽ nhận thêm `driver`
                    page_listings = self.get_listings(soup, driver)
                    if not page_listings:
                        logger.info(f"No found {page}.")
                        break

                    listings.extend(page_listings)
                    page += 1

            except Exception as e:
                logger.error(f"Error scrape: {str(e)}", exc_info=True)

        logger.info(f"Total: {len(listings)}.")
        return listings

    def get_listings(self, soup: BeautifulSoup, driver: WebDriver) -> List[Dict]:
        listings = []
        try:
            elements = soup.select('.w-full.my-6 div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')
            if not elements:
                elements = soup.select('div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')

            logger.info(f"Found {len(elements)}.")
            for el in elements:
                data = self.process_listing(el, driver)
                if data:
                    listings.append(data)
        except Exception as e:
            logger.error(f"Error in get_listings: {str(e)}", exc_info=True)
        return listings

    def process_listing(self, element: BeautifulSoup, driver: WebDriver) -> Optional[Dict]:
        try:
            main_link = element.select_one('a[href^="/bat-dong-san"]')
            title_container = element.select_one('a.title.md\\:flex.space-x-1.items-center')

            if not main_link or not title_container:
                return None

            url = main_link['href']
            if url.startswith('/'):
                url = f"{self.config['base_url'].split('/bat-dong-san')[0]}{url}"

            logger.info(f"Get phone for: {url}")
            driver.get(url) # Điều khiển trình duyệt tới trang chi tiết
            dynamic_contact = self._get_contact_raovat_logic(driver, url) # Gọi hàm logic
            
            author_name = dynamic_contact.get('author_name')
            contact_phone = dynamic_contact.get('contact_phone')
            contact_info = {
                'name': author_name,
                'phone': contact_phone
            }

            img_element = element.select_one('img.w-full.h-24.md\\:h-auto.object-cover')
            is_vip = bool(element.select_one('img[src*="vip.gif"]'))
            price_element = element.select_one('span.text-red-600.font-bold.text-base')
            time_element = element.select_one('span.text-date')
            category_container = element.select_one('div.block.md\\:flex.mt-1.text-sm.text-category')
            category_links = category_container.select('span:first-child a.category-link') if category_container else []
            location_links = category_container.select('span.location a.category-link') if category_container else []
            price_text = clean_text(price_element.text) if price_element else ''
            price_data = {'amount': 0, 'currency': 'VND', 'unit': 'total', 'original_text': price_text} if price_text.lower() == 'liên hệ' else self._validate_price(price_text)
            listing_data = {
                'source': 'raovat321',
                'source_id': url.split('/')[-1],
                'title': clean_text(title_container.text),
                'url': url,
                'image_url': img_element['src'] if img_element and 'src' in img_element.attrs else '',
                'price': price_data,
                'posted_time': clean_text(time_element.text) if time_element else '',
                'contact_info': contact_info,
                'categories': [clean_text(link.text) for link in category_links],
                'location': {
                    'district': clean_text(location_links[0].text) if len(location_links) > 0 else '',
                    'city': clean_text(location_links[1].text) if len(location_links) > 1 else ''
                },
                'is_vip': is_vip,
                'crawled': False,
                'crawled_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            listing_data['location']['full_address'] = f"{listing_data['location']['district']}, {listing_data['location']['city']}".strip(', ')
            return listing_data

        except Exception as e:
            logger.error(f"Lỗi xử lý tin đăng: {str(e)}", exc_info=True)
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
    
    # def _get_dynamic_contact(self, url: str) -> Dict[str, Optional[str]]:
    #     return self._execute_selenium_task(url, self._get_contact_raovat_logic)

    # def scrape(self) -> List[Dict]:
    #     listings = []
    #     page = 1

    #     try:
    #         while page <= self.config['max_pages']:
    #             url = f"{self.config['base_url']}?page={page}"
    #             logger.info(f"Scraping page {page}: {url}")

    #             soup = self.get_page(url)
    #             if not soup:
    #                 logger.error(f"Failed to get page {page}")
    #                 break

    #             page_listings = self.get_listings(soup)
    #             if not page_listings:
    #                 logger.info(f"No listings found on page {page}")
    #                 break

    #             listings.extend(page_listings)
    #             page += 1

    #     except Exception as e:
    #         logger.error(f"Error in scrape method: {str(e)}", exc_info=True)

    #     logger.info(f"Total listings scraped: {len(listings)}")
    #     return listings

    # def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
    #     listings = []
    #     try:
    #         elements = soup.select('.w-full.my-6 div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')
    #         if not elements:
    #             elements = soup.select('div.flex.relative.space-x-4.my-4.p-2.border.border-gray-200')

    #         logger.info(f"Found {len(elements)} listings")
    #         for el in elements:
    #             data = self.process_listing(el)
    #             if data:
    #                 listings.append(data)

    #     except Exception as e:
    #         logger.error(f"Error in get_listings: {str(e)}", exc_info=True)

    #     return listings

    # def process_listing(self, element) -> Dict:
    #     try:
    #         main_link = element.select_one('a[href^="/bat-dong-san"]')
    #         img_element = element.select_one('img.w-full.h-24.md\\:h-auto.object-cover')
    #         title_container = element.select_one('a.title.md\\:flex.space-x-1.items-center')

    #         if not main_link or not title_container:
    #             return None

    #         url = main_link['href']
    #         if url.startswith('/'):
    #             url = f"{self.config['base_url'].split('/bat-dong-san')[0]}{url}"

    #         # NEW: Get author and phone using Selenium
    #         dynamic_contact = self._get_dynamic_contact(url)
    #         author_name = dynamic_contact.get('author_name')
    #         contact_phone = dynamic_contact.get('contact_phone')

    #         is_vip = bool(element.select_one('img[src*="vip.gif"]'))
    #         price_element = element.select_one('span.text-red-600.font-bold.text-base')
    #         time_element = element.select_one('span.text-date')

    #         category_container = element.select_one('div.block.md\\:flex.mt-1.text-sm.text-category')
    #         category_links = category_container.select('span:first-child a.category-link') if category_container else []
    #         location_links = category_container.select('span.location a.category-link') if category_container else []

    #         price_text = clean_text(price_element.text) if price_element else ''
    #         price_data = {
    #             'amount': 0,
    #             'currency': 'VND',
    #             'unit': 'total',
    #             'original_text': price_text
    #         } if price_text.lower() == 'liên hệ' else self._validate_price(price_text)

    #         listing_data = {
    #             'source': 'raovat321',
    #             'source_id': url.split('/')[-1],
    #             'title': clean_text(title_container.text),
    #             'url': url,
    #             'image_url': img_element['src'] if img_element and 'src' in img_element.attrs else '',
    #             'price': price_data,
    #             'posted_time': clean_text(time_element.text) if time_element else '',
    #             'author_name': author_name,
    #             'contact_phone': contact_phone,
    #             'categories': [clean_text(link.text) for link in category_links],
    #             'location': {
    #                 'district': clean_text(location_links[0].text) if len(location_links) > 0 else '',
    #                 'city': clean_text(location_links[1].text) if len(location_links) > 1 else ''
    #             },
    #             'is_vip': is_vip,
    #             'crawled': False,
    #             'crawled_at': datetime.now(),
    #             'created_at': datetime.now(),
    #             'updated_at': datetime.now()
    #         }

    #         listing_data['location']['full_address'] = f"{listing_data['location']['district']}, {listing_data['location']['city']}".strip(', ')
    #         return listing_data

    #     except Exception as e:
    #         logger.error(f"Error processing listing: {str(e)}", exc_info=True)
    #         return None

    # def _validate_price(self, price_text: str) -> Dict:
    #     price_data = {
    #         'amount': 0,
    #         'currency': 'VND',
    #         'unit': 'total',
    #         'original_text': price_text
    #     }

    #     if not price_text:
    #         return price_data

    #     try:
    #         clean_price = price_text.lower().replace(' ', '')
    #         if '/tháng' in clean_price:
    #             clean_price = clean_price.replace('/tháng', '')

    #         amount = extract_number(clean_price)
    #         if amount is None:
    #             return price_data

    #         if 'tỷ' in clean_price:
    #             amount *= 1_000_000_000
    #             price_data['unit'] = 'billion'
    #         elif 'triệu' in clean_price:
    #             amount *= 1_000_000
    #             price_data['unit'] = 'million'
    #         elif 'nghìn' in clean_price:
    #             amount *= 1_000
    #             price_data['unit'] = 'thousand'

    #         price_data['amount'] = amount
    #         if '/tháng' in price_text:
    #             price_data['period'] = 'monthly'

    #     except Exception as e:
    #         logger.error(f"Error parsing price {price_text}: {str(e)}", exc_info=True)

    #     return price_data
