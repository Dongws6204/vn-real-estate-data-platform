import json
import os
import random
import re
import time
import traceback
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse, parse_qs, unquote
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import sys

from scrapers.base.web_driver import webDriverManager
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number, parse_date
from config.logging_config import setup_logger
from utils.proxy_manager import ProxyManager
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException,  StaleElementReferenceException    


logger = setup_logger(__name__)

PHONE_REGEX = re.compile(r'(\+?\d[\d\-\s\.\(\)]{6,}\d|\d{2,3}[\s.-]\d{3}[\s.-]\d{3,4})')

def parse_coords_from_url(url: str):
    if not url:
        return None
    u = unquote(url)  # decode %2C etc
    parsed = urlparse(u)
    qs = parse_qs(parsed.query)

    # 1) common embed: .../embed/v1/place?q=LAT,LON&...
    if 'q' in qs:
        qv = qs['q'][0]
        m = re.match(r'\s*([+-]?\d+(?:\.\d+)?),\s*([+-]?\d+(?:\.\d+)?)', qv)
        if m:
            return float(m.group(1)), float(m.group(2)), 'q_param'

    # 2) look for @LAT,LON in path (typical maps URLs)
    m = re.search(r'@([+-]?\d+(?:\.\d+)?),\s*([+-]?\d+(?:\.\d+)?)', u)
    if m:
        return float(m.group(1)), float(m.group(2)), '@pattern'

    # 3) old-style google tokens: !3dLAT!4dLON
    m = re.search(r'!3d([+-]?\d+(?:\.\d+)?)!4d([+-]?\d+(?:\.\d+)?)', u)
    if m:
        return float(m.group(1)), float(m.group(2)), '!3d4d'

    # 4) any simple "lat,lon" sequence in url string
    m = re.search(r'([+-]?\d{1,3}\.\d+)[,_\s]+([+-]?\d{1,3}\.\d+)', u)
    if m:
        return float(m.group(1)), float(m.group(2)), 'generic_decimal'

    return None

_DMS_PATTERN = re.compile(r"""
        (?P<dir_prefix>[NSEW])?\s*          # Hướng có thể đứng trước
        (?P<deg>-?\d+(?:\.\d+)?)\s*°?\s*    # Độ (có thể âm, có thể thiếu °)
        (?:(?P<min>\d+(?:\.\d+)?)\s*['’′m]?\s*)?   # Phút (optional)
        (?:(?P<sec>\d+(?:\.\d+)?)\s*["”″s]?\s*)?   # Giây (optional)
        (?P<dir_suffix>[NSEW])?             # Hướng có thể đứng sau
    """, re.VERBOSE | re.IGNORECASE)

class BatDongSanDetailScraper(DetailScraper):
    def get_detail(self, url):
        return {}
    
    def scrape_details_in_batch(self, listings_to_enrich: List[Dict]) -> List[Dict]:
        enriched_listings = []
        for i, listing_item in enumerate(listings_to_enrich):
            url = listing_item.get('url')
            if not url: continue
            
            logger.info(f"--- Processing detail {i+1}/{len(listings_to_enrich)}: {url} ---")
            soup = self.get_page(url)
            
            if soup:
                detail_data = self.process_detail(soup)
                # Gộp dữ liệu cũ và mới
                combined_data = {**listing_item, **detail_data}
                enriched_listings.append(combined_data)
            
            time.sleep(random.uniform(1, 3)) # Nghỉ nhẹ giữa các request

        logger.info(f"DetailScraper finished. Enriched {len(enriched_listings)} items.")
        return enriched_listings
    
    # SELENIUM
#     def scrape_details_in_batch(self, listings_to_enrich: List[Dict]) -> List[Dict]:
#     """
#     Enriches a list of listings with detailed information.
#     Opens WebDriver once and reuses it for all URLs for maximum performance.
#     """
#     enriched_listings = []
    
#     # Mở driver MỘT LẦN DUY NHẤT cho toàn bộ quá trình
#     with webDriverManager(headless=False, use_undetected=True) as driver:
#         logger.info(f"BDS Detail Scraper: WebDriver initialized. Starting to enrich {len(listings_to_enrich)} items.")
        
#         # Tạo "chuyên gia" giải CAPTCHA, tái sử dụng cho tất cả các trang
#         solver = CloudflareSolver(driver)
        
#         for i, listing_item in enumerate(listings_to_enrich):
#             url = listing_item.get('url')
#             if not url:
#                 continue
            
#             logger.info(f"--- Processing detail {i+1}/{len(listings_to_enrich)}: {url} ---")
            
#             # Gọi hàm con để xử lý MỘT URL, truyền driver và solver vào
#             detail_data = self._get_single_detail(driver, solver, url)
            
#             if detail_data:
#                 # Gộp dữ liệu cũ (từ listing) và dữ liệu mới (từ detail)
#                 combined_data = {**listing_item, **detail_data}
#                 enriched_listings.append(combined_data)
            
#             # Nghỉ giữa các lần cào chi tiết để tránh bị ban
#             time.sleep(random.uniform(4, 7))
            
#     logger.info(f"BDS Detail Scraper: Finished. Successfully enriched {len(enriched_listings)} items.")
#     return enriched_listings

# # HÀM CON THỰC THI (logic xử lý một URL)
# def _get_single_detail(self, driver: WebDriver, solver: CloudflareSolver, url: str) -> Optional[Dict]:
#     """
#     The core logic for scraping a single detail page using a pre-existing WebDriver.
#     """
#     try:
#         driver.get(url)
        
#         # Gọi chuyên gia giải quyết CAPTCHA
#         if not solver.solve_captcha(dry_run_click=False): # Chạy thật
#             logger.error(f"Could not bypass CAPTCHA for detail page {url}. Skipping item.")
#             return None

#         # Chờ trang load xong
#         WebDriverWait(driver, 20).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, "h1.re__pr-title"))
#         )
#         soup = BeautifulSoup(driver.page_source, "html.parser")
        
#         # Gọi hàm bóc tách cuối cùng
#         return self.process_detail(driver, soup, url)
        
#     except Exception as e:
#         logger.error(f"Failed to get or process detail page for {url}: {e}", exc_info=True)
#         driver.save_screenshot(f"debug_detail_FAIL_{url.split('/')[-1]}.png")
#         return None


    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            detail_data = {
                'features': self.extract_features(soup),
                'description': self.extract_description(soup),
                'contact_info': self.extract_contact_info(soup),
                'location': self.extract_location(soup),
                'media': self.extract_media(soup),
                # 'metadata': self.extract_metadata(soup),
                'time': self.extract_time(soup),
                'map': self.extract_map(soup),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            return detail_data
        except Exception as e:
            logger.error(f"Error processing BDS detail: {str(e)}")
            return None

    def extract_features(self, soup: BeautifulSoup) -> Dict:
        features = {}
        try:
            # Tìm tất cả các item trong phần thông tin chi tiết
            item_divs = soup.select('div.re__pr-other-info-display div.re__pr-specs-content-item')
            for item in item_divs:
                label_tag = item.select_one('span.re__pr-specs-content-item-title')
                value_tag = item.select_one('span.re__pr-specs-content-item-value')

                if label_tag and value_tag:
                    label = label_tag.get_text(strip=True)
                    value = value_tag.get_text(strip=True)
                    features[label] = value 
        except Exception as e:
            logger.error(f"Error extracting BDS features: {str(e)}")
        return features
    
    def extract_description(self, soup: BeautifulSoup) -> str:
        try:
            desc_element = soup.select_one(
                'div.re__section-body.re__detail-content.js__section-body.js__pr-description.js__tracking'
            )
            if not desc_element:
                return ''

            lines = []
            for content in desc_element.contents:
                if isinstance(content, Tag) and content.name == 'br':
                    lines.append('\n')
                elif isinstance(content, str):
                    lines.append(content.strip())
                elif isinstance(content, Tag):
                    lines.append(content.get_text(strip=True))

            return clean_text(''.join(lines))
        
        except Exception as e:
            logger.error(f"Error extracting BDS description: {str(e)}")
            return ''

    def extract_time(self, soup: BeautifulSoup) -> dict:
        time = {}
        try:
            # Chọn tất cả các item chứa label + value
            time_items = soup.select('div.re__pr-short-info-item.js__pr-config-item')
            
            for i, item in enumerate(time_items):
                label_tag = item.select_one('span.title')
                value_tag = item.select_one('span.value')
                if label_tag and value_tag:
                    label = label_tag.get_text(strip=True)
                    value = value_tag.get_text(strip=True)
                    time[label] = value
                # logger.info(f"Processed time item {i + 1}")
            
            return time
        except Exception as e:
            logger.error(f"Error extracting BDS time: {str(e)}")
            return {}
    def extract_location(self, soup: BeautifulSoup) -> Dict:
        location = {}
        try:
            location_tag = soup.select_one('span.re__pr-short-description.js__pr-address')
            location_text = location_tag.get_text(strip=True) if location_tag else ''

            parts = [part.strip() for part in location_text.split(',')]

            location['ward'] = parts[1] if len(parts) > 1 else None
            location['district'] = parts[2] if len(parts) > 2 else None
            location['city'] = parts[3] if len(parts) > 3 else None
            location['full_address'] = location_text

            return location
        except Exception as e:
            logger.error(f"Error extracting BDS location: {str(e)}")
            return {}

    def extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        contact_info = {
            'contact_name': None,
            'contact_phone': None,
            'contact_email': None,
            'raw_phone_hash': None
        }

        try:
            contact_tag = soup.select_one(
                'div.re__btn.re__btn-cyan-solid--md.re__link-phone.re__with-zalo.phone.js__phone.phoneEvent.js__phone-event.js__phone-event-tablet'
    )

            if contact_tag:
                # Lấy tên người đăng từ data-kyc-name
                contact_info['contact_name'] = contact_tag.get('data-kyc-name')

                # Lấy mã hash raw (mã định danh số điện thoại ẩn)
                raw_phone = contact_tag.get('raw')
                if raw_phone:
                    contact_info['raw_phone_hash'] = raw_phone

            # Lấy phần hiện ra trên giao diện (dù là số bị ẩn: 0966 399 ***)
            phone_display_tag = soup.select_one('span.re__content span')
            if phone_display_tag:
                contact_info['contact_phone'] = phone_display_tag.get_text(strip=True)

        except Exception as e:
            logger.error(f"Error extracting contact info: {str(e)}", exc_info=True)
            return contact_info
        return contact_info


    def extract_media(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        media = {'images': [], 'videos': []}
        try:
            image_tags = soup.select('li.swiper-slide img')
            for img in image_tags:
                url = None
                # Ưu tiên các thuộc tính lazy loading
                for attr in ('data-src', 'data-srcset', 'srcset', 'src'):
                    if img.has_attr(attr):
                        val = (img.get(attr) or '').strip()
                        if not val:
                            continue
                        if attr in ('srcset', 'data-srcset'):
                            url = self._pick_from_srcset(val)
                        else:
                            url = val
                            media['images'].append(url)
                            break 
        except Exception as e:
            logger.error(f"Error extracting BDS media: {str(e)}", exc_info=True)
            
        return media

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {}
        try:
            date_element = soup.select_one('span.update-time')
            if date_element:
                metadata['posted_date'] = parse_date(clean_text(date_element.text))
            
            type_element = soup.select_one('span.property-type')
            if type_element:
                metadata['property_type'] = clean_text(type_element.text)

            project_element = soup.select_one('span.project-name')
            if project_element:
                metadata['project'] = clean_text(project_element.text)
                
        except Exception as e:
            logger.error(f"Error extracting BDS metadata: {str(e)}")
        return metadata
        
    def extract_map(self, soup: BeautifulSoup) -> dict:
        """
        Nhận đầu vào là BeautifulSoup soup (HTML tĩnh).
        Trả về dict: {"coordinates": str_or_None, "lat": float|None, "lon": float|None,
                    "raw_url": str|None, "method": str|None}
        Ghi chú: nếu iframe src được thêm bằng JS sau khi render, hàm này sẽ *không* thấy được.
        """
        map_data = {"coordinates": None, "lat": None, "lon": None, "raw_url": None, "method": None}

        # helper: cố chuyển str->float
        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return None

        # fallback parser from URL (dành cho trường hợp parse_coords_from_url không tồn tại)
        def _parse_coords_from_url_fallback(url: str) -> Optional[Tuple[float, float, str]]:
            if not url:
                return None
            # @lat,lon pattern (Google maps /@lat,lon,...)
            m = re.search(r'@([+-]?\d+(?:\.\d+)),\s*([+-]?\d+(?:\.\d+))', url)
            if m:
                return float(m.group(1)), float(m.group(2)), "url_at"
            # q=lat,lon or ?q=lat,lon
            m = re.search(r'[?&]q=([+-]?\d+(?:\.\d+)),\s*([+-]?\d+(?:\.\d+))', url)
            if m:
                return float(m.group(1)), float(m.group(2)), "url_q"
            # pattern !3dLAT!4dLON
            m = re.search(r'!3d([+-]?\d+(?:\.\d+)?)!4d([+-]?\d+(?:\.\d+)?)', url)
            if m:
                return float(m.group(1)), float(m.group(2)), "url_3d4d"
            # generic lat,lon anywhere (careful but last resort)
            m = re.search(r'([+-]?\d{1,3}(?:\.\d+))[,;\s]+([+-]?\d{1,3}(?:\.\d+))', url)
            if m:
                return float(m.group(1)), float(m.group(2)), "url_generic"
            return None

        def _try_parse_coords_from_url(url: str):
            # prefer user-defined parse_coords_from_url if available
            try:
                parsed = parse_coords_from_url(url)  # may raise NameError if not defined
                if parsed:
                    return parsed  # expected: (lat, lon, method)
            except Exception:
                pass
            return _parse_coords_from_url_fallback(url)

        # 1) iframe tags (src or data-src)
        iframes = soup.select("iframe[src*='google.com/maps'], iframe[data-src*='google.com/maps'], iframe[src*='maps.google.com'], iframe[data-src*='maps.google.com']")
        for iframe in iframes:
            url = iframe.get('data-src') or iframe.get('src')
            parsed = _try_parse_coords_from_url(url)
            if parsed:
                lat, lon, method = parsed
                map_data.update({
                    "coordinates": f"{lat},{lon}",
                    "lat": _to_float(lat),
                    "lon": _to_float(lon),
                    "raw_url": url,
                    "method": f"iframe_url:{method}"
                })
                return map_data

        # 2) direct <a href> to maps (google, osm, bing, etc.)
        anchors = soup.select("a[href*='google.com/maps'], a[href*='maps.google.com'], a[href*='openstreetmap.org'], a[href*='bing.com/maps'], a[href^='geo:']")
        for a in anchors:
            href = a.get('href')
            # geo:lat,lon
            if href and href.startswith("geo:"):
                m = re.match(r'geo:([+-]?\d+(?:\.\d+)),\s*([+-]?\d+(?:\.\d+))', href)
                if m:
                    lat, lon = float(m.group(1)), float(m.group(2))
                    map_data.update({"coordinates": f"{lat},{lon}", "lat": lat, "lon": lon, "raw_url": href, "method": "geo_link"})
                    return map_data
            parsed = _try_parse_coords_from_url(href)
            if parsed:
                lat, lon, method = parsed
                map_data.update({"coordinates": f"{lat},{lon}", "lat": _to_float(lat), "lon": _to_float(lon), "raw_url": href, "method": f"anchor_url:{method}"})
                return map_data

        # 3) data-* attributes (data-lat, data-lng, etc.)
        candidates = soup.select("[data-lat],[data-lng],[data-lon],[data-long],[data-latitude],[data-longitude]")
        for el in candidates:
            lat = el.get('data-lat') or el.get('data-latitude')
            lon = el.get('data-lng') or el.get('data-long') or el.get('data-longitude') or el.get('data-lon')
            if lat and lon:
                latf = _to_float(lat)
                lonf = _to_float(lon)
                if latf is not None and lonf is not None:
                    map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "data_attrs"})
                    return map_data

        # 4) meta tags like geo.position, ICBM or geo.position content="lat;lon" or "lat,lon"
        meta = None
        for name in ("geo.position", "ICBM", "geo.region", "place:location:latitude"):
            tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
            if tag and tag.get("content"):
                content = tag["content"]
                parts = re.split(r'[;,]\s*', content)
                if len(parts) >= 2:
                    latf = _to_float(parts[0]); lonf = _to_float(parts[1])
                    if latf is not None and lonf is not None:
                        map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": f"meta:{name}"})
                        return map_data

        # 5) JSON-LD <script type="application/ld+json"> -> look for GeoCoordinates or geo keys
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                payload = json.loads(s.string or "{}")
            except Exception:
                continue
            # payload might be dict or list
            objs = payload if isinstance(payload, list) else [payload]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                # if object has 'geo' or '@type': 'GeoCoordinates'
                if "geo" in obj and isinstance(obj["geo"], dict):
                    lat = obj["geo"].get("latitude") or obj["geo"].get("lat")
                    lon = obj["geo"].get("longitude") or obj["geo"].get("lon")
                    latf = _to_float(lat); lonf = _to_float(lon)
                    if latf is not None and lonf is not None:
                        map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "jsonld_geo"})
                        return map_data
                # direct GeoCoordinates object
                if obj.get("@type") in ("GeoCoordinates", "Place") or "latitude" in obj or "longitude" in obj:
                    lat = obj.get("latitude") or obj.get("lat")
                    lon = obj.get("longitude") or obj.get("lon")
                    latf = _to_float(lat); lonf = _to_float(lon)
                    if latf is not None and lonf is not None:
                        map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "jsonld_direct"})
                        return map_data
                # sometimes nested under 'mainEntity' or 'address'
                # try shallow search
                def _search_dict_for_coords(d):
                    if not isinstance(d, dict):
                        return None
                    if "geo" in d and isinstance(d["geo"], dict):
                        g = d["geo"]
                        la = g.get("latitude") or g.get("lat")
                        lo = g.get("longitude") or g.get("lon")
                        if la and lo:
                            return _to_float(la), _to_float(lo)
                    for k, v in d.items():
                        if isinstance(v, dict):
                            res = _search_dict_for_coords(v)
                            if res:
                                return res
                    return None
                res = _search_dict_for_coords(obj)
                if res:
                    latf, lonf = res
                    if latf is not None and lonf is not None:
                        map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "jsonld_nested"})
                        return map_data

        # 6) search page text for decimal coordinates (most common)
        text = soup.get_text(separator=" ", strip=True)
        # decimal pair like "16.0738, 108.1664" or "16.0738 108.1664"
        m = re.search(r'([+-]?\d{1,3}(?:\.\d+))[,;\s]+([+-]?\d{1,3}(?:\.\d+))', text)
        if m:
            latf = _to_float(m.group(1)); lonf = _to_float(m.group(2))
            # quick sanity check ranges
            if latf is not None and lonf is not None and -90 <= latf <= 90 and -180 <= lonf <= 180:
                map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "page_text_decimal"})
                return map_data

        # 7) try DMS pattern e.g. 16° 04' 23.4" N 108° 12' 07.3" E
        dms_matches = re.findall(r'(\d{1,3})°\s*(\d{1,2})[\'’]?\s*(\d{1,2}(?:\.\d+)?)"?\s*([NnSs])', text)
        dms_matches_lon = re.findall(r'(\d{1,3})°\s*(\d{1,2})[\'’]?\s*(\d{1,2}(?:\.\d+)?)"?\s*([EeWw])', text)
        if len(dms_matches) >= 1 and len(dms_matches_lon) >= 1:
            try:
                def _dms_to_dec(d, m, s, hemi):
                    dd = float(d) + float(m)/60.0 + float(s)/3600.0
                    if hemi.upper() in ("S", "W"):
                        dd = -abs(dd)
                    return dd
                lat_parts = dms_matches[0]
                lon_parts = dms_matches_lon[0]
                latf = _dms_to_dec(lat_parts[0], lat_parts[1], lat_parts[2], lat_parts[3])
                lonf = _dms_to_dec(lon_parts[0], lon_parts[1], lon_parts[2], lon_parts[3])
                if -90 <= latf <= 90 and -180 <= lonf <= 180:
                    map_data.update({"coordinates": f"{latf},{lonf}", "lat": latf, "lon": lonf, "raw_url": None, "method": "page_text_dms"})
                    return map_data
            except Exception:
                pass

        # nothing found
        return map_data
    # def extract_map(self, driver) -> dict:
    #     """
    #     Trả về dict: {"coordinates": str_or_None, "lat": float|None, "lon": float|None,
    #                 "raw_url": str|None, "method": str|None}
    #     """
    #     map_data = {"coordinates": None, "lat": None, "lon": None, "raw_url": None, "method": None}
    #     wait = WebDriverWait(driver, 5)

    #     try:
    #         # 1) Tìm iframe có google maps (src hoặc data-src). nhiều site lazy-load vào data-src
    #         iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src*='google.com/maps'], iframe[data-src*='google.com/maps']")
    #         for iframe in iframes:
    #             url = iframe.get_attribute('data-src') or iframe.get_attribute('src')
    #             if not url:
    #                 continue
    #             parsed = parse_coords_from_url(url)
    #             if parsed:
    #                 lat, lon, method = parsed
    #                 map_data.update({
    #                     "coordinates": f"{lat},{lon}",
    #                     "lat": lat,
    #                     "lon": lon,
    #                     "raw_url": url,
    #                     "method": f"iframe_url:{method}"
    #                 })
    #                 return map_data

    #             # nếu iframe lazy-load (vẫn chưa có data-src), thử scroll để kích hoạt lazyload rồi đọc lại src
    #             try:
    #                 driver.execute_script("arguments[0].scrollIntoView(true);", iframe)
    #             except Exception:
    #                 pass
    #             # read attributes again
    #             url2 = iframe.get_attribute('data-src') or iframe.get_attribute('src')
    #             if url2 and url2 != url:
    #                 parsed = parse_coords_from_url(url2)
    #                 if parsed:
    #                     lat, lon, method = parsed
    #                     map_data.update({
    #                         "coordinates": f"{lat},{lon}",
    #                         "lat": lat,
    #                         "lon": lon,
    #                         "raw_url": url2,
    #                         "method": f"iframe_url_after_scroll:{method}"
    #                     })
    #                     return map_data

    #         try:
    #             elem = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'°') or contains(text(), 'N') or contains(text(),'S') or contains(text(),'E') or contains(text(),'W')]")))
    #             text = elem.text.strip()
    #             m = re.search(r'([+-]?\d+(?:\.\d+)?)\D+([+-]?\d+(?:\.\d+)?)', text)
    #             if m:
    #                 lat = float(m.group(1)); lon = float(m.group(2))
    #                 map_data.update({"coordinates": f"{lat},{lon}", "lat": lat, "lon": lon, "raw_url": None, "method": "page_text_decimal"})
    #                 return map_data
    #             parts = text.split()
    #             if len(parts) >= 2:
    #                 try:
    #                     lat = parts[0]
    #                     lon = parts[1]
    #                     map_data.update({"coordinates": text, "lat": lat, "lon": lon, "raw_url": None, "method": "page_text_dms"})
    #                     return map_data
    #                 except Exception:
    #                     pass
    #         except Exception:
    #             pass

    #     except Exception as e:
    #         # log whatever you use e.g., logger.exception(...)
    #         print("extract_map error:", e)

    #     return map_data

    def _get_single_detail_with_driver(self, driver: WebDriver, url: str) -> Optional[Dict]:
        """Helper function to get details for one URL using a pre-existing driver."""
        try:
            driver.get(url)
            # Chờ một element quan trọng để đảm bảo trang đã load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.re__pr-title"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")
            return self.process_detail(driver, soup, url) # Truyền cả 3 tham số
        except Exception as e:
            logger.error(f"Failed to get detail page for {url} with existing driver: {e}", exc_info=True)
            return None
    def _pick_from_srcset(srcset: str) -> str:
        first = srcset.split(',')[0].strip()
        return first.split()[0] if first else ''
    
    def dms_to_decimal(coord: str) -> Optional[float]:
        """
        Convert a DMS (Degrees/Minutes/Seconds) string to decimal degrees.
        Hỗ trợ nhiều format:
        - "21°04'41.2\"N"
        - "21 04 41.2 N"
        - "N21°04'"
        - "105.82039E"
        Trả về float hoặc None nếu không parse được.
        """
        text = coord.strip()
        match = _DMS_PATTERN.search(text)
        if not match:
            return None

        g = match.groupdict()
        deg  = float(g['deg'])
        minu = float(g['min']) if g['min'] else 0.0
        sec  = float(g['sec']) if g['sec'] else 0.0
        dir_ = (g['dir_prefix'] or g['dir_suffix'] or '').upper()

        decimal = abs(deg) + minu / 60.0 + sec / 3600.0

        # Giữ dấu âm nếu ban đầu đã âm
        if deg < 0:
            decimal = -decimal

        # Điều chỉnh theo hướng
        if dir_ in ['S', 'W']:
            decimal = -abs(decimal)
        elif dir_ in ['N', 'E']:
            decimal = abs(decimal)

        return decimal

    def _dump_debug(driver, tag="contact", folder="/tmp"):
        """Save screenshot + page source, return paths."""
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        os.makedirs(folder, exist_ok=True)
        png = os.path.join(folder, f"{tag}_screenshot_{ts}.png")
        html = os.path.join(folder, f"{tag}_page_{ts}.html")
        try:
            driver.save_screenshot(png)
        except Exception:
            png = None
        try:
            with open(html, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
        except Exception:
            html = None
        return {"screenshot": png, "page_source": html}