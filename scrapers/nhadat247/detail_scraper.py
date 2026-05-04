from typing import Dict
from bs4 import BeautifulSoup
from datetime import datetime
import re
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number, parse_date
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class NhaDat247DetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Dict:
        soup = self.get_page(url, use_js=False)
        if not soup:
            return None
        return self.process_detail(soup)

    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            detail_data = {
                'features': self.extract_features(soup),
                'description': self.extract_description(soup),
                'contact_info': self.extract_contact_info(soup),
                'media': self.extract_media(soup),
                'metadata': self.extract_metadata(soup),
                'location': self.extract_location(soup),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            return detail_data
        except Exception as e:
            logger.error(f"Error processing NhaDat247 detail: {str(e)}")
            return None

    def extract_features(self, soup: BeautifulSoup) -> Dict:
        features = {}
        try:
            rows = soup.select('.re__pr-specs-content-item')
            for row in rows:
                title_el = row.select_one('.re__pr-specs-content-item-title')
                value_el = row.select_one('.re__pr-specs-content-item-value')
                if not title_el or not value_el:
                    continue
                label = clean_text(title_el.get_text(strip=True))
                value = clean_text(value_el.get_text(strip=True))
                
                if 'Diện tích' in label or 'Area' in label:
                    features['area'] = extract_number(value)
                elif 'Số phòng ngủ' in label or 'Bedrooms' in label:
                    features['bedrooms'] = int(extract_number(value))
                elif 'Số toilet' in label or 'Bathrooms' in label:
                    features['bathrooms'] = int(extract_number(value))
                elif 'Số tầng' in label or 'Floors' in label:
                    features['floors'] = int(extract_number(value))
                else:
                    # Các đặc điểm khác lưu dạng key gốc
                    features[label.lower()] = value
        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
        return features

    def extract_description(self, soup: BeautifulSoup) -> str:
        try:
            # Mô tả nằm trong .js__tracking
            desc_element = soup.select_one('.js__tracking')
            if desc_element:
                return clean_text(desc_element.get_text(strip=True))
            return ''
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
            return ''

    def extract_contact_info(self, soup: BeautifulSoup) -> dict:
        contact = {"name": None, "phone": None, "email": None}

        try:
            # 1. NAME
            name_el = soup.select_one('span.mnSbTitle a')
            contact['name'] = clean_text(name_el.text) if name_el else ''

            # 2. 
            phone = None

            # Case 1: data-formatted-phone
            el = soup.select_one("a.phoneLinkpopup")
            if el:
                href = el.get("href", "")
                # tìm dạng tel:xxxxxxxxxx
                m = re.search(r"tel:(\d+)", href)
                if m:
                    phone = m.group(1).strip()

            # Case 2: id="phoneLinkpopup"
            if not phone:
                popup = soup.select_one('#phoneLinkpopup')
                if popup:
                    phone = popup.text.strip()

            # Case 3: số che dạng 091 **** 686
            if not phone:
                masked = soup.select_one("span.formatted-phone")
                if masked:
                    phone = masked.text.strip()

            # Case 4: regex toàn trang (bắt số thật)
            if not phone:
                text = soup.get_text(" ")
                m = re.search(r"0\d{9,10}", text)
                if m:
                    phone = m.group(0)

            # Case 5: tránh gán nhầm ID /thanh-vien/047316289.html
            # Chỉ dùng khi số dài 9–11 chữ số VÀ không phải bắt đầu bằng 0 → thêm 0
            if not phone and name_el:
                href = name_el.get("href", "")
                m = re.search(r'/thanh-vien/(\d+)\.html', href)
                if m:
                    code = m.group(1)
                    if 7 <= len(code) <= 10:  # ID ngắn bất thường → không dùng
                        # chỉ convert nếu nhìn giống số điện thoại thật
                        phone = "0" + code if not code.startswith("0") else code

            if phone:
                contact["phone"] = phone

            # 3. EMAIL
            email_label = soup.find("td", class_="td-name", string=lambda x: x and "Email" in x)
            if email_label:
                next_td = email_label.find_next("td")
                if next_td:
                    contact["email"] = next_td.get("title") or next_td.get_text(strip=True)

            return contact

        except Exception as e:
            print("Error:", e)
            return contact

    def extract_media(self, soup: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            # Ảnh - Sửa selector bị thiếu dấu chấm (.) và trỏ thẳng vào thẻ img
            image_elements = soup.select('.js-pr-img-item img')
            for img in image_elements:
                src = img.get('src') or img.get('data-src') or ''
                if src:
                    # Nếu link ảnh bắt đầu bằng / thì nối thêm domain
                    if src.startswith('/'):
                        src = 'https://nhadat247.net' + src
                    # Lọc trùng lặp nếu có 2 ảnh giống hệt (do swiper copy slide)
                    if src not in media['images']:
                        media['images'].append(src)
            # Video
            video_element = soup.select_one('div.video-container iframe')
            if video_element and 'src' in video_element.attrs:
                media['videos'].append(video_element['src'])
        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media

    def extract_location(self, soup: BeautifulSoup) -> Dict:
        location = {}
        try:
            # Địa chỉ text có thể nằm trong breadcrumb hoặc tiêu đề, nhưng tọa độ lấy từ script
            html_str = str(soup)
            match = re.search(r'mapcenter\s*=\s*\[\s*([\d.]+)\s*,\s*([\d.]+)\s*\]', html_str)
            if match:
                lon = float(match.group(1))
                lat = float(match.group(2))
                location['coordinates'] = {'latitude': lat, 'longitude': lon}
            breadcrumb = soup.select_one('.re_breadcrumb')
            if breadcrumb:
                location['address'] = clean_text(breadcrumb.get_text(separator=', '))
        except Exception as e:
            logger.error(f"Error extracting location: {str(e)}")
        return location

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {}
        try:
            # Dùng selector đã test: .js__pr-config-item
            meta_items = soup.select('.js__pr-config-item')
            for item in meta_items:
                title_el = item.select_one('.title')
                value_el = item.select_one('.value')
                if not title_el or not value_el:
                    continue
                label = clean_text(title_el.get_text(strip=True))
                value = clean_text(value_el.get_text(strip=True))
                
                if 'ngày đăng' in label.lower() or 'post date' in label.lower():
                    metadata['posted_date'] = parse_date(value)
                elif 'loại tin' in label.lower() or 'property type' in label.lower():
                    metadata['property_type'] = value
                elif 'mã tin' in label.lower() or 'post id' in label.lower():
                    metadata['id'] = value
                elif 'trạng thái' in label.lower() or 'status' in label.lower():
                    metadata['status'] = value
                else:
                    metadata[label.lower()] = value
        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}")
        return metadata