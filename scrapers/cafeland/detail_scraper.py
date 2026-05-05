from typing import Dict, Optional
import re
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class CafelandDetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Optional[Dict]:
        # Lấy trang bằng requests (có thể cần JS nếu dữ liệu ẩn)
        soup = self.get_page(url, use_js=False)
        if not soup:
            return None
        return self.process_detail(soup)

    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            detail_data = {
                'basic_info': self._extract_basic_info(soup),
                'features': self._extract_features(soup),
                'description': self._extract_description(soup),
                'contact_info': self._extract_contact_info(soup),
                'media': self._extract_media(soup),
                'location': self._extract_location(soup),
                'metadata': self._extract_metadata(soup),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            return detail_data
        except Exception as e:
            logger.error(f"Error processing detail: {str(e)}")
            return None

    def _extract_basic_info(self, soup: BeautifulSoup) -> Dict:
        basic_info = {}
        try:
            title_elem = soup.select_one('h1.head-title')
            if title_elem:
                basic_info['title'] = title_elem.get_text(strip=True)
            
            addr_elem = soup.select_one('div.infor')
            if addr_elem:
                addr_clone = BeautifulSoup(str(addr_elem), 'html.parser').div
                if addr_clone:
                    for div in addr_clone.find_all('div'):
                        div.decompose()
                    clean_fragments = [s for s in addr_clone.stripped_strings if s not in ["Location:", "▸", '"', '", "']]
                    basic_info['location'] = ", ".join(clean_fragments).strip(", ")

        except Exception as e:
            logger.error(f"Error extracting basic info: {str(e)}")
        return basic_info

    def _extract_features(self, soup: BeautifulSoup) -> Dict:
        features = {}
        try:
            for item in soup.select('div.reals-house-item, div.col-item, .reals-info-group .col-item'):
                label = item.select_one('.title-item, .infor-note')
                value = item.select_one('.value-item, .infor-data')
                if label and value:
                    label_text = label.get_text(strip=True)
                    # Bỏ qua giá và diện tích vì đã được lấy từ trang liệt kê (listing)
                    if any(kw in label_text.lower() for kw in ['giá', 'diện tích', 'price', 'area']):
                        continue
                    features[label_text] = value.get_text(strip=True)
        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
        return features

    def _extract_description(self, soup: BeautifulSoup) -> str:
        try:
            desc_elem = soup.select_one('div.reals-description .blk-content')
            if desc_elem:
                return desc_elem.get_text("\n", strip=True)
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
        return ''

    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        contact = {'name': None, 'phone': None, 'email': None}
        try:
            contact_block = soup.select_one('div.profile-info, div.block-contact-infor')
            if not contact_block:
                return contact

            name_elem = contact_block.select_one('.profile-name h2')
            if name_elem:
                contact['name'] = name_elem.get_text(strip=True)

            phone_elem = contact_block.select_one('.profile-phone span')
            if phone_elem and phone_elem.get('onclick'):
                match = re.search(r"'(0\d+\*+)'", phone_elem['onclick'])
                if match:
                    contact['phone'] = match.group(1)

            email_elem = contact_block.select_one('.profile-email span')
            if email_elem:
                user = email_elem.get('data-hidden-name', '')
                domain = email_elem.get('data-hidden-domain', '')
                if user and domain:
                    contact['email'] = f"{user}@{domain}"
        except Exception as e:
            logger.error(f"Error extracting contact finding: {str(e)}")
        return contact

    def _extract_media(self, soup: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            carousel = soup.select_one('div.carousel-inner')
            if carousel:
                preload = carousel.find('link', rel='preload')
                if preload and preload.get('href'):
                    media['images'].append(preload['href'])
                
                for a_tag in carousel.select('a.lg-item'):
                    href = a_tag.get('href')
                    if href and href not in media['images']:
                        media['images'].append(href)
                
                media['images'] = media['images'][:3]
                
                video = carousel.select_one('a.videoks')
                if video and video.get('data-url'):
                    media['videos'].append(video.get('data-url'))
        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media

    def _extract_location(self, soup: BeautifulSoup) -> Dict:
        location = {'latitude': None, 'longitude': None}
        try:
            map_iframe = soup.select_one('iframe[src*="maps.google.com"], .frame-map iframe, .reals-map iframe')
            found_coords = False
            
            if map_iframe and map_iframe.get('src'):
                match = re.search(r'q=([\d.]+),([\d.]+)', map_iframe['src'])
                if match:
                    location['latitude'] = float(match.group(1))
                    location['longitude'] = float(match.group(2))
                    found_coords = True

            if not found_coords:
                # Quét thô toàn trang nếu iframe ẩn hoặc sai class
                raw_coords = re.findall(r'(21\.\d+|10\.\d+),\s*(105\.\d+|106\.\d+|107\.\d+)', str(soup))
                if raw_coords:
                    location['latitude'] = float(raw_coords[0][0])
                    location['longitude'] = float(raw_coords[0][1])

        except Exception as e:
            logger.error(f"Error extracting location coords: {str(e)}")
        return location

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {'post_id': None, 'posted_date': None}
        try:
            meta_div = soup.select_one('div.col-right .infor')
            if meta_div:
                txt = meta_div.get_text(" ", strip=True)
                id_match = re.search(r"(?:Mã tài sản|Asset Code):\s*(\d+)", txt)
                date_match = re.search(r"(?:Ngày đăng|Date posted):\s*(\d{2}-\d{2}-\d{4})", txt)
                
                if id_match:
                    metadata['post_id'] = id_match.group(1)
                if date_match:
                    metadata['posted_date'] = date_match.group(1)
        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}")
        return metadata