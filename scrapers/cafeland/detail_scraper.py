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

            # Lấy tạm features để bóc giá và diện tích
            features = self._extract_features(soup)
            price_key = next((k for k in features if 'giá' in k.lower() or 'price' in k.lower()), None)
            if price_key:
                basic_info['price'] = self._parse_price(features[price_key])
                
            area_key = next((k for k in features if 'diện tích' in k.lower() or 'area' in k.lower()), None)
            if area_key:
                basic_info['area'] = extract_number(features[area_key])
                
        except Exception as e:
            logger.error(f"Error extracting basic info: {str(e)}")
        return basic_info

    def _extract_features(self, soup: BeautifulSoup) -> Dict:
        features = {}
        try:
            for item in soup.select('div.reals-info-group .col-item, div.reals-architecture .col-item'):
                label = item.select_one('.infor-note, .title-item')
                value = item.select_one('.infor-data, .value-item')
                if label and value:
                    features[label.get_text(strip=True)] = value.get_text(strip=True)
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
        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media

    def _extract_location(self, soup: BeautifulSoup) -> Dict:
        location = {'latitude': None, 'longitude': None}
        try:
            map_iframe = soup.select_one('iframe')
            if map_iframe and map_iframe.get('src'):
                match = re.search(r'q=([\d.]+),([\d.]+)', map_iframe['src'])
                if match:
                    location['latitude'] = float(match.group(1))
                    location['longitude'] = float(match.group(2))
        except Exception as e:
            logger.error(f"Error extracting location coords: {str(e)}")
        return location

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {'post_id': None, 'posted_date': None}
        try:
            meta_div = soup.select_one('div.col-right .infor')
            if meta_div:
                txt = meta_div.get_text("|", strip=True)
                id_match = re.search(r"Asset Code:\|(\d+)", txt)
                date_match = re.search(r"Date posted:\s*(\d{2}-\d{2}-\d{4})", txt)
                
                if id_match:
                    metadata['post_id'] = id_match.group(1)
                if date_match:
                    metadata['posted_date'] = date_match.group(1)
        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}")
        return metadata

    def _parse_price(self, price_text: str) -> Dict:
        price_data = {
            'amount': 0,
            'currency': 'VND',
            'unit': 'total',
            'original_text': price_text
        }
        try:
            clean_price = price_text.lower().replace(' ', '')
            amount = extract_number(clean_price)
            if amount is None:
                return price_data

            if 'tỷ' in clean_price:
                amount *= 1000000000
                price_data['unit'] = 'billion'
            elif 'triệu' in clean_price:
                amount *= 1000000
                price_data['unit'] = 'million'
            price_data['amount'] = amount

        except Exception as e:
            logger.error(f"Error parsing price '{price_text}': {str(e)}")
        return price_data