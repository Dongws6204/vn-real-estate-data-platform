from typing import Dict, Optional
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class CafelandDetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Optional[Dict]:
        soup = self.get_page(url)
        if not soup:
            return None
        return self.process_detail(soup)

    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            # Get main content
            main_content = soup.find('div', class_='col-md-9')
            if not main_content:
                logger.error("Main content section not found")
                return None

            detail_data = {
                'basic_info': self._extract_basic_info(main_content),
                'features': self._extract_features(main_content),
                'description': self._extract_description(main_content),
                'contact_info': self._extract_contact_info(soup),
                'media': self._extract_media(main_content),
                'location': self._extract_location(main_content),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            return detail_data

        except Exception as e:
            logger.error(f"Error processing detail: {str(e)}")
            return None

    def _extract_basic_info(self, content: BeautifulSoup) -> Dict:
        basic_info = {}
        try:
            # Get title
            title_elem = content.find('h1', class_='head-title')
            if title_elem:
                basic_info['title'] = clean_text(title_elem.text)

            # Get location
            location_elem = content.find('div', class_='reales-location')
            if location_elem:
                basic_info['location'] = clean_text(location_elem.text)

            # Get price and area info
            info_group = content.find('div', class_='reals-info-group')
            if info_group:
                info_items = info_group.find_all('div', class_='col-item')
                for item in info_items:
                    label = item.find('div', class_='infor-note')
                    value = item.find('div', class_='infor-data')
                    if label and value:
                        key = clean_text(label.text).lower()
                        if 'giá' in key:
                            basic_info['price'] = self._parse_price(value.text)
                        elif 'diện tích' in key:
                            basic_info['area'] = extract_number(value.text)

        except Exception as e:
            logger.error(f"Error extracting basic info: {str(e)}")
        return basic_info

    def _extract_features(self, content: BeautifulSoup) -> Dict:
        features = {}
        try:
            features_section = content.find('div', class_='reals-architecture')
            if features_section:
                feature_items = features_section.find_all('div', class_='reals-house-item')
                for item in feature_items:
                    label = item.find('span', class_='title-item')
                    value = item.find('span', class_='value-item')
                    if label and value:
                        key = clean_text(label.text).lower()
                        val = clean_text(value.text)
                        features[key] = val

        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
        return features

    def _extract_description(self, content: BeautifulSoup) -> str:
        try:
            desc_elem = content.find('div', class_='reals-description')
            if desc_elem:
                return clean_text(desc_elem.text)
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
        return ''

    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        contact_info = {}
        try:
            contact_block = soup.find('div', class_='block-contact-infor')
            if contact_block:
                # Get name
                name_elem = contact_block.find('div', class_='profile-name')
                if name_elem:
                    contact_info['name'] = clean_text(name_elem.text)

                # Get phone
                phone_elem = contact_block.find('div', class_='profile-phone')
                if phone_elem:
                    contact_info['phone'] = clean_text(phone_elem.text)

                # Get email
                email_elem = contact_block.find('div', class_='profile-email')
                if email_elem:
                    contact_info['email'] = clean_text(email_elem.text)

                # Get address
                addr_elem = contact_block.find('div', class_='profile-addr')
                if addr_elem:
                    contact_info['address'] = clean_text(addr_elem.text)

        except Exception as e:
            logger.error(f"Error extracting contact info: {str(e)}")
        return contact_info

    def _extract_media(self, content: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            carousel = content.find('div', class_='carousel-inner')
            if carousel:
                for img in carousel.find_all('img'):
                    if 'src' in img.attrs:
                        media['images'].append(img['src'])

        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media

    def _extract_location(self, content: BeautifulSoup) -> Dict:
        location = {}
        try:
            map_section = content.find('div', class_='reals-map')
            if map_section:
                iframe = map_section.find('iframe')
                if iframe and 'src' in iframe.attrs:
                    location['map_url'] = iframe['src']
                    # Extract coordinates from map URL
                    coords = iframe['src'].split('?q=')[1].split('&')[0].split(',')
                    location['latitude'] = coords[0]
                    location['longitude'] = coords[1]

        except Exception as e:
            logger.error(f"Error extracting location: {str(e)}")
        return location

    def _parse_price(self, price_text: str) -> Dict:
        """Parse price text into structured data"""
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
            logger.error(f"Error parsing price {price_text}: {str(e)}")
            
        return price_data