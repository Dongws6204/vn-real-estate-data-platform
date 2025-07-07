from typing import Dict
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number, parse_date
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class RaoVat321DetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Dict:
        soup = self.get_page(url)
        if not soup:
            return None
        return self.process_detail(soup)

    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            # Adjust selectors based on actual HTML structure
            detail_data = {
                'features': self.extract_features(soup),
                'description': self.extract_description(soup),
                'contact_info': self.extract_contact_info(soup),
                'media': self.extract_media(soup),
                'metadata': self.extract_metadata(soup),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            return detail_data
        except Exception as e:
            logger.error(f"Error processing detail: {str(e)}")
            return None

    def extract_features(self, soup: BeautifulSoup) -> Dict:
        features = {}
        try:
            # Adjust selectors based on actual HTML structure
            feature_list = soup.select('div.features li')
            for feature in feature_list:
                label = clean_text(feature.select_one('span.label').text)
                value = clean_text(feature.select_one('span.value').text)
                
                if 'Diện tích' in label:
                    features['area'] = extract_number(value)
                elif 'Phòng ngủ' in label:
                    features['bedrooms'] = int(extract_number(value))
                elif 'Phòng tắm' in label:
                    features['bathrooms'] = int(extract_number(value))
                elif 'Số tầng' in label:
                    features['floors'] = int(extract_number(value))
                
        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
        return features

    def extract_description(self, soup: BeautifulSoup) -> str:
        try:
            desc_element = soup.select_one('div.description')
            return clean_text(desc_element.text) if desc_element else ''
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
            return ''

    def extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        contact_info = {}
        try:
            # Adjust selectors based on actual HTML structure
            contact_section = soup.select_one('div.contact-info')
            if contact_section:
                contact_info['name'] = clean_text(contact_section.select_one('div.name').text)
                contact_info['phone'] = clean_text(contact_section.select_one('div.phone').text)
                contact_info['email'] = clean_text(contact_section.select_one('div.email').text)
        except Exception as e:
            logger.error(f"Error extracting contact info: {str(e)}")
        return contact_info

    def extract_media(self, soup: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            # Adjust selectors based on actual HTML structure
            image_elements = soup.select('div.gallery img')
            media['images'] = [img['src'] for img in image_elements if 'src' in img.attrs]
            
            video_element = soup.select_one('div.video iframe')
            if video_element and 'src' in video_element.attrs:
                media['videos'].append(video_element['src'])
        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {}
        try:
            # Adjust selectors based on actual HTML structure
            date_element = soup.select_one('span.post-date')
            if date_element:
                metadata['posted_date'] = parse_date(clean_text(date_element.text))
            
            category_element = soup.select_one('span.category')
            if category_element:
                metadata['category'] = clean_text(category_element.text)
        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}")
        return metadata