from typing import Dict
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number, parse_date
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class BatDongSanDetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Dict:
        soup = self.get_page(url)
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
            feature_list = soup.select('div.product-config ul.short-detail-2 li')
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
            logger.error(f"Error extracting BDS features: {str(e)}")
        return features

    def extract_description(self, soup: BeautifulSoup) -> str:
        try:
            desc_element = soup.select_one('div.product-detail-description')
            return clean_text(desc_element.text)
        except Exception as e:
            logger.error(f"Error extracting BDS description: {str(e)}")
            return ''

    def extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        contact_info = {}
        try:
            contact_section = soup.select_one('div.contact-info')
            contact_info['name'] = clean_text(contact_section.select_one('div.name').text)
            contact_info['phone'] = clean_text(contact_section.select_one('div.phone').text)
            contact_info['address'] = clean_text(contact_section.select_one('div.address').text)
        except Exception as e:
            logger.error(f"Error extracting BDS contact info: {str(e)}")
        return contact_info

    def extract_media(self, soup: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            # Extract image URLs
            image_elements = soup.select('div.product-gallery img')
            media['images'] = [img['src'] for img in image_elements if 'src' in img.attrs]
            
            # Extract video URLs if any
            video_element = soup.select_one('div.product-video iframe')
            if video_element and 'src' in video_element.attrs:
                media['videos'].append(video_element['src'])
                
        except Exception as e:
            logger.error(f"Error extracting BDS media: {str(e)}")
        return media

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {}
        try:
            # Extract posting date
            date_element = soup.select_one('span.update-time')
            if date_element:
                metadata['posted_date'] = parse_date(clean_text(date_element.text))
            
            # Extract property type
            type_element = soup.select_one('span.property-type')
            if type_element:
                metadata['property_type'] = clean_text(type_element.text)
            
            # Extract project info if available
            project_element = soup.select_one('span.project-name')
            if project_element:
                metadata['project'] = clean_text(project_element.text)
                
        except Exception as e:
            logger.error(f"Error extracting BDS metadata: {str(e)}")
        return metadata