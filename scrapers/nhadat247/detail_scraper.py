from typing import Dict
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number, parse_date
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class NhaDat247DetailScraper(DetailScraper):
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
            feature_list = soup.select('div.features-list li')
            for feature in feature_list:
                label = clean_text(feature.select_one('span.label').text)
                value = clean_text(feature.select_one('span.value').text)
                
                if 'Diện tích' in label:
                    features['area'] = extract_number(value)
                elif 'Số phòng ngủ' in label:
                    features['bedrooms'] = int(extract_number(value))
                elif 'Số toilet' in label:
                    features['bathrooms'] = int(extract_number(value))
                elif 'Số tầng' in label:
                    features['floors'] = int(extract_number(value))
                
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 features: {str(e)}")
        return features

    def extract_description(self, soup: BeautifulSoup) -> str:
        try:
            desc_element = soup.select_one('div.detail-content')
            return clean_text(desc_element.text)
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 description: {str(e)}")
            return ''

    def extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        contact_info = {}
        try:
            contact_section = soup.select_one('div.contact-box')
            contact_info['name'] = clean_text(contact_section.select_one('div.contact-name').text)
            contact_info['phone'] = clean_text(contact_section.select_one('div.contact-phone').text)
            contact_info['email'] = clean_text(contact_section.select_one('div.contact-email').text)
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 contact info: {str(e)}")
        return contact_info

    def extract_media(self, soup: BeautifulSoup) -> Dict:
        media = {'images': [], 'videos': []}
        try:
            # Extract image URLs
            image_elements = soup.select('div.gallery img')
            media['images'] = [img['src'] for img in image_elements if 'src' in img.attrs]
            
            # Extract video URLs if any
            video_element = soup.select_one('div.video-container iframe')
            if video_element and 'src' in video_element.attrs:
                media['videos'].append(video_element['src'])
                
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 media: {str(e)}")
        return media

    def extract_location(self, soup: BeautifulSoup) -> Dict:
        location = {}
        try:
            location_section = soup.select_one('div.location-info')
            location['address'] = clean_text(location_section.select_one('div.address').text)
            location['district'] = clean_text(location_section.select_one('div.district').text)
            location['city'] = clean_text(location_section.select_one('div.city').text)
            
            # Extract coordinates if available
            map_element = soup.select_one('div#map-canvas')
            if map_element:
                location['coordinates'] = {
                    'latitude': float(map_element['data-lat']),
                    'longitude': float(map_element['data-lng'])
                }
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 location: {str(e)}")
        return location

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata = {}
        try:
            # Extract posting date
            date_element = soup.select_one('span.post-date')
            if date_element:
                metadata['posted_date'] = parse_date(clean_text(date_element.text))
            
            # Extract property type
            type_element = soup.select_one('span.property-type')
            if type_element:
                metadata['property_type'] = clean_text(type_element.text)
            
            # Extract additional metadata
            metadata['status'] = clean_text(soup.select_one('span.status').text)
            metadata['id'] = clean_text(soup.select_one('span.post-id').text)
                
        except Exception as e:
            logger.error(f"Error extracting NhaDat247 metadata: {str(e)}")
        return metadata