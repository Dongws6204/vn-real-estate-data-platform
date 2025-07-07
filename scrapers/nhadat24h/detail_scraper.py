from typing import Dict, Optional
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import DetailScraper
from ..base.utils import clean_text, extract_number
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class Nhadat24hDetailScraper(DetailScraper):
    def get_detail(self, url: str) -> Optional[Dict]:
        soup = self.get_page(url)
        if not soup:
            return None
        return self.process_detail(soup)

    def process_detail(self, soup: BeautifulSoup) -> Dict:
        try:
            # Get breadcrumb data
            breadcrumb = self._extract_breadcrumb(soup)
            
            # Get main content
            # main_content = soup.find('div', class_='dv-main-detail')
            main_content = soup.find('div', id='content')
            if not main_content:
                logger.error("Main content section not found")
                return None

            # Extract data
            detail_data = {
                'breadcrumb': breadcrumb,
                'basic_info': self._extract_basic_info(main_content),
                'features': self._extract_features(main_content),
                'description': self._extract_description(main_content),
                'contact_info': self._extract_contact_info(main_content),
                'media': self._extract_media(main_content),
                'crawled': True,
                'crawled_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            return detail_data

        except Exception as e:
            logger.error(f"Error processing detail: {str(e)}")
            return None

    def _extract_breadcrumb(self, soup: BeautifulSoup) -> Dict:
        """Extract breadcrumb navigation data"""
        breadcrumb = {
            'path': []
        }
        try:
            breadcrumb_div = soup.find('div', class_='dv-breadcrumb')
            if breadcrumb_div:
                links = breadcrumb_div.find_all('a')
                for link in links:
                    breadcrumb['path'].append({
                        'title': clean_text(link.text),
                        'url': link.get('href', '')
                    })
        except Exception as e:
            logger.error(f"Error extracting breadcrumb: {str(e)}")
        return breadcrumb

    def _extract_basic_info(self, content: BeautifulSoup) -> Dict:
        """Extract basic listing information"""
        basic_info = {}
        try:
            # Extract title
            title_elem = content.find('h1', id='txtcontenttieudetin')
            if title_elem:
                basic_info['title'] = clean_text(title_elem.text)
                basic_info['listing_id'] = title_elem.get('data-idn')

            # Extract post time
            time_elem = content.find('label', id='ContentPlaceHolder1_ctl00_lbDate')
            if time_elem:
                basic_info['posted_time'] = clean_text(time_elem.text)

            # Extract price and area
            price_area_elem = content.find('label', id='ContentPlaceHolder1_ctl00_lbGiaDienTich')
            if price_area_elem:
                price_strong = price_area_elem.find('label', class_='strong1')
                area_strong = price_area_elem.find('label', class_='strong2')
                
                if price_strong:
                    basic_info['price'] = extract_number(price_strong.text)
                if area_strong:
                    basic_info['area'] = extract_number(area_strong.text)

            # Extract location
            location_elem = content.find('label', id='ContentPlaceHolder1_ctl00_lbTinhThanh')
            if location_elem:
                basic_info['location'] = clean_text(location_elem.text)

            # Extract property type
            property_type_elem = content.find('label', id='ContentPlaceHolder1_ctl00_lbLoaiBDS')
            if property_type_elem:
                basic_info['property_type'] = clean_text(property_type_elem.text)

        except Exception as e:
            logger.error(f"Error extracting basic info: {str(e)}")
        return basic_info

    def _extract_features(self, content: BeautifulSoup) -> Dict:
        """Extract property features"""
        features = {}
        try:
            # Find all feature tables
            feature_tables = content.find_all('div', class_='dv-tsbds')
            
            for table in feature_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        key = clean_text(cols[0].text)
                        value = clean_text(cols[1].text)
                        
                        if 'Phòng Ngủ' in key:
                            features['bedrooms'] = extract_number(value)
                        elif 'Phòng WC' in key:
                            features['bathrooms'] = extract_number(value)
                        elif 'Số tầng' in key:
                            features['floors'] = extract_number(value)
                        elif 'Hướng' in key:
                            features['direction'] = value
                        elif 'Đường vào' in key:
                            features['road_width'] = extract_number(value)
                        elif 'Mặt tiền' in key:
                            features['frontage'] = extract_number(value)
                        elif 'Mã BĐS' in key:
                            features['property_id'] = value

        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
        return features

    def _extract_description(self, content: BeautifulSoup) -> str:
        """Extract property description"""
        try:
            desc_div = content.find('div', class_='dv-txt-mt')
            if desc_div:
                return clean_text(desc_div.text)
        except Exception as e:
            logger.error(f"Error extracting description: {str(e)}")
        return ''

    def _extract_contact_info(self, content: BeautifulSoup) -> Dict:
        """Extract contact information"""
        contact_info = {}
        try:
            contact_div = content.find('div', class_='detailUserName')
            if contact_div:
                # Extract name
                name_elem = contact_div.find('label', class_='fullname')
                if name_elem:
                    contact_info['name'] = clean_text(name_elem.text)

                # Extract other info
                info_labels = contact_div.find_all('label')
                for label in info_labels:
                    text = clean_text(label.text)
                    icon = label.find('i')
                    if icon:
                        classes = icon.get('class', [])
                        if 'fa-user' in classes:
                            contact_info['type'] = text
                        if 'fa-phone-alt' in classes:
                            # phone = label.find('a').text.strip() if label.find('a') else label.text.strip()
                            contact_info['phone'] = text
                        elif 'fa-map-marker-alt' in classes:
                            # address = label.text.replace(icon.text, '').strip()  # Loại bỏ icon khỏi nội dung text
                            contact_info['address'] = text


        except Exception as e:
            logger.error(f"Error extracting contact info: {str(e)}")
        return contact_info

    def _extract_media(self, content: BeautifulSoup) -> Dict:
        """Extract media (images and videos)"""
        media = {'images': [], 'videos': []}
        try:
            # Extract images
            image_list = content.find('ul', id='ContentPlaceHolder1_ctl00_viewImage1_divLi')
            if image_list:
                for img in image_list.find_all('img', class_=['imageThumb1', 'imageThumb50']):
                    if 'src' in img.attrs:
                        media['images'].append(img['src'])

            # Extract videos if any
            videos = content.find_all('video')
            for video in videos:
                source = video.find('source')
                if source and 'src' in source.attrs:
                    media['videos'].append(source['src'])

        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
        return media