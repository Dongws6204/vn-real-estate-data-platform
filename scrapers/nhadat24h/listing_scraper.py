from typing import Dict, List
from bs4 import BeautifulSoup
from datetime import datetime
from ..base.base_scraper import ListingScraper
from ..base.utils import clean_text, extract_number
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class Nhadat24hListingScraper(ListingScraper):
    def scrape(self) -> List[Dict]:
        listings = []
        page = 1
        
        try:
            while page <= self.config['max_pages']:
                url = f"{self.config['listings_url']}{page}"
                logger.info(f"Scraping page {page}: {url}")
                
                soup = self.get_page(url)
                if not soup:
                    logger.error(f"Failed to get page {page}")
                    break
                    
                page_listings = self.get_listings(soup)
                if not page_listings:
                    logger.info(f"No listings found on page {page}")
                    break
                    
                listings.extend(page_listings)
                logger.info(f"Found {len(page_listings)} listings on page {page}")
                
                page += 1
                
        except Exception as e:
            logger.error(f"Error in scrape method: {str(e)}")
            
        return listings

    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        listings = []
        try:
            listing_elements = soup.find_all('div', class_='dv-item')
            
            for element in listing_elements:
                try:
                    listing_data = self.process_listing(element)
                    if listing_data:
                        listings.append(listing_data)
                except Exception as e:
                    logger.error(f"Error processing listing: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error in get_listings: {str(e)}")
            
        return listings

    def process_listing(self, element) -> Dict:
        try:
            # Get basic information
            title_elem = element.find('span', class_=['a-title-100', 'a-title-110'])
            link_elem = element.find('a', class_='a-title')
            price_elem = element.find('label', class_='a-txt-cl1')
            area_elem = element.find('label', class_='a-txt-cl2')
            location_elem = element.find('label', class_='rvVitri')
            desc_elem = element.find('label', class_='lb-des')
            property_type_elem = element.find('span', class_='ex3')
            contact_elem = element.find('div', class_='fullname')

            # Get property details
            details = self._extract_details(element)

            # Build listing data
            listing_data = {
                'source': 'nhadat24h',
                'source_id': link_elem['href'].split('-ID')[-1] if link_elem else None,
                'url': f"{self.config['base_url']}{link_elem['href']}" if link_elem else None,
                'title': clean_text(title_elem.text) if title_elem else None,
                'price': self._parse_price(price_elem.text) if price_elem else None,
                'area': extract_number(area_elem.text) if area_elem else None,
                'area_unit': 'm2',
                'location': clean_text(location_elem.text) if location_elem else None,
                'road_width': details.get('road_width'),
                'frontage': details.get('frontage'),
                'direction': details.get('direction'),
                'description': clean_text(desc_elem.text) if desc_elem else None,
                'property_type': clean_text(property_type_elem.text) if property_type_elem else None,
                'contact_name': clean_text(contact_elem.text) if contact_elem else None,
                'crawled': False,
                'crawled_at': datetime.now(),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }

            return listing_data

        except Exception as e:
            logger.error(f"Error parsing listing element: {str(e)}")
            return None

    def _extract_details(self, element) -> Dict:
        details = {}
        try:
            property_details = element.find_all('label', class_=None)
            for detail in property_details:
                text = clean_text(detail.text)
                if 'Đường vào:' in text:
                    details['road_width'] = text.replace('Đường vào:', '').strip()
                elif 'Mặt tiền:' in text:
                    details['frontage'] = text.replace('Mặt tiền:', '').strip()
                elif 'Hướng:' in text:
                    details['direction'] = text.replace('Hướng:', '').strip()
        except Exception as e:
            logger.error(f"Error extracting details: {str(e)}")
        return details

    def _parse_price(self, price_text: str) -> Dict:
        price_data = {
            'amount': 0,
            'currency': 'VND',
            'unit': 'total',
            'original_text': price_text
        }

        try:
            if not price_text or 'thỏa thuận' in price_text.lower():
                return price_data

            clean_price = price_text.lower().replace(' ', '')
            amount = extract_number(clean_price)

            if amount is None:
                return price_data

            if 'tỷ' in clean_price:
                amount *= 1000
                price_data['unit'] = 'billion'
            elif 'triệu' in clean_price:
                price_data['unit'] = 'million'

            price_data['amount'] = amount

        except Exception as e:
            logger.error(f"Error parsing price {price_text}: {str(e)}")

        return price_data