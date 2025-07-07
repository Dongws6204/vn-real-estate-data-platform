from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from time import sleep
from fake_useragent import UserAgent
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class BaseScraper(ABC):
    def __init__(self, config: Dict):
        self.config = config
        self.session = requests.Session()
        self.ua = UserAgent()
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            headers = {'User-Agent': self.ua.random}
            response = self.session.get(url, headers=headers, timeout=self.config.get('timeout', 30))
            response.raise_for_status()
            
            if self.config.get('delay'):
                sleep(self.config['delay'])
                
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

class ListingScraper(BaseScraper):
    @abstractmethod
    def scrape(self) -> List[Dict]:
        pass
    
    @abstractmethod
    def get_listings(self, soup: BeautifulSoup) -> List[Dict]:
        pass
    
    @abstractmethod
    def process_listing(self, element) -> Dict:
        pass

class DetailScraper(BaseScraper):
    @abstractmethod
    def get_detail(self, url: str) -> Dict:
        pass
    
    @abstractmethod
    def process_detail(self, soup: BeautifulSoup) -> Dict:
        pass