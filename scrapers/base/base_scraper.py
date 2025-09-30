from abc import ABC, abstractmethod
from attr import dataclass
from joblib import Logger
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Callable
from time import sleep
from fake_useragent import UserAgent
from config.logging_config import setup_logger
from curl_cffi import requests as curl_requests

from selenium.webdriver.remote.webdriver import WebDriver

from utils.proxy_manager import ProxyManager
from .web_driver import webDriverManager

logger = setup_logger(__name__)

@dataclass
class FetchResult:
    ok: bool
    soup: Optional[BeautifulSoup] = None
    status_code: Optional[int] = None
    error: Optional[Exception] = None
    

class BaseScraper(ABC):
    def __init__(self, config: Dict):
        self.config = config
        self.session = curl_requests.Session(impersonate="chrome110")

    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Successfully fetched page with status code: {response.status_code}")
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching {url} with curl_cffi: {e}", exc_info=True)
            return None
        #     headers = {'User-Agent': self.ua.random}
        #     response = self.session.get(url, headers=headers, timeout=self.config.get('timeout', 30))
        #     response.raise_for_status()
            
        #     if self.config.get('delay'):
        #         sleep(self.config['delay'])
                
        #     return BeautifulSoup(response.text, 'html.parser')
        # except Exception as e:
        #     logger.error(f"Error fetching {url}: {str(e)}")
        #     return None

class ListingScraper(BaseScraper):

    def _execute_selenium_task(self, url: str, extraction_logic: Callable[[WebDriver, str], Dict[str, Optional[str]]]) -> Dict [str, Optional[str]]:
        try:
            with webDriverManager(headless=True) as driver:
                driver.get(url)
                return extraction_logic(driver, url)
        except Exception as e:
            Logger.error(f"Error executing Selenium task for {url}: {str(e)}")
            return {'author_name': None, 'contact_phone': None}

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