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
        self._driver = None

    @property
    def driver(self):
        """Khởi tạo Lazy-load Selenium Driver. Chỉ bật khi nào thực sự được gọi đến để tiết kiệm RAM."""
        if self._driver is None:
            logger.info("Khởi tạo shared Selenium WebDriver...")
            # Dùng lại class webDriverManager nhưng không xài context manager (with)
            manager = webDriverManager(headless=self.config.get('headless', True))
            self._driver = manager.__enter__()
        return self._driver

    def close(self):
        """Hàm dọn dẹp, tắt trình duyệt sau khi scraper làm xong việc."""
        if self._driver:
            logger.info("Tắt shared Selenium WebDriver...")
            self._driver.quit()
            self._driver = None

    def fetch_fast(self, url: str) -> Optional[BeautifulSoup]:
        """Tốc độ cao: HTTP Request siêu tốc giả lập Chrome."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"[Fast] Successfully fetched: {url}")
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"[Fast] Error fetching {url} với curl_cffi: {e}")
            return None

    def fetch_render(self, url: str) -> Optional[BeautifulSoup]:
        """Hạng nặng: Dùng chung 1 Selenium Driver để Render page có JS/Cloudflare."""
        try:
            self.driver.get(url)
            sleep(3) # Cân nhắc chuyển sleep này vào scrapers cụ thể nếu cần chờ element cụ thể
            logger.info(f"[Render] Successfully fetched bằng Selenium: {url}")
            return BeautifulSoup(self.driver.page_source, 'html.parser')
        except Exception as e:
            logger.error(f"[Render] Lỗi Selenium khi mở {url}: {e}")
            return None

    def get_page(self, url: str, use_js: bool = False) -> Optional[BeautifulSoup]:
        """Hàm proxy mặc định. use_js=True để gọi fetch_render, ngược lại gọi fetch_fast"""
        if use_js:
            return self.fetch_render(url)
        return self.fetch_fast(url)

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