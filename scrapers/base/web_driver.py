# File: scrapers/base/web_driver.py (hoặc nơi mày đặt class này)

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common.exceptions import WebDriverException
import undetected_chromedriver as uc
from typing import Optional
import logging
import random
from selenium_stealth import stealth

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)
# chặn việc __del__ gọi quit() lần 2 gây lỗi WinError 6
uc.Chrome.__del__ = lambda self: None

# Danh sách User-Agent đa dạng hơn
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
]

class webDriverManager:
    """
    A context manager for Selenium WebDriver, supporting both standard
    and undetected-chromedriver for enhanced stealth capabilities.
    """
    def __init__(self, headless: bool = True, use_undetected: bool = False):
        """
        Initializes the WebDriverManager.

        Args:
            headless (bool): If True, runs the browser in headless mode.
            use_undetected (bool): If True, uses undetected-chromedriver to bypass bot detection.
        """
        self.headless = headless
        self.use_undetected = use_undetected
        self.driver: Optional[WebDriver] = None

    def _create_standard_driver(self) -> WebDriver:
        """Initializes a standard Selenium ChromeDriver."""
        logger.info("Initializing Standard Selenium ChromeDriver...")
        options = webdriver.ChromeOptions()
        
        # --- Common options setup ---
        self._configure_options(options)
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(20) # Increased timeout for robustness
            return driver
        except WebDriverException as e:
            logger.error("Failed to initialize standard ChromeDriver: %s", str(e), exc_info=True)
            raise

    def _create_undetected_driver(self) -> WebDriver:
        """Initializes an Undetected ChromeDriver for stealth."""
        logger.info("Initializing Undetected ChromeDriver...")
        options = uc.ChromeOptions()

        # --- Common options setup ---
        self._configure_options(options)

        try:
            driver = uc.Chrome(options=options)
            driver.set_page_load_timeout(30) # Undetected might need more time
            return driver
        except Exception as e:
            logger.error("Failed to initialize Undetected ChromeDriver: %s", str(e), exc_info=True)
            raise

    def _configure_options(self, options):
        """Applies a common set of configurations to the options object."""
        # Anti-detection flags
        options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1920,1080")
        
        # Experimental options for stealth
        if not self.use_undetected: # undetected-chromedriver handles these internally
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
        
        # Headless mode
        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
        
        # Performance and stability flags
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.page_load_strategy = 'eager'

    def __enter__(self) -> WebDriver:
        try:
            # Logic chọn driver giữ nguyên
            if self.use_undetected:
                self.driver = self._create_undetected_driver()
            else:
                self.driver = self._create_standard_driver()
            
            # === NÂNG CẤP QUAN TRỌNG NHẤT ===
            # Áp dụng các bản vá của selenium-stealth
            logger.info("Applying stealth patches to the WebDriver...")
            stealth(
                self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            logger.info("Stealth patches applied.")

            # Script xóa navigator.webdriver vẫn hữu ích
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("WebDriver successfully initialized and configured.")
            return self.driver
        except Exception as e:
            raise e

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        Exits the context, quitting the driver and logging any exceptions.
        """
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver has been quit.")
        
        if exc_type:
            logger.error(
                "An exception occurred within the WebDriver context: %s", 
                exc_value, 
                exc_info=(exc_type, exc_value, exc_tb)
            )
        # Return False to propagate the exception if one occurred
        return False


