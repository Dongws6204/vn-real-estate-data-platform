import pytest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup
from scrapers.raovat321.listing_scraper import RaoVat321ListingScraper
from scrapers.raovat321.detail_scraper import RaoVat321DetailScraper
from utils.data_validator import PropertyDataValidator
from utils.proxy_manager import ProxyManager
from utils.rate_limiter import RateLimiter

@pytest.fixture
def mock_config():
    return {
        'base_url': 'https://example.com',
        'max_pages': 2,
        'delay': 0
    }

@pytest.fixture
def mock_html():
    return '''
    <div class="property-item">
        <h2 class="title">Test Property</h2>
        <div class="price">1.5 tỷ</div>
        <div class="location">District 1, HCMC</div>
        <a href="/property/123">Details</a>
    </div>
    '''

class TestRaoVat321Scraper:
    def test_process_listing(self, mock_config):
        scraper = RaoVat321ListingScraper(mock_config)
        soup = BeautifulSoup(mock_html, 'html.parser')
        listing = scraper.process_listing(soup.select_one('.property-item'))
        
        assert listing['title'] == 'Test Property'
        assert listing['price'] == '1.5 tỷ'
        assert listing['location'] == 'District 1, HCMC'
        assert listing['url'] == '/property/123'
        assert listing['source'] == 'raovat321'

    @patch('requests.Session.get')
    def test_get_page(self, mock_get, mock_config):
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = mock_html
        
        scraper = RaoVat321ListingScraper(mock_config)
        soup = scraper.get_page('https://example.com')
        
        assert soup is not None
        assert isinstance(soup, BeautifulSoup)

    def test_listing_extraction(self, mock_config):
        scraper = RaoVat321ListingScraper(mock_config)
        soup = BeautifulSoup(mock_html, 'html.parser')
        listings = scraper.get_listings(soup)
        
        assert len(listings) == 1
        assert all(key in listings[0] for key in ['title', 'price', 'location', 'url'])

class TestDataValidator:
    @pytest.fixture
    def validator(self):
        return PropertyDataValidator()

    def test_validate_listing(self, validator):
        valid_data = {
            'source': 'raovat321',
            'source_id': '123',
            'url': 'https://example.com/property/123',
            'title': 'Test Property',
            'price': {'amount': 1500000000, 'currency': 'VND', 'unit': 'total'},
            'location': {'address': 'Test Address', 'district': 'District 1', 'city': 'HCMC'},
            'crawled_at': '2024-02-11T10:00:00'
        }
        
        is_valid, errors = validator.validate_listing(valid_data)
        assert is_valid
        assert not errors

    def test_invalid_listing(self, validator):
        invalid_data = {
            'source': 'raovat321',
            'url': 'not_a_valid_url',
            'price': 'invalid_price'
        }
        
        is_valid, errors = validator.validate_listing(invalid_data)
        assert not is_valid
        assert len(errors) > 0

class TestProxyManager:
    @pytest.fixture
    def proxy_manager(self):
        return ProxyManager()

    def test_add_proxy(self, proxy_manager):
        test_proxy = '127.0.0.1:8080'
        with patch.object(proxy_manager, '_check_proxy', return_value=True):
            proxy_manager.add_proxies([test_proxy])
            assert test_proxy in proxy_manager.proxies

    def test_proxy_failure(self, proxy_manager):
        test_proxy = '127.0.0.1:8080'
        with patch.object(proxy_manager, '_check_proxy', return_value=True):
            proxy_manager.add_proxies([test_proxy])
            
        proxy_manager.report_failure(test_proxy)
        assert proxy_manager.working_proxies[test_proxy]['failures'] == 1

class TestRateLimiter:
    def test_rate_limiting(self):
        rate_limiter = RateLimiter(requests_per_second=2)
        domain = 'example.com'
        
        start_time = time.time()
        for _ in range(3):
            rate_limiter.wait(domain)
        duration = time.time() - start_time
        
        assert duration >= 0.5  # Should take at least 0.5 seconds for 3 requests at 2 RPS