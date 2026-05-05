import pytest

from bs4 import BeautifulSoup
from scrapers.raovat321.listing_scraper import RaoVat321ListingScraper
from scrapers.raovat321.detail_scraper import RaoVat321DetailScraper

@pytest.fixture
def listing_scraper():
    config = {
        'base_url': 'https://raovat321.com/bat-dong-san',
        'max_pages': 1,
        'delay': 0
    }
    return RaoVat321ListingScraper(config)

@pytest.fixture
def detail_scraper():
    config = {
        'timeout': 30,
        'retry_attempts': 3
    }
    return RaoVat321DetailScraper(config)

def test_process_listing(listing_scraper):
    html = '''
    <div class="property-item">
        <h2 class="title">Test Property</h2>
        <div class="price">1.5 tỷ</div>
        <div class="location">Quận 1, TP.HCM</div>
        <a href="https://example.com/property/123">Link</a>
    </div>
    '''
    element = BeautifulSoup(html, 'html.parser')
    result = listing_scraper.process_listing(element)
    
    assert result['title'] == 'Test Property'
    assert result['price'] == '1.5 tỷ'
    assert result['location'] == 'Quận 1, TP.HCM'
    assert result['url'] == 'https://example.com/property/123'
    assert result['source'] == 'raovat321'

def test_process_detail(detail_scraper):
    html = '''
    <div class="property-detail">
        <h1 class="property-title">Test Property</h1>
        <div class="property-description">Test description</div>
        <div class="property-features">
            <span class="area">100 m²</span>
            <span class="bedrooms">3</span>
            <span class="bathrooms">2</span>
        </div>
    </div>
    '''
    soup = BeautifulSoup(html, 'html.parser')
    result = detail_scraper.process_detail(soup)
    
    assert result['title'] == 'Test Property'
    assert result['description'] == 'Test description'
    assert result['features']['area'] == 100
    assert result['features']['bedrooms'] == 3
    assert result['features']['bathrooms'] == 2