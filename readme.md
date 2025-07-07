# Property Data Scraper

Multi-site web scraper for real estate listings in Vietnam with MongoDB storage.

## Supported Sites

- RaoVat321.com
- Batdongsan.com.vn 
- Nhadat247.vn
- Alonhadat.com.vn
- Propzy.vn
- ...

## Project Structure

```
property-scraper/
├── scrapers/
│   ├── raovat321/
│   │   ├── __init__.py
│   │   ├── listing_scraper.py
│   │   ├── detail_scraper.py
│   │   └── utils.py
│   ├── batdongsan/
│   │   ├── __init__.py
│   │   ├── listing_scraper.py
│   │   ├── detail_scraper.py
│   │   └── utils.py
│   ├── nhadat247/
│   │   ├── __init__.py
│   │   ├── listing_scraper.py
│   │   ├── detail_scraper.py
│   │   └── utils.py
│   └── base/
│       ├── __init__.py
│       ├── base_scraper.py
│       └── utils.py
├── config/
│   ├── __init__.py
│   ├── mongodb_config.py
│   ├── scraper_config.py
│   └── logging_config.py
├── models/
│   ├── __init__.py
│   └── property.py
├── utils/
│   ├── __init__.py
│   ├── mongodb.py
│   └── logging.py
├── tests/
│   ├── test_raovat321.py
│   ├── test_batdongsan.py
│   └── test_nhadat247.py
├── requirements.txt
├── README.md
└── logs/
    └── scraper.log
```

## Features

- Modular scraper design for each property website
- Unified data schema across all sources
- MongoDB storage with site-specific collections
- Error handling and automatic retries
- Comprehensive logging
- Proxy and rotating user agent support
- Auto-detection of new listings
- Data deduplication
- Data normalization

## System Requirements

- Python 3.11+
- MongoDB 4.0+
- Pip package manager

## Installation

1. Clone repository:
```bash
git clone https://github.com/Property-Guru/property-scraper.git
cd property-scraper
```

2. Create virtual environment:
- Option 1 - Using venv (Python's built-in virtual environment):
```bash
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

- Option 2 - Using Conda:
```bash
conda create -n property python=3.10
conda activate property
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure MongoDB:
- Copy `config/mongodb_config.example.py` to `config/mongodb_config.py`
- Update MongoDB connection settings

## Usage

### Run Individual Scrapers

0. Run all scrapers with default settings
```bash
python main.py
```

1. Run specific source with custom pages:
```bash
python main.py --source raovat321 --max-pages 5
```

2. Run with proxy rotation and export to JSON
```bash
python main.py --use-proxies --export-format json
```

3. Run specific source with Excel export
```bash
python main.py --source batdongsan --export-format excel
```

### Cleaning Logs

1. Clean with backup (default):
```bash
python clean_logs.py
```

2. Clean without backup:
```bash
python clean_logs.py --no-backup
```

3. Specify different logs directory:
```bash
python clean_logs.py --log-dir=path/to/logs
```

## Data Structure

### Base Property Schema

```json
{
    "_id": ObjectId(),
    "source": "string",  // website source
    "source_id": "string", // original id from source
    "url": "string",
    "title": "string",
    "price": {
        "amount": float,
        "currency": "string",
        "unit": "string"  // per m2, total, etc
    },
    "location": {
        "address": "string",
        "district": "string",
        "city": "string",
        "coordinates": {
            "latitude": float,
            "longitude": float
        }
    },
    "property_type": "string",
    "description": "string",
    "features": {
        "area": float,
        "bedrooms": int,
        "bathrooms": int,
        "floors": int
    },
    "contact_info": {
        "name": "string",
        "phone": "string",
        "email": "string"
    },
    "media": {
        "images": ["string"],
        "videos": ["string"]
    },
    "posted_date": "datetime",
    "crawled_at": "datetime",
    "updated_at": "datetime",
    "status": "string"
}
```

## Configuration

### MongoDB Config (config/mongodb_config.py)
```python
MONGODB_CONFIG = {
    'uri': 'mongodb://localhost:27017',
    'database': 'property_data',
    'collections': {
        'raovat321': 'raovat321_listings',
        'batdongsan': 'batdongsan_listings',
        'nhadat247': 'nhadat247_listings'
    }
}
```

### Scraper Config (config/scraper_config.py)
```python
SCRAPER_CONFIG = {
    'raovat321': {
        'base_url': 'https://raovat321.com/bat-dong-san',
        'listings_url': 'https://raovat321.com/bat-dong-san/p{}',
        'max_pages': 10,
        'delay': 2,
        'timeout': 30,
        'retry_attempts': 3
    },
    'batdongsan': {
        'base_url': 'https://batdongsan.com.vn/ban-nha-dat',
        'listings_url': 'https://batdongsan.com.vn/ban-nha-dat/p{}',
        'max_pages': 10,
        'delay': 2,
        'timeout': 30,
        'retry_attempts': 3
    },
    'nhadat247': {
        'base_url': 'https://nhadat247.com.vn',
        'listings_url': 'https://nhadat247.com.vn/nha-dat-ban/p{}',
        'max_pages': 10,
        'delay': 2,
        'timeout': 30,
        'retry_attempts': 3
    }
}
```

## Development

### Adding New Scraper

1. Create new directory under `scrapers/`
2. Implement required classes extending base scraper:
   - `ListingScraper`
   - `DetailScraper`
3. Add configuration in `config/`
4. Add tests
5. Update documentation

## Error Handling

### MongoDB Errors

1. Connection errors:
```python
try:
    db_client = MongoDBClient()
except PyMongoError as e:
    logger.error(f"MongoDB connection error: {e}")
```

2. Bulk write errors:
```python
try:
    result = collection.bulk_write(operations)
except BulkWriteError as e:
    logger.error(f"Bulk write error: {e.details}")
```

### Logging

- Logs are stored in the `logs/` directory
- Uses RotatingFileHandler with UTF-8 encoding
- Log levels: INFO for regular information, ERROR for errors
- Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

## Features

1. **Data Collection**
   - Multi-source crawling support
   - Rate limiting and request throttling
   - Proxy support for IP rotation

2. **Data Processing**
   - Automatic data cleaning and validation
   - Duplicate detection and removal
   - Data normalization

3. **Storage**
   - MongoDB integration
   - Bulk operations for efficient data storage
   - Automatic retry on connection failures

4. **Monitoring**
   - Detailed logging system
   - Error tracking and reporting
   - Performance metrics collection


### Testing
```bash
# Run all tests
pytest

# Test specific scraper
pytest tests/test_raovat321.py
```

## Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/new-site`
3. Implement scraper following project structure
4. Add tests
5. Submit Pull Request

## Best Practices

1. **Code Style**
   - Follow PEP 8 guidelines
   - Use type hints
   - Write docstrings for functions and classes

2. **Error Handling**
   - Use try-except blocks appropriately
   - Log errors with sufficient context
   - Implement proper cleanup in error cases

3. **Testing**
   - Write unit tests for new features
   - Maintain test coverage
   - Test edge cases and error conditions

## Performance Considerations

1. **Memory Management**
   - Batch processing for large datasets
   - Stream processing where applicable
   - Regular garbage collection

2. **Network Optimization**
   - Connection pooling
   - Request throttling
   - Cache management

3. **Database Operations**
   - Bulk operations for better performance
   - Index optimization
   - Connection pooling


## Contact

- Author: PhuongHX
- Email: phuonghx.me@gmail.com
- GitHub: [phuonghx](https://github.com/phuonghx)