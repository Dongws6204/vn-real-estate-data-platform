from typing import Dict, Optional, List
from datetime import datetime
import re
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class PropertyDataValidator:
    def __init__(self):
        self.required_fields = {
            'source', 'source_id', 'url', 'title', 'price', 
            'location', 'crawled_at'
        }
        
        self.price_pattern = re.compile(r'^[\d,.]+$')
        self.phone_pattern = re.compile(r'^\+?[\d\s-]{10,}$')
        self.email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
        
    def validate_listing(self, data: Dict) -> tuple[bool, List[str]]:
        """Validate listing data"""
        errors = []
        
        # Check required fields
        missing_fields = self.required_fields - set(data.keys())
        if missing_fields:
            errors.append(f"Missing required fields: {missing_fields}")
            
        # Validate individual fields
        if 'url' in data and not self._validate_url(data['url']):
            errors.append("Invalid URL format")
            
        if 'price' in data:
            price_errors = self._validate_price(data['price'])
            errors.extend(price_errors)
            
        if 'location' in data:
            location_errors = self._validate_location(data['location'])
            errors.extend(location_errors)
            
        if 'contact_info' in data:
            contact_errors = self._validate_contact_info(data['contact_info'])
            errors.extend(contact_errors)
            
        return len(errors) == 0, errors
    
    def clean_data(self, data: Dict) -> Dict:
        """Clean and normalize data"""
        cleaned = data.copy()
        
        # Clean text fields
        text_fields = ['title', 'description']
        for field in text_fields:
            if field in cleaned:
                cleaned[field] = self._clean_text(cleaned[field])
                
        # Normalize price
        if 'price' in cleaned:
            cleaned['price'] = self._normalize_price(cleaned['price'])
            
        # Clean contact info
        if 'contact_info' in cleaned:
            cleaned['contact_info'] = self._clean_contact_info(cleaned['contact_info'])
            
        return cleaned
    
    def _validate_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            pattern = re.compile(
                r'^https?://'  # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            return bool(pattern.match(url))
        except:
            return False
            
    def _validate_price(self, price: Dict) -> List[str]:
        """Validate price data"""
        errors = []
        required_price_fields = {'amount', 'currency', 'unit'}
        
        if not all(field in price for field in required_price_fields):
            errors.append("Missing required price fields")
            
        if 'amount' in price and not isinstance(price['amount'], (int, float)):
            errors.append("Invalid price amount")
            
        return errors
        
    def _validate_location(self, location: Dict) -> List[str]:
        """Validate location data"""
        errors = []
        if 'coordinates' in location:
            coords = location['coordinates']
            if not isinstance(coords.get('latitude'), (int, float)):
                errors.append("Invalid latitude")
            if not isinstance(coords.get('longitude'), (int, float)):
                errors.append("Invalid longitude")
                
        return errors
        
    def _validate_contact_info(self, contact: Dict) -> List[str]:
        """Validate contact information"""
        errors = []
        
        if 'phone' in contact and not self.phone_pattern.match(contact['phone']):
            errors.append("Invalid phone number format")
            
        if 'email' in contact and not self.email_pattern.match(contact['email']):
            errors.append("Invalid email format")
            
        return errors
        
    def _clean_text(self, text: str) -> str:
        """Clean text content"""
        if not text:
            return ""
        # Remove extra whitespace
        text = " ".join(text.split())
        # Remove special characters
        text = re.sub(r'[^\w\s\-.,]', '', text)
        return text.strip()
        
    def _normalize_price(self, price: Dict) -> Dict:
        """Normalize price data"""
        if isinstance(price, str):
            # Convert string price to structured format
            amount = float(re.sub(r'[^\d.]', '', price))
            return {
                'amount': amount,
                'currency': 'VND',
                'unit': 'total'
            }
        return price
        
    def _clean_contact_info(self, contact: Dict) -> Dict:
        """Clean contact information"""
        cleaned = {}
        if 'phone' in contact:
            cleaned['phone'] = re.sub(r'[^\d+]', '', contact['phone'])
        if 'email' in contact:
            cleaned['email'] = contact['email'].lower().strip()
        if 'name' in contact:
            cleaned['name'] = self._clean_text(contact['name'])
        return cleaned