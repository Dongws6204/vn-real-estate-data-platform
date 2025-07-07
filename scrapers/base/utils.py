import re
from typing import Optional, Dict
from datetime import datetime

def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.strip().split())

def extract_number(text: str) -> Optional[float]:
    try:
        return float(re.sub(r'[^\d.]', '', text))
    except:
        return None

def parse_date(date_str: str, format: str = "%d/%m/%Y") -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, format)
    except:
        return None

def normalize_price(price_str: str) -> Dict:
    """Convert various price formats to standardized format"""
    price_data = {
        "amount": None,
        "currency": "VND",
        "unit": "total"
    }
    
    if not price_str:
        return price_data

    try:
        # Remove all spaces and convert to lowercase
        price_str = price_str.lower().replace(' ', '')
        
        # Extract numeric value
        amount = extract_number(price_str)
        
        if amount is None:
            return price_data
            
        # Determine unit and adjust amount
        if 'tỷ' in price_str:
            amount *= 1000000000
            price_data['unit'] = 'billion'
        elif 'triệu' in price_str:
            amount *= 1000000
            price_data['unit'] = 'million'
        elif 'nghìn' in price_str:
            amount *= 1000
            price_data['unit'] = 'thousand'
            
        price_data['amount'] = amount
        
        # Determine currency (default is VND)
        if 'usd' in price_str or '$' in price_str:
            price_data['currency'] = 'USD'
            
    except Exception:
        pass
        
    return price_data