import time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class RateLimiter:
    def __init__(self, requests_per_second: int = 2, burst_limit: int = 5):
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit
        self.request_times = defaultdict(list)
        
    def wait(self, domain: str):
        """Wait if necessary to respect rate limits"""
        current_time = datetime.now()
        domain_requests = self.request_times[domain]
        
        # Remove old requests
        while domain_requests and current_time - domain_requests[0] > timedelta(seconds=1):
            domain_requests.pop(0)
            
        # Check if we need to wait
        if len(domain_requests) >= self.burst_limit:
            sleep_time = (domain_requests[0] + timedelta(seconds=1) - current_time).total_seconds()
            if sleep_time > 0:
                logger.debug(f"Rate limit reached for {domain}. Waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
        # Add current request
        self.request_times[domain].append(current_time)
        
    def reset(self, domain: Optional[str] = None):
        """Reset rate limiting for a domain or all domains"""
        if domain:
            self.request_times[domain] = []
        else:
            self.request_times.clear()