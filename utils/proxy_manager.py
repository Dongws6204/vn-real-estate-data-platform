import random
import requests
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class ProxyManager:
    def __init__(self, proxy_list: List[str] = None):
        self.proxies = []
        self.working_proxies = {}
        self.failed_proxies = {}
        self.proxy_timeout = 10
        self.max_failures = 3
        self.check_interval = timedelta(minutes=30)
        
        if proxy_list:
            self.add_proxies(proxy_list)

    def add_proxies(self, proxy_list: List[str]):
        """Add new proxies to the pool"""
        for proxy in proxy_list:
            if self._check_proxy(proxy):
                self.proxies.append(proxy)
                self.working_proxies[proxy] = {
                    'failures': 0,
                    'last_check': datetime.now(),
                    'success_rate': 100.0
                }

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get a working proxy with formatted structure"""
        if not self.proxies:
            return None

        working_proxies = [p for p in self.proxies 
                         if self.working_proxies[p]['failures'] < self.max_failures]
        
        if not working_proxies:
            self._refresh_proxies()
            return None

        proxy = random.choice(working_proxies)
        return {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }

    def report_failure(self, proxy: str):
        """Report a proxy failure"""
        if proxy in self.working_proxies:
            self.working_proxies[proxy]['failures'] += 1
            self.working_proxies[proxy]['success_rate'] *= 0.9

            if self.working_proxies[proxy]['failures'] >= self.max_failures:
                self.failed_proxies[proxy] = datetime.now()
                self.proxies.remove(proxy)
                del self.working_proxies[proxy]

    def report_success(self, proxy: str):
        """Report a proxy success"""
        if proxy in self.working_proxies:
            self.working_proxies[proxy]['failures'] = max(0, self.working_proxies[proxy]['failures'] - 1)
            self.working_proxies[proxy]['success_rate'] = min(100.0, self.working_proxies[proxy]['success_rate'] * 1.1)
            self.working_proxies[proxy]['last_check'] = datetime.now()

    def _check_proxy(self, proxy: str) -> bool:
        """Check if a proxy is working"""
        try:
            test_url = 'http://httpbin.org/ip'
            proxies = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
            response = requests.get(test_url, proxies=proxies, timeout=self.proxy_timeout)
            return response.status_code == 200
        except:
            return False

    def _refresh_proxies(self):
        """Refresh failed proxies after check_interval"""
        current_time = datetime.now()
        restored_proxies = []

        for proxy, failed_time in list(self.failed_proxies.items()):
            if current_time - failed_time > self.check_interval:
                if self._check_proxy(proxy):
                    restored_proxies.append(proxy)
                    del self.failed_proxies[proxy]

        if restored_proxies:
            self.add_proxies(restored_proxies)