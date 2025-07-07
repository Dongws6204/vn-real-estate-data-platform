import psutil
import time
from datetime import datetime
from typing import Dict, List, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class ScraperMonitor:
    def __init__(self, alert_threshold: Dict = None, email_config: Dict = None):
        self.start_time = datetime.now()
        self.stats = {
            'requests_made': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'items_scraped': 0,
            'errors': [],
            'performance_metrics': []
        }
        self.alert_threshold = alert_threshold or {
            'error_rate': 0.2,  # 20% error rate
            'memory_usage': 85,  # 85% memory usage
            'cpu_usage': 90     # 90% CPU usage
        }
        self.email_config = email_config

    def record_request(self, success: bool, error: Optional[str] = None):
        """Record request statistics"""
        self.stats['requests_made'] += 1
        if success:
            self.stats['successful_requests'] += 1
        else:
            self.stats['failed_requests'] += 1
            if error:
                self.stats['errors'].append({
                    'timestamp': datetime.now(),
                    'error': error
                })
                self._check_alert_conditions()

    def record_item_scraped(self):
        """Record scraped item"""
        self.stats['items_scraped'] += 1

    def record_performance(self):
        """Record system performance metrics"""
        metrics = {
            'timestamp': datetime.now(),
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent
        }
        self.stats['performance_metrics'].append(metrics)
        self._check_performance_alerts(metrics)

    def get_summary(self) -> Dict:
        """Get summary of scraping statistics"""
        duration = (datetime.now() - self.start_time).total_seconds()
        error_rate = (self.stats['failed_requests'] / self.stats['requests_made'] 
                     if self.stats['requests_made'] > 0 else 0)
        
        return {
            'duration_seconds': duration,
            'requests_per_second': self.stats['requests_made'] / duration if duration > 0 else 0,
            'success_rate': 1 - error_rate,
            'items_scraped': self.stats['items_scraped'],
            'total_errors': len(self.stats['errors']),
            'latest_performance': self.stats['performance_metrics'][-1] if self.stats['performance_metrics'] else None
        }

    def _check_alert_conditions(self):
        """Check if alert conditions are met"""
        error_rate = (self.stats['failed_requests'] / self.stats['requests_made'] 
                     if self.stats['requests_made'] > 0 else 0)
        
        if error_rate > self.alert_threshold['error_rate']:
            self._send_alert(
                'High Error Rate Alert',
                f'Error rate has reached {error_rate:.2%}, exceeding threshold of {self.alert_threshold["error_rate"]:.2%}'
            )

    def _check_performance_alerts(self, metrics: Dict):
        """Check performance metrics for alert conditions"""
        if metrics['cpu_usage'] > self.alert_threshold['cpu_usage']:
            self._send_alert(
                'High CPU Usage Alert',
                f'CPU usage has reached {metrics["cpu_usage"]}%, exceeding threshold of {self.alert_threshold["cpu_usage"]}%'
            )
            
        if metrics['memory_usage'] > self.alert_threshold['memory_usage']:
            self._send_alert(
                'High Memory Usage Alert',
                f'Memory usage has reached {metrics["memory_usage"]}%, exceeding threshold of {self.alert_threshold["memory_usage"]}%'
            )

    def _send_alert(self, subject: str, message: str):
        """Send alert email"""
        if not self.email_config:
            logger.warning(f"Alert triggered but no email config provided: {subject}")
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from']
            msg['To'] = self.email_config['to']
            msg['Subject'] = subject

            msg.attach(MIMEText(message, 'plain'))

            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                if self.email_config.get('use_tls'):
                    server.starttls()
                if self.email_config.get('username') and self.email_config.get('password'):
                    server.login(self.email_config['username'], self.email_config['password'])
                server.send_message(msg)

            logger.info(f"Alert sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send alert email: {str(e)}")