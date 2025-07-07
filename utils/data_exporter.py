import csv
import json
import pandas as pd
from typing import List, Dict
from datetime import datetime
from pathlib import Path
from config.logging_config import setup_logger

logger = setup_logger(__name__)

class DataExporter:
    def __init__(self, export_dir: str = "exports"):
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def export_to_csv(self, data: List[Dict], filename: str = None) -> str:
        """Export data to CSV file"""
        try:
            if not filename:
                filename = f"property_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            filepath = self.export_dir / filename
            df = pd.DataFrame(self._flatten_data(data))
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            logger.info(f"Successfully exported {len(data)} records to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            raise

    def export_to_json(self, data: List[Dict], filename: str = None) -> str:
        """Export data to JSON file"""
        try:
            if not filename:
                filename = f"property_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            filepath = self.export_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Successfully exported {len(data)} records to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Error exporting to JSON: {str(e)}")
            raise

    def export_to_excel(self, data: List[Dict], filename: str = None) -> str:
        """Export data to Excel file"""
        try:
            if not filename:
                filename = f"property_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            filepath = self.export_dir / filename
            df = pd.DataFrame(self._flatten_data(data))
            df.to_excel(filepath, index=False, engine='openpyxl')
            
            logger.info(f"Successfully exported {len(data)} records to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Error exporting to Excel: {str(e)}")
            raise

    def _flatten_data(self, data: List[Dict]) -> List[Dict]:
        """Flatten nested dictionaries for export"""
        flattened_data = []
        
        for item in data:
            flat_item = {}
            for key, value in item.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        flat_item[f"{key}_{sub_key}"] = sub_value
                else:
                    flat_item[key] = value
            flattened_data.append(flat_item)
            
        return flattened_data