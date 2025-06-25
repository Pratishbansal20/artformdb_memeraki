"""
CSV parsing functionality for artform data
"""

import csv
import logging
from typing import List, Dict, Iterator
from .models import ArtformData
from .utils import parse_array, safe_float, safe_int

logger = logging.getLogger('artform_uploader.csv_parser')

class CSVParser:
    """Handles CSV parsing and data validation"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
    
    def parse_csv(self) -> Iterator[tuple]:
        """
        Parse CSV file and yield (row_number, ArtformData) tuples
        
        Yields:
            Tuple of (row_number, ArtformData or None)
        """
        logger.info(f"Starting to parse CSV file: {self.csv_path}")
        
        try:
            with open(self.csv_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        artform = self._create_artform_from_row(row)
                        yield (row_num, artform)
                    except ValueError as e:
                        logger.warning(f"Row {row_num}: Invalid data - {str(e)}")
                        yield (row_num, None)
                    except Exception as e:
                        logger.error(f"Row {row_num}: Unexpected error - {str(e)}")
                        yield (row_num, None)
        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.csv_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
    
    def _create_artform_from_row(self, row: Dict[str, str]) -> ArtformData:
        """Create ArtformData from CSV row"""
        return ArtformData(
            name=row.get("name", "").strip(),
            slug=row.get("slug", "").strip(),
            description=row.get("description", "").strip(),
            origin_region=parse_array(row.get("origin_region", "")),
            heritage_level=row.get("heritage_level", "").strip(),
            thumbnail_url=row.get("thumbnail_url", "").strip(),
            banner_image_url=row.get("banner_image_url", "").strip(),
            category=row.get("category", "").strip(),
            materials_used=parse_array(row.get("materials_used", "")),
            colours_used=parse_array(row.get("colours_used", "")),
            related_art_form_ids=parse_array(row.get("related_art_form_ids", "")),
            artist_ids=parse_array(row.get("artist_ids", "")),
            total_value_sold=safe_float(row.get("total_value_sold", "")),
            artist_count=safe_int(row.get("artist_count", "")),
            total_unit_sold=safe_int(row.get("total_unit_sold", ""))
        )
    
    def get_total_rows(self) -> int:
        """Get total number of rows in CSV (excluding header)"""
        try:
            with open(self.csv_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                return sum(1 for _ in reader)
        except Exception as e:
            logger.error(f"Error counting CSV rows: {str(e)}")
            return 0
