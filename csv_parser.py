"""
CSV parsing functionality for artform data
"""

import csv
import logging
from typing import List, Dict, Iterator, Any
from .models import ArtformData
from .utils import parse_array, safe_float, safe_int

logger = logging.getLogger('artform_uploader.csv_parser')

class CSVParser:
    """Handles CSV parsing and data validation"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.csv_headers = None
    
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
                self.csv_headers = reader.fieldnames
                logger.info(f"CSV headers found: {self.csv_headers}")
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        artform = self._create_artform_from_row(row)
                        if artform:
                            yield (row_num, artform)
                        else:
                            yield (row_num, None)
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
        """Create ArtformData from CSV row with dynamic field mapping"""
        artform = ArtformData()
        
        # Process each column in the CSV
        for header, value in row.items():
            if header and value is not None:
                header = header.strip()
                value = str(value).strip() if value else ""
                
                # Skip empty values
                if not value:
                    continue
                
                # Convert value based on field type
                converted_value = self._convert_value(header, value)
                
                # Set the field value and mark it for update
                if hasattr(artform, header):
                    artform.set_field_value(header, converted_value)
                else:
                    logger.warning(f"Unknown field '{header}' in CSV, skipping")
        
        # Validate that we have at least the required fields
        if not artform.slug:
            logger.error("Missing required field 'slug' in CSV row")
            return None
        
        return artform
    
    def _convert_value(self, field_name: str, value: str) -> Any:
        """Convert string value to appropriate type based on field"""
        if not value:
            return None
        
        # Get the field type from ArtformData
        field_type = ArtformData._field_types.get(field_name)
        
        if field_type == list:
            return parse_array(value)
        elif field_type == float:
            return safe_float(value)
        elif field_type == int:
            return safe_int(value)
        else:
            # Default to string
            return value
    
    def get_total_rows(self) -> int:
        """Get total number of rows in CSV (excluding header)"""
        try:
            with open(self.csv_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                return sum(1 for _ in reader)
        except Exception as e:
            logger.error(f"Error counting CSV rows: {str(e)}")
            return 0
    
    def get_csv_headers(self) -> List[str]:
        """Get the headers from the CSV file"""
        if self.csv_headers is None:
            try:
                with open(self.csv_path, mode='r', encoding='utf-8-sig') as csvfile:
                    reader = csv.DictReader(csvfile)
                    self.csv_headers = reader.fieldnames or []
            except Exception as e:
                logger.error(f"Error reading CSV headers: {str(e)}")
                self.csv_headers = []
        
        return self.csv_headers or []