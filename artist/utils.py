"""
Utility functions for the artist upload system
"""

import logging
import sys
from datetime import datetime
from typing import List, Any, Generator  # Added Generator import
from .config import LoggingConfig

def setup_logging(config: LoggingConfig) -> logging.Logger:
    """Set up logging configuration"""
    logger = logging.getLogger('artist_uploader')
    logger.setLevel(getattr(logging, config.level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(config.log_format)
    
    # Console handler
    if config.console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if config.file_output:
        log_filename = f"artist_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_path = f"{config.log_dir}/{log_filename}"
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def parse_array(field: str) -> List[str]:
    """Parse pipe-separated values into array"""
    if not field or field.strip() == "":
        return []
    return [item.strip() for item in field.split("|") if item.strip()]

def safe_float(value: str, default: float = 0.0) -> float:
    """Safely convert string to float"""
    try:
        return float(value) if value and value.strip() else default
    except (ValueError, TypeError):
        return default

def safe_int(value: str, default: int = 0) -> int:
    """Safely convert string to int"""
    try:
        return int(value) if value and value.strip() else default
    except (ValueError, TypeError):
        return default

def safe_bool(value: str, default: bool = False) -> bool:
    """Safely convert string to bool"""
    if not value or not value.strip():
        return default
    
    value = value.strip().lower()
    if value in ('true', '1', 'yes', 'on', 'enabled', 'active'):
        return True
    elif value in ('false', '0', 'no', 'off', 'disabled', 'inactive'):
        return False
    else:
        return default

def chunk_list(data: List[Any], chunk_size: int) -> Generator[List[Any], None, None]:  # Changed return type from List[List[Any]]
    """Split list into chunks of specified size"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Alternative version if you want to return a list instead of generator:
def chunk_list_as_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks of specified size, returning a list"""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]