"""
Configuration management for the artist upload system
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

@dataclass
class LoggingConfig:
    """Configuration for logging"""
    level: str = "INFO"
    log_dir: str = "logs"
    console_output: bool = True
    file_output: bool = True
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def __post_init__(self):
        """Validate logging configuration"""
        if self.file_output:
            os.makedirs(self.log_dir, exist_ok=True)

@dataclass
class FirebaseConfig:
    """Firebase configuration"""
    credentials_path: str
    project_id: Optional[str] = None
    collection_name: str = "artists"
    
    def __post_init__(self):
        """Validate Firebase configuration"""
        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"Firebase credentials not found at {self.credentials_path}")

@dataclass
class ProcessingConfig:
    """Processing configuration"""
    max_workers: int = 10
    batch_size: int = 500
    retry_attempts: int = 5
    retry_min_wait: int = 4
    retry_max_wait: int = 60
    batch_delay: float = 0.1
    
    def __post_init__(self):
        """Validate processing configuration"""
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.retry_attempts <= 0:
            raise ValueError("retry_attempts must be positive")

@dataclass
class UploadConfig:
    """Main configuration class combining all configs"""
    csv_path: str
    firebase: FirebaseConfig
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    def __post_init__(self):
        """Validate main configuration"""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV file not found at {self.csv_path}")
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'UploadConfig':
        """Create UploadConfig from dictionary"""
        firebase_config = FirebaseConfig(**config_dict.get('firebase', {}))
        processing_config = ProcessingConfig(**config_dict.get('processing', {}))
        logging_config = LoggingConfig(**config_dict.get('logging', {}))
        
        return cls(
            csv_path=config_dict['csv_path'],
            firebase=firebase_config,
            processing=processing_config,
            logging=logging_config
        )