__version__ = "1.0.0"
__author__ = "Your Name"

from .config import UploadConfig, LoggingConfig
from .models import ArtistData, UploadStats, ProcessResult
from .uploader import ArtistUploader
from .main import main

__all__ = [
    'UploadConfig',
    'LoggingConfig', 
    'ArtistData',
    'UploadStats',
    'ProcessResult',
    'ArtistUploader',
    'main'
]