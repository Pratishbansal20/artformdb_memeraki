"""
Data models for the artform upload system
"""

import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from enum import Enum

class HeritageLevel(Enum):
    """Enumeration for heritage levels"""
    UNESCO = "UNESCO"
    NATIONAL = "National"
    STATE = "State"
    LOCAL = "Local"
    UNRECOGNIZED = "Unrecognized"

@dataclass
class ArtformData:
    """Data class representing an artform record"""
    name: str
    slug: str
    description: str = ""
    origin_region: List[str] = field(default_factory=list)
    heritage_level: str = ""
    thumbnail_url: str = ""
    banner_image_url: str = ""
    category: str = ""
    materials_used: List[str] = field(default_factory=list)
    colours_used: List[str] = field(default_factory=list)
    related_art_form_ids: List[str] = field(default_factory=list)
    artist_ids: List[str] = field(default_factory=list)
    total_value_sold: float = 0.0
    artist_count: int = 0
    total_unit_sold: int = 0
    
    def __post_init__(self):
        """Validate data after initialization"""
        if not self.name.strip():
            raise ValueError("Name cannot be empty")
        if not self.slug.strip():
            raise ValueError("Slug cannot be empty")
    
    def to_firestore_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for Firestore"""
        return asdict(self)
    
    def is_valid(self) -> bool:
        """Check if the artform data is valid"""
        return bool(self.name.strip() and self.slug.strip())

@dataclass
class ProcessResult:
    """Result of processing a single document"""
    doc_id: str
    success: bool
    error_message: Optional[str] = None
    row_number: int = 0
    processing_time: float = 0.0
    is_new_document: bool = False

@dataclass
class UploadStats:
    """Statistics for upload process"""
    success: int = 0
    errors: int = 0
    skipped: int = 0
    total_processed: int = 0
    new_documents: int = 0
    updated_documents: int = 0
    start_time: float = field(default_factory=time.time)
    
    @property
    def duration(self) -> float:
        """Calculate duration since start"""
        return time.time() - self.start_time
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.total_processed == 0:
            return 0.0
        return (self.success / self.total_processed) * 100
    
    @property
    def rate_per_second(self) -> float:
        """Calculate processing rate per second"""
        duration = self.duration
        if duration == 0:
            return 0.0
        return self.total_processed / duration
    
    def __str__(self) -> str:
        """String representation of stats"""
        return (f"Success: {self.success}, Errors: {self.errors}, "
                f"Skipped: {self.skipped}, Rate: {self.rate_per_second:.2f}/sec")