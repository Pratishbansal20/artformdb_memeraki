"""
Data models for the artist upload system
"""

import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Set
from enum import Enum
from datetime import datetime

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
    name: str = ""
    slug: str = ""
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
    
    # Track which fields were actually set from CSV
    _fields_to_update: Set[str] = field(default_factory=set, init=False)
    
    # Field type mapping for proper conversion
    _field_types = {
        'origin_region': list,
        'materials_used': list,
        'colours_used': list,
        'related_art_form_ids': list,
        'artist_ids': list,
        'total_value_sold': float,
        'artist_count': int,
        'total_unit_sold': int
    }
    
    def __post_init__(self):
        """Validate data after initialization"""
        if self.slug and not self.slug.strip():
            raise ValueError("Slug cannot be empty")
    
    def set_field_value(self, field_name: str, value: Any):
        """Set a field value and mark it for update"""
        if hasattr(self, field_name):
            setattr(self, field_name, value)
            self._fields_to_update.add(field_name)
    
    def to_firestore_dict(self) -> Dict[str, Any]:
        """Convert only the fields marked for update to dictionary suitable for Firestore"""
        if not self._fields_to_update:
            # If no specific fields marked, return all fields (backward compatibility)
            return asdict(self)
        
        # Only include fields that were explicitly set
        result = {}
        for field_name in self._fields_to_update:
            if hasattr(self, field_name):
                value = getattr(self, field_name)
                # Don't include private fields
                if not field_name.startswith('_'):
                    result[field_name] = value
        
        return result
    
    def is_valid(self) -> bool:
        """Check if the artform data is valid"""
        return bool(self.slug and self.slug.strip())
    
    def get_fields_to_update(self) -> Set[str]:
        """Get the set of fields that should be updated"""
        return self._fields_to_update.copy()

@dataclass
class ArtistData:
    """Data class representing an artist record"""
    # Basic Info
    name: str = ""
    slug: str = ""
    bio: str = ""
    profile_image_url: str = ""
    cover_image_url: str = ""
    
    # Location & Background
    city: str = ""
    state: str = ""
    country: str = "India"
    language: List[str] = field(default_factory=list)
    years_of_practice: int = 0
    
    # Art Form Links
    art_form_ids: List[str] = field(default_factory=list)
    specialization: str = ""
    awards: List[str] = field(default_factory=list)
    highlights: List[str] = field(default_factory=list)
    
    # Status and Business Analytics
    is_active: bool = True
    total_value_sold: float = 0.0
    total_units_sold: int = 0
    last_sale_date: Optional[str] = None
    bank_account: str = ""
    
    # Social & Contact
    instagram_handle: str = ""
    website_url: str = ""
    contact_number: str = ""
    whatsapp_enabled: bool = False
    
    # Track which fields were actually set from CSV
    _fields_to_update: Set[str] = field(default_factory=set, init=False)
    
    # Field type mapping for proper conversion
    _field_types = {
        'language': list,
        'art_form_ids': list,
        'awards': list,
        'highlights': list,
        'years_of_practice': int,
        'total_value_sold': float,
        'total_units_sold': int,
        'is_active': bool,
        'whatsapp_enabled': bool
    }
    
    def __post_init__(self):
        """Validate data after initialization"""
        if self.slug and not self.slug.strip():
            raise ValueError("Slug cannot be empty")
    
    def set_field_value(self, field_name: str, value: Any):
        """Set a field value and mark it for update"""
        if hasattr(self, field_name):
            setattr(self, field_name, value)
            self._fields_to_update.add(field_name)
    
    def to_firestore_dict(self) -> Dict[str, Any]:
        """Convert only the fields marked for update to dictionary suitable for Firestore"""
        if not self._fields_to_update:
            # If no specific fields marked, return all fields (backward compatibility)
            return asdict(self)
        
        # Only include fields that were explicitly set
        result = {}
        for field_name in self._fields_to_update:
            if hasattr(self, field_name):
                value = getattr(self, field_name)
                # Don't include private fields
                if not field_name.startswith('_'):
                    result[field_name] = value
        
        return result
    
    def is_valid(self) -> bool:
        """Check if the artist data is valid"""
        return bool(self.slug and self.slug.strip())
    
    def get_fields_to_update(self) -> Set[str]:
        """Get the set of fields that should be updated"""
        return self._fields_to_update.copy()

@dataclass
class ProcessResult:
    """Result of processing a single document"""
    doc_id: str
    success: bool
    error_message: Optional[str] = None
    row_number: int = 0
    processing_time: float = 0.0
    is_new_document: bool = False
    fields_updated: Set[str] = field(default_factory=set)

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