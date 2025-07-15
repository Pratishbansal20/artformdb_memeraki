from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Set
import time

@dataclass
class ProductData:
    """
    Data class representing a single product entry from a GenAI JSON file.
    """
    name: str = ""
    description: str = ""
    size_inches: str = ""
    price_inr: str = ""

    _fields_to_update: Set[str] = field(default_factory=set, init=False)

    def set_field_value(self, field_name: str, value: Any):
        if hasattr(self, field_name):
            setattr(self, field_name, value)
            self._fields_to_update.add(field_name)

    def to_firestore_dict(self) -> Dict[str, Any]:
        # Include only fields explicitly set, or all if none are marked.
        if not self._fields_to_update:
            return {
                "name": self.name,
                "description": self.description,
                "size_inches": self.size_inches,
                "price_inr": self.price_inr
            }
        return {field: getattr(self, field) for field in self._fields_to_update if hasattr(self, field)}
    
    def is_valid(self) -> bool:
        return bool(self.name and self.name.strip())
    
    def get_fields_to_update(self) -> Set[str]:
        return self._fields_to_update.copy()

@dataclass
class ProcessResult:
    """
    Result of processing a single product upload.
    """
    doc_id: str
    success: bool
    error_message: Optional[str] = None
    processing_time: float = 0.0
    is_new_document: bool = False
    fields_updated: Set[str] = field(default_factory=set)

@dataclass
class UploadStats:
    """
    Statistics tracker for the entire JSON upload process.
    """
    success: int = 0
    errors: int = 0
    skipped: int = 0
    total_processed: int = 0
    new_documents: int = 0
    updated_documents: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        return time.time() - self.start_time

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return (self.success / self.total_processed) * 100

    @property
    def rate_per_second(self) -> float:
        duration = self.duration
        if duration == 0:
            return 0.0
        return self.total_processed / duration

    def __str__(self) -> str:
        return (
            f"Success: {self.success}, Errors: {self.errors}, "
            f"Skipped: {self.skipped}, Rate: {self.rate_per_second:.2f}/sec"
        )
