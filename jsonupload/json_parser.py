import os
import json
import logging
from typing import List, Dict, Iterator, Tuple, Any

logger = logging.getLogger('genai_json_uploader.json_parser')

class JSONProductParser:
    """
    Handles batch parsing and validation of GenAI product JSON files.
    """

    def __init__(self, json_folder: str):
        self.json_folder = json_folder
        self.files = [
            fname for fname in os.listdir(json_folder)
            if fname.endswith('_top_products.json')
        ]
        self.fieldnames = ["name", "description", "size_inches", "price_inr"]

    def parse_json(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Yields (slug, product_dict) tuples for each product in the JSON files.
        Yields:
            (slug: str, product: dict)
        """
        logger.info(f"Parsing JSON files in: {self.json_folder}")
        for fname in self.files:
            slug = fname.replace('_top_products.json', '')
            json_path = os.path.join(self.json_folder, fname)
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    product = data.get("product", [])
                    for row_num, product in enumerate(product, start=1):
                        record = self._validate_and_transform(product)
                        if record is not None:
                            yield (slug, record)
                        else:
                            logger.warning(f"[{slug}] Skipped product at row {row_num}: missing or invalid fields.")
            except Exception as e:
                logger.error(f"[{slug}] Error parsing file '{fname}': {e}")
                continue

    def _validate_and_transform(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a cleaned/validated product dict or None if required fields are missing.
        """
        name = row.get("name", "").strip()
        if not name:
            return None
        # Map and clean fields; adapt as needed for downstream Firestore upload
        return {
            "name": name,
            "description": row.get("description", "").strip(),
            "size_inches": row.get("size_inches", "").strip(),
            "price_inr": row.get("price_inr", "").strip()
        }

    def get_total_records(self) -> int:
        """
        Get the total number of product records across all JSON files.
        """
        total = 0
        for fname in self.files:
            try:
                with open(os.path.join(self.json_folder, fname), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    total += len(data.get("product", []))
            except Exception:
                continue
        return total

    def get_fieldnames(self) -> List[str]:
        """
        Return the expected product fieldnames.
        """
        return self.fieldnames
