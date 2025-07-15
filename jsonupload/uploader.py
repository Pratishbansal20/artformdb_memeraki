"""
Main upload orchestration logic (JSON uploader for GenAI top products) — updated for your JSON structure.
"""

import os
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Tuple

from .config import UploadConfig
from .models import UploadStats, ProcessResult
from .database import FirestoreManager

logger = logging.getLogger('genai_json_uploader.uploader')

class GenAIProductUploader:
    """
    Uploader class that reads GenAI top-product JSON files and uploads data to Firestore.
    Accepts files where each JSON contains:
    {
      "slug": ...,
      "products": [
        {"name": ..., "description": ..., "size_inches": ..., "price_inr": ...},
        ...
      ]
    }
    """

    def __init__(self, config: UploadConfig):
        self.config = config
        self.db_manager = FirestoreManager(config.firebase, config.processing)
        self.stats = UploadStats()
        self.stats_lock = Lock()
        self.json_folder = config.json_folder

    def upload_top_products(self) -> UploadStats:
        logger.info("Starting GenAI top product upload from JSON files...")
        logger.info(f"Worker configuration: {self.config.processing.max_workers} threads, batch size: {self.config.processing.batch_size}")
        logger.info(f"Input directory: {self.json_folder}")

        try:
            product_batches = self._load_all_json()
            logger.info(f"Found {len(product_batches)} products in JSON files")

            current_batch = []
            for slug, product in product_batches:
                current_batch.append((slug, product))
                if len(current_batch) >= self.config.processing.batch_size:
                    self._process_batch(current_batch)
                    current_batch = []

                    if self.config.processing.batch_delay > 0:
                        logger.debug(f"Delaying next batch by {self.config.processing.batch_delay}s")
                        time.sleep(self.config.processing.batch_delay)

            if current_batch:
                self._process_batch(current_batch)

            self._log_final_stats()
            return self.stats

        except Exception as e:
            logger.error(f"Critical error during upload: {e}")
            raise

    def _load_all_json(self) -> List[Tuple[str, dict]]:
        """
        Load and parse all top-product JSON files in the given folder.
        Assumes structure:
            {
              "slug": "kalamkari",
              "products": [
                {
                    "name": "...",
                    "description": "...",
                    "size_inches": "...",
                    "price_inr": "..."
                },
                ...
              ]
            }
        Returns:
            List of tuples: (slug, product_dict)
        """
        product_data = []
        for fname in os.listdir(self.json_folder):
            if fname.endswith('_top_products.json'):
                json_path = os.path.join(self.json_folder, fname)
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        slug = content.get("slug")
                        if not slug:
                            slug = fname.replace('_top_products.json', '')
                        products = content.get("products", []) or content.get("columns", [])
                        for product in products:
                            product_data.append((slug, product))
                except Exception as e:
                    logger.warning(f"Skipping file {fname} due to error: {e}")
                    with self.stats_lock:
                        self.stats.skipped += 1
        return product_data

    def _process_batch(self, batch_data: List[Tuple[str, dict]]):
        logger.info(f"Processing batch of {len(batch_data)} products with {self.config.processing.max_workers} threads...")

        with ThreadPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
            future_to_data = {
                executor.submit(self._upload_product_document, slug, product): (slug, product)
                for slug, product in batch_data
            }
            for future in as_completed(future_to_data):
                slug, product = future_to_data[future]
                try:
                    result = future.result()
                    self._update_stats(result)
                except Exception as e:
                    logger.error(f"Unexpected error uploading product for '{slug}': {e}")
                    with self.stats_lock:
                        self.stats.errors += 1
                        self.stats.total_processed += 1

    def _upload_product_document(self, slug: str, product: dict) -> ProcessResult:
        """
        Upload a product document with keys matching your JSON ("name", "description", etc.)
        Fields will be stored as-is in Firestore: artform_top_products/<slug>/products/<name>
        """
        start_time = time.time()
        try:
            doc_ref = self.db_manager.db.collection(self.config.firebase.collection_name).document(slug)
            product_name = product.get("name")
            if not product_name:
                raise ValueError("Product missing required 'name' field.")
            prod_ref = doc_ref.collection("products").document(product_name)
            prod_ref.set({
                "description": product.get("description", ""),
                "size_inches": product.get("size_inches", ""),
                "price_inr": product.get("price_inr", "")
            }, merge=True)
            processing_time = time.time() - start_time
            logger.info(f"Uploaded product '{product_name}' under '{slug}'")
            return ProcessResult(
                doc_id=product_name,
                success=True,
                processing_time=processing_time,
                is_new_document=False
            )
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Failed to upload product '{product.get('name', 'unknown')}' under '{slug}': {str(e)}"
            logger.error(error_msg)
            return ProcessResult(
                doc_id=product.get("name", "unknown"),
                success=False,
                error_message=error_msg,
                processing_time=processing_time
            )

    def _update_stats(self, result: ProcessResult):
        with self.stats_lock:
            if result.success:
                self.stats.success += 1
                action = "CREATED" if result.is_new_document else "UPDATED"
                logger.info(f"{action}: {result.doc_id}")
            else:
                self.stats.errors += 1
                logger.error(f"FAILED: {result.doc_id} - {result.error_message}")
            self.stats.total_processed += 1

    def _log_final_stats(self):
        logger.info("=" * 60)
        logger.info("UPLOAD COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Successfully processed: {self.stats.success} records")
        logger.info(f"New documents CREATED: {self.stats.new_documents}")
        logger.info(f"Existing documents UPDATED: {self.stats.updated_documents}")
        logger.info(f"Errors encountered: {self.stats.errors} records")
        logger.info(f"Skipped records: {self.stats.skipped} records")
        logger.info(f"Total processed: {self.stats.total_processed} records")
        logger.info(f"Total duration: {self.stats.duration:.2f} seconds")
        logger.info(f"Average rate: {self.stats.rate_per_second:.2f} records/second")
        logger.info(f"Threading: Using {self.config.processing.max_workers} worker threads")
        logger.info(f"Batch size: {self.config.processing.batch_size} records per batch")
