"""
Main upload orchestration logic
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Tuple

from .config import UploadConfig
from .models import ArtformData, UploadStats, ProcessResult
from .csv_parser import CSVParser
from .database import FirestoreManager
from .utils import chunk_list

logger = logging.getLogger('artform_uploader.uploader')

class ArtformUploader:
    """Main uploader class that orchestrates the upload process"""
    
    def __init__(self, config: UploadConfig):
        self.config = config
        self.csv_parser = CSVParser(config.csv_path)
        self.db_manager = FirestoreManager(config.firebase, config.processing)
        self.stats = UploadStats()
        self.stats_lock = Lock()
    
    def upload_artforms(self) -> UploadStats:
        """
        Main method to upload artforms
        
        Returns:
            UploadStats with comprehensive statistics
        """
        logger.info("🚀 Starting artform upload process...")
        logger.info(f"🔧 Configuration: {self.config.processing.max_workers} threads, batch size: {self.config.processing.batch_size}")
        
        # Log CSV headers for debugging
        headers = self.csv_parser.get_csv_headers()
        logger.info(f"📋 CSV contains the following fields: {headers}")
        
        try:
            total_rows = self.csv_parser.get_total_rows()
            logger.info(f"📊 Total rows to process: {total_rows}")
            
            # Process in batches
            current_batch = []
            
            for row_num, artform in self.csv_parser.parse_csv():
                if artform is None:
                    with self.stats_lock:
                        self.stats.skipped += 1
                    continue
                
                if not artform.is_valid():
                    logger.warning(f"⚠️  Row {row_num}: Invalid artform data - {artform.slug}")
                    with self.stats_lock:
                        self.stats.skipped += 1
                    continue
                
                # Log which fields will be updated
                fields_to_update = artform.get_fields_to_update()
                logger.debug(f"📝 Row {row_num}: Will update fields {fields_to_update} for {artform.slug}")
                
                current_batch.append((row_num, artform))
                
                # Process batch when it reaches batch_size
                if len(current_batch) >= self.config.processing.batch_size:
                    self._process_batch(current_batch)
                    current_batch = []
                    
                    # Add delay between batches
                    if self.config.processing.batch_delay > 0:
                        logger.debug(f"⏸️  Batch delay: {self.config.processing.batch_delay}s")
                        time.sleep(self.config.processing.batch_delay)
            
            # Process remaining documents
            if current_batch:
                self._process_batch(current_batch)
            
            self._log_final_stats()
            return self.stats
            
        except Exception as e:
            logger.error(f"💥 Critical error during upload process: {str(e)}")
            raise
    
    def _process_batch(self, batch_data: List[Tuple[int, ArtformData]]):
        """Process a batch of documents using threading"""
        logger.info(f"🔄 Processing batch of {len(batch_data)} documents using {self.config.processing.max_workers} threads...")
        
        with ThreadPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
            # Submit all tasks
            future_to_data = {
                executor.submit(self.db_manager.upload_document, artform): (row_num, artform)
                for row_num, artform in batch_data
            }
            
            logger.info(f"📤 Submitted {len(future_to_data)} tasks to thread pool")
            
            # Process completed tasks
            for future in as_completed(future_to_data):
                row_num, artform = future_to_data[future]
                
                try:
                    result = future.result()
                    result.row_number = row_num
                    self._update_stats(result)
                    
                except Exception as e:
                    logger.error(f"❌ Unexpected error processing '{artform.slug}' (row {row_num}): {str(e)}")
                    with self.stats_lock:
                        self.stats.errors += 1
                        self.stats.total_processed += 1
    
    def _update_stats(self, result: ProcessResult):
        """Update statistics based on processing result"""
        with self.stats_lock:
            if result.success:
                self.stats.success += 1
                if result.is_new_document:
                    self.stats.new_documents += 1
                    action = "CREATED"
                    emoji = "🆕"
                else:
                    self.stats.updated_documents += 1
                    action = "UPDATED"
                    emoji = "🔄"
                
                # Log which fields were updated
                fields_info = f" (fields: {', '.join(result.fields_updated)})" if result.fields_updated else ""
                logger.info(f"{emoji} {action}: {result.doc_id}{fields_info}")
            else:
                self.stats.errors += 1
                logger.error(f"❌ FAILED: {result.doc_id} - {result.error_message}")
            
            self.stats.total_processed += 1
    
    def _log_final_stats(self):
        """Log comprehensive final statistics"""
        logger.info("\n" + "="*60)
        logger.info("🎉 UPLOAD COMPLETED")
        logger.info("="*60)
        logger.info(f"✅ Successfully processed: {self.stats.success} records")
        logger.info(f"  🆕 New documents CREATED: {self.stats.new_documents}")
        logger.info(f"  🔄 Existing documents UPDATED: {self.stats.updated_documents}")
        logger.info(f"❌ Errors encountered: {self.stats.errors} records")
        logger.info(f"⚠️  Skipped records: {self.stats.skipped} records")
        logger.info(f"📊 Total processed: {self.stats.total_processed} records")
        logger.info(f"⏱️  Total duration: {self.stats.duration:.2f} seconds")
        logger.info(f"🚀 Average rate: {self.stats.rate_per_second:.2f} records/second")
        logger.info(f"📈 Success rate: {self.stats.success_rate:.1f}%")
        logger.info(f"🔧 Threading: Using {self.config.processing.max_workers} concurrent workers")
        logger.info(f"📦 Batch size: {self.config.processing.batch_size} records per batch")