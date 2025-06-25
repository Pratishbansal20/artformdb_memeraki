"""
Database operations for Firebase/Firestore
"""

import logging
import time
from typing import Optional
import firebase_admin
from firebase_admin import credentials, firestore
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.cloud.exceptions import GoogleCloudError, TooManyRequests, ServiceUnavailable

from .config import FirebaseConfig, ProcessingConfig
from .models import ArtformData, ProcessResult

logger = logging.getLogger('artform_uploader.database')

class FirestoreManager:
    """Manages Firestore database operations"""
    
    def __init__(self, firebase_config: FirebaseConfig, processing_config: ProcessingConfig):
        self.firebase_config = firebase_config
        self.processing_config = processing_config
        self.db = None
        self._initialize_firebase()
    
    def _initialize_firebase(self):
        """Initialize Firebase connection"""
        try:
            cred = credentials.Certificate(self.firebase_config.credentials_path)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("Firebase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {str(e)}")
            raise
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((TooManyRequests, ServiceUnavailable, GoogleCloudError)),
        before_sleep=lambda retry_state: logger.debug(f"Retrying after {retry_state.seconds_since_start:.1f}s...")
    )
    def upload_document(self, artform: ArtformData) -> ProcessResult:
        """
        Upload a single document to Firestore
        
        Args:
            artform: ArtformData instance
            
        Returns:
            ProcessResult with operation details
        """
        start_time = time.time()
        
        try:
            doc_ref = self.db.collection(self.firebase_config.collection_name).document(artform.slug)
            doc_snapshot = doc_ref.get()
            
            # Prepare document data
            doc_data = artform.to_firestore_dict()
            doc_data["updated_at"] = firestore.SERVER_TIMESTAMP
            
            is_new_document = not doc_snapshot.exists
            
            if is_new_document:
                doc_data["created_at"] = firestore.SERVER_TIMESTAMP
                logger.debug(f"Creating new document: {artform.slug}")
            else:
                logger.debug(f"Updating existing document: {artform.slug}")
            
            # Upload to Firestore
            doc_ref.set(doc_data, merge=True)
            
            processing_time = time.time() - start_time
            return ProcessResult(
                doc_id=artform.slug,
                success=True,
                processing_time=processing_time,
                is_new_document=is_new_document
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Failed to upload document '{artform.slug}': {str(e)}"
            logger.error(error_msg)
            
            return ProcessResult(
                doc_id=artform.slug,
                success=False,
                error_message=error_msg,
                processing_time=processing_time
            )
    
    def batch_upload(self, artforms: list) -> list:
        """
        Upload multiple documents using Firestore batch operations
        
        Args:
            artforms: List of ArtformData instances
            
        Returns:
            List of ProcessResult objects
        """
        results = []
        batch = self.db.batch()
        
        try:
            for artform in artforms:
                doc_ref = self.db.collection(self.firebase_config.collection_name).document(artform.slug)
                doc_data = artform.to_firestore_dict()
                doc_data["updated_at"] = firestore.SERVER_TIMESTAMP
                doc_data["created_at"] = firestore.SERVER_TIMESTAMP
                
                batch.set(doc_ref, doc_data, merge=True)
            
            # Commit batch
            batch.commit()
            
            # Create success results
            for artform in artforms:
                results.append(ProcessResult(
                    doc_id=artform.slug,
                    success=True,
                    is_new_document=True  # Simplified for batch operations
                ))
                
        except Exception as e:
            logger.error(f"Batch upload failed: {str(e)}")
            
            # Create error results
            for artform in artforms:
                results.append(ProcessResult(
                    doc_id=artform.slug,
                    success=False,
                    error_message=str(e)
                ))
        
        return results
