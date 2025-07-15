import logging
import time
from typing import Optional, Dict, Any
import firebase_admin
from firebase_admin import credentials, firestore
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.cloud.exceptions import GoogleCloudError, TooManyRequests, ServiceUnavailable

from .config import FirebaseConfig, ProcessingConfig
from .models import ProcessResult

logger = logging.getLogger('json_uploader.database')

class FirestoreManager:
    """Manages Firestore database operations"""

    def __init__(self, firebase_config: FirebaseConfig, processing_config: ProcessingConfig):
        self.firebase_config = firebase_config
        self.processing_config = processing_config
        self.db = None
        self._initialize_firebase()

    def _initialize_firebase(self):
        """Initialize Firebase connection safely"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(self.firebase_config.credentials_path)
                firebase_admin.initialize_app(cred)
                logger.info("Firebase initialized successfully")
            self.db = firestore.client()
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((TooManyRequests, ServiceUnavailable, GoogleCloudError)),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying after {retry_state.seconds_since_start:.1f}s...")
    )
    def upload_product_document(self, slug: str, product: Dict[str, Any]) -> ProcessResult:
        """
        Upload a single product document to Firestore:
        collection/<slug>/products/<product_name>
        """
        start_time = time.time()
        try:
            doc_ref = self.db.collection(self.firebase_config.collection_name).document(slug)
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
            if not product_name:
                logger.warning(f"Product in '{slug}' missing required 'name': {product}")
            return ProcessResult(
                doc_id=product['name'],
                success=True,
                processing_time=processing_time,
                is_new_document=False  # Firestore does not expose new/existing doc info here
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
