"""
Main entry point for the GenAI JSON uploader system (no recommendations, pure batch JSON upload)
"""

import sys
import os
import json
import logging
from pathlib import Path

from .config import UploadConfig, FirebaseConfig, ProcessingConfig, LoggingConfig
from .utils import setup_logging
from firebase_admin import firestore

def create_default_config():
    """Create default configuration for Firestore credentials and collection"""
    firebase_config = FirebaseConfig(
        credentials_path=r"C:\Users\MY PC\OneDrive\Desktop\CODE\Python\Memeraki\jsonupload\serviceAccountKey.json",
        collection_name="artform_top_products"
    )
    processing_config = ProcessingConfig(
        max_workers=10,
        batch_size=500,
        retry_attempts=5
    )
    logging_config = LoggingConfig(
        level="INFO",
        log_dir="logs"
    )
    # JSON upload: only json_folder will be relevant in UploadConfig
    return UploadConfig(
        json_folder=r"C:\Users\MY PC\OneDrive\Desktop\CODE\Python\Memeraki\jsonupload\jsonoutputs",
        firebase=firebase_config,
        processing=processing_config,
        logging=logging_config
    )

def upload_genai_collections(config):
    """
    Batch uploader for GenAI JSON collections:
    - Reads all *_top_products.json files in config.json_folder
    - Uploads each product under artform_top_products/<slug>/products/<product_name>
    """
    from firebase_admin import credentials,firestore, initialize_app
    import firebase_admin
    

    logger = setup_logging(config.logging)
    logger.info("Starting GenAI JSON top art collection uploader — no recommendations features.")
    if not firebase_admin._apps:
        creds = credentials.Certificate(config.firebase.credentials_path)
        initialize_app(creds)
    db = firestore.client()

    files_processed = 0
    total_products = 0
    errors = 0

    for fname in os.listdir(config.json_folder):
        if fname.endswith("_top_products.json"):
            slug = fname.replace("_top_products.json", "")
            file_path = os.path.join(config.json_folder, fname)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    products = data.get("products", []) or data.get("columns", [])
                    doc_ref = db.collection(config.firebase.collection_name).document(slug)
                    uploaded = 0
                    for prod in products:
                        name = prod.get("name", "")
                        prod_doc = doc_ref.collection("products").document(name)
                        prod_doc.set({
                            "description": prod.get("sescription", ""),
                            "size_inches": prod.get("size_inches", ""),
                            "price_inr": prod.get("price_inr", "")
                        }, merge=True)
                        uploaded += 1
                    logger.info(f"Uploaded {uploaded} products for '{slug}'.")
                    total_products += uploaded
                    files_processed += 1
            except Exception as err:
                logger.error(f"Failed to process {fname}: {err}")
                errors += 1

    logger.info("=" * 60)
    logger.info(f"UPLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files processed: {files_processed}")
    logger.info(f"Total products uploaded: {total_products}")
    logger.info(f"Files failed: {errors}")

def main():
    """Main entry for GenAI JSON batch art collection upload (no recommendations)"""
    logging.basicConfig(level=logging.CRITICAL)
    logger = logging.getLogger("FallbackLogger")
    try:
        config = create_default_config()
        upload_genai_collections(config)
        logger.info("All GenAI JSON art collections uploaded successfully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
