"""
Main entry point for the artform upload system
"""

import sys
import logging
from pathlib import Path

# Fixed imports (use package-relative syntax)
from .config import UploadConfig, FirebaseConfig, ProcessingConfig, LoggingConfig
from .uploader import ArtformUploader
from .utils import setup_logging

def create_default_config() -> UploadConfig:
    """Create default configuration"""
    firebase_config = FirebaseConfig(
        credentials_path=r"C:\Users\MY PC\OneDrive\Desktop\CODE\Python\Memeraki\artform\serviceAccountKey.json",
        collection_name="art_forms"
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
    
    return UploadConfig(
        csv_path=r"C:\Users\MY PC\OneDrive\Desktop\CODE\Python\Memeraki\artform\artforms.csv",
        firebase=firebase_config,
        processing=processing_config,
        logging=logging_config
    )

def main():
    """Main function to run the upload process"""
     # Initialize basic logger first for error handling
    logging.basicConfig(level=logging.CRITICAL)
    logger = logging.getLogger("FallbackLogger")
    try:
        # Create configuration
        config = create_default_config()
        
        # Set up logging
        logger = setup_logging(config.logging)
        logger.info("Artform Upload System Starting...")
        
        # Create and run uploader
        uploader = ArtformUploader(config)
        stats = uploader.upload_artforms()
        
        # Exit with appropriate code
        if stats.errors > 0:
            logger.warning("Upload completed with errors")
            sys.exit(1)
        else:
            logger.info("Upload completed successfully")
            sys.exit(0)
            
    except Exception as e:
        logger.critical(f"Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
