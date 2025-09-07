import logging
import os
from datetime import datetime

def setup_logging(level=logging.INFO, log_file="logs/app.log"):
    """
    Set up logging configuration for the sync document processing runtime.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Path to log file (default: logs/app.log)
    """
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    # Set specific loggers
    logging.getLogger('exospherehost').setLevel(logging.INFO)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.INFO)
    
    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info("Sync document processing runtime logging initialized")
    logger.info(f"Log level: {logging.getLevelName(level)}")
    logger.info(f"Log file: {log_file}")
