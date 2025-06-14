"""
BluStash - A file system indexing and search tool

This package provides tools for indexing and searching files on a file system.
It includes utilities for scanning directories, calculating file hashes,
and storing file information in a database.
"""
import os
import logging
from dotenv import load_dotenv

def setup_logging(logger_name="bluestash", level=logging.INFO):
    """
    Set up logging with a consistent configuration using LOG_PATH from environment.
    
    Args:
        logger_name (str): Name of the logger to create
        level (int): Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Load environment variables if not already loaded
    load_dotenv()
    
    # Get log path from environment or use default
    log_path = os.getenv("LOG_PATH", "bluestash.log")
    
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Remove existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # Create file handler
    handler = logging.FileHandler(log_path)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger