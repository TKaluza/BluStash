"""
BluStash - File System Indexing Tool

This module provides the main functionality for scanning a file system
and storing the information in a database. It creates necessary database
tables and initiates the scanning process from a specified root directory.
"""
import asyncio
import os
from pathlib import Path
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

from bluestash.db.models import reg, engine
from bluestash.db.utils import scan_and_store
from bluestash import setup_logging

# Load environment variables from .env file
load_dotenv()

# Set up logger
logger = setup_logging(logger_name="bluestash.main")

async def main():
    """
    Main application function that initializes the database and performs file system scanning.

    This function:
    1. Creates database tables if they don't exist
    2. Scans the file system starting from a specified root directory
    3. Stores file and directory information in the database

    Handles integrity errors (like duplicate entries) and other exceptions
    that might occur during the scanning process.
    """
    # Initialize database tables
    async with engine.begin() as conn:
        await conn.run_sync(reg.metadata.create_all)
    logger.info("Database tables ready.")

    # Get the folder entrypoint from environment variables or use default
    folder_entrypoint = os.getenv("FOLDER_ENTRYPOINT", "/home/tim/Documents")
    basis_pfad = Path(folder_entrypoint)
    logger.info(f"Starting scan from: {basis_pfad}")

    # Get user's home directory to exclude from scan
    home_dir = Path.home()
    logger.info(f"Excluding user home directory from scan: {home_dir}")

    try:
        # Call scan_and_store with the home directory as an exclude path
        await scan_and_store(basis_pfad, exclude_paths=[home_dir])
    except IntegrityError as e:
        logger.error(f"Integrity error (possibly duplicate entries): {e}")
    except Exception as e:
        logger.error(f"General error: {e}", exc_info=True)

    logger.info("Scan completed, database is up to date.")

if __name__ == "__main__":
    asyncio.run(main())
