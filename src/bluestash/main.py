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

from db.models import reg, engine
from db.utils import scan_and_store

# Load environment variables from .env file
load_dotenv()

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
    print("Tabellen bereit.")

    # Scan and store everything from the desired root path
    basis_pfad = Path("/home/tim/Documents")  # <-- Replace with the actual start path!

    # Get user's home directory to exclude from scan
    home_dir = Path.home()
    print(f"Benutzerverzeichnis wird vom Scan ausgeschlossen: {home_dir}")

    try:
        # Call scan_and_store with the home directory as an exclude path
        await scan_and_store(basis_pfad, exclude_paths=[home_dir])
    except IntegrityError as e:
        print(f"Integritätsfehler (vermutlich doppelte Einträge?): {e}")
    except Exception as e:
        print(f"Allgemeiner Fehler: {e}")

    print("Scan abgeschlossen, Datenbank ist aktuell.")

if __name__ == "__main__":
    asyncio.run(main())
