"""
Utility Functions for File System Scanning and Database Operations

This module provides utility functions for scanning the file system,
calculating file hashes, and storing directory and file information in the database.
It includes functions for recursive directory traversal, file content hashing,
and database operations.

The module uses asyncio for concurrent operations to improve performance
when scanning large directory structures.
"""
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
import logging
from xxhash import xxh3_128_hexdigest
import os
from typing import Optional, Callable

from bluestash.db.models import Dir, File, AsyncSession
from bluestash import setup_logging

# Set up logger using the standardized logging configuration
logger = setup_logging(logger_name="fs_index")

@asynccontextmanager
async def get_async_session():
    """
    Async context manager for database session handling.

    This function creates and yields an async SQLAlchemy session that can be used
    for database operations. The session is automatically closed when the context
    is exited, ensuring proper resource cleanup.

    Yields:
        AsyncSession: An async SQLAlchemy session
    """
    async with AsyncSession() as session:
        yield session

async def get_size_and_hash(file_path: Path):
    """
    Asynchronously read a file, calculate its size and xxHash128 hash.

    This function reads the entire file content, calculates its size based on
    the read bytes, and computes a xxHash128 hash of the content. The operation
    is performed in a separate thread to avoid blocking the event loop.

    Args:
        file_path (Path): Path to the file to read and hash

    Returns:
        tuple: A tuple containing (size, hash_value) where:
            - size (int): Size of the file in bytes
            - hash_value (bytes): xxHash128 hash of the file content as bytes
    """
    def read_and_hash():
        with open(file_path, "rb") as f:
            data = f.read()
        size = len(data)  # Size based on the read bytes
        hash_val = bytes.fromhex(xxh3_128_hexdigest(data))
        return size, hash_val
    return await asyncio.to_thread(read_and_hash)


async def count_dirs_and_files(start_path: Path):
    """
    Recursively count the total number of directories and files (excluding symlinks).

    This function traverses the directory structure starting from the given path
    and counts all directories and files, excluding symbolic links. The counts
    can be used for progress bars or status reporting during scanning operations.

    Args:
        start_path (Path): The root directory to start counting from

    Returns:
        tuple: A tuple containing (dir_count, file_count) where:
            - dir_count (int): Total number of directories (including the root)
            - file_count (int): Total number of files
    """
    dir_count = 0
    file_count = 0
    loop = asyncio.get_running_loop()

    for root, dirs, files in await loop.run_in_executor(None, lambda: list(os.walk(start_path, followlinks=False))):
        dir_count += len(dirs)  # Unterverzeichnisse
        for d in dirs:
            d_path = Path(root) / d
            if d_path.is_symlink():
                continue
        for f in files:
            f_path = Path(root) / f
            if f_path.is_symlink():
                continue
            file_count += 1
    dir_count += 1  # Wurzelverzeichnis mitzählen

    return dir_count, file_count


async def scan_dirs_and_build_lookup(start_path: Path, session,
                                     total_dirs: int,
                                     progress_callback: Optional[Callable[[int, int], None]] = None) -> dict[Path, Dir]:
    """
    Recursively walk directory structure, add directories to the database, and build a lookup.

    This function traverses the directory structure, creates Dir objects
    for each directory, and maintains the parent-child relationships. It also
    builds a lookup dictionary mapping paths to Dir objects for later use.
    It reports progress specifically for directory scanning.

    Args:
        start_path (Path): The root directory to start scanning from.
        session: The database session to use for the operation.
        total_dirs (int): The total number of directories expected (for progress).
        progress_callback (Callable[[int, int], None], optional):
            A callback function that will be called with (current_dirs, total_dirs).

    Returns:
        dict[Path, Dir]: A dictionary mapping Path objects to Dir objects.
    """
    dir_lookup = {}
    current_dirs_processed = [0] # Mutable list for callback

    async def walk_dirs_internal(path, parent_obj=None):
        if path.is_symlink():
            return
        dir_obj = Dir(
            name=path.name,
            full_path_hash=Dir.compute_full_path_hash(path),
            parent=parent_obj
        )
        session.add(dir_obj)
        await session.flush()
        dir_lookup[path] = dir_obj

        current_dirs_processed[0] += 1
        if progress_callback:
            progress_callback(current_dirs_processed[0], total_dirs)

        try:
            for entry in await asyncio.to_thread(lambda: list(path.iterdir())):
                if entry.is_dir() and not entry.is_symlink():
                    await walk_dirs_internal(entry, dir_obj)
        except Exception as e:
            logger.error(f"Error reading directory {path}: {e}")

    await walk_dirs_internal(start_path)
    return dir_lookup


async def insert_files_with_progress(session, dir_lookup: dict,
                                     total_files: int,
                                     progress_callback: Optional[Callable[[int, int], None]] = None,
                                     chunk_size: int = 1000): # Hinzugefügter chunk_size Parameter
    """
    Insert all files from all directories into the database with xxHash128 and size.
    This function reports progress specifically for file insertion.

    Args:
        session: The database session to use for the operation.
        dir_lookup (dict): A dictionary mapping Path objects to Dir objects.
        total_files (int): The total number of files expected (for progress).
        progress_callback (Callable[[int, int], None], optional):
            A callback function that will be called with (current_files, total_files).
        chunk_size (int): Number of files to process before committing a chunk to the database.
    """
    current_files_processed_ref = [0] # Mutable list for callback

    file_processing_tasks = []
    for dir_path, dir_obj in dir_lookup.items():
        try:
            for entry in await asyncio.to_thread(lambda: list(dir_path.iterdir())):
                if entry.is_file() and not entry.is_symlink():
                    file_processing_tasks.append((entry, dir_obj))
        except Exception as e:
            logger.error(f"Error reading directory {dir_path}: {e}")

    # Process files in chunks
    for i in range(0, len(file_processing_tasks), chunk_size):
        chunk = file_processing_tasks[i:i + chunk_size]

        tasks = []
        for file_path, dir_obj in chunk:
            async def process_single_file(fp: Path, do):
                if fp.is_symlink() or not fp.is_file():
                    return
                try:
                    size, hash_val = await get_size_and_hash(fp)
                    file_obj = File(
                        name=fp.name,
                        dir=do,
                        size=size,
                        hash_xx128=hash_val
                    )
                    session.add(file_obj)
                except Exception as e:
                    logger.error(f"Error reading {fp}: {e}")

            tasks.append(process_single_file(file_path, dir_obj))

        await asyncio.gather(*tasks)
        await session.commit() # Commit after each chunk

        current_files_processed_ref[0] += len(chunk)
        if progress_callback:
            progress_callback(min(current_files_processed_ref[0], total_files), total_files) # Ensure progress doesn't exceed total

    # Final commit for any remaining files if the last chunk was smaller than chunk_size
    # (Though with the loop above, everything should be committed)
    # This might not be strictly necessary if the loop handles all, but good for robustness.
    await session.commit()
