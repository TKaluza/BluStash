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

from bluestash.db.models import Dir, File, AsyncSession

logger = logging.getLogger("fs_index")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("fs_index.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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


async def count_dirs_and_files(start_path: Path, exclude_paths=None):
    """
    Recursively count the total number of directories and files (excluding symlinks).

    This function traverses the directory structure starting from the given path
    and counts all directories and files, excluding symbolic links and paths in exclude_paths.
    The counts can be used for progress bars or status reporting during scanning operations.

    Args:
        start_path (Path): The root directory to start counting from
        exclude_paths (list, optional): List of paths to exclude from counting

    Returns:
        tuple: A tuple containing (dir_count, file_count) where:
            - dir_count (int): Total number of directories (including the root)
            - file_count (int): Total number of files
    """
    # Default to empty list if None
    if exclude_paths is None:
        exclude_paths = []

    # Convert all exclude paths to absolute and resolved paths
    exclude_paths = [Path(p).expanduser().resolve() for p in exclude_paths]
    dir_count = 0
    file_count = 0
    loop = asyncio.get_running_loop()

    for root, dirs, files in await loop.run_in_executor(None, lambda: list(os.walk(start_path, followlinks=False))):
        # Filter out excluded directories
        dirs_to_count = []
        for d in dirs:
            d_path = Path(root) / d
            if d_path.is_symlink():
                continue

            # Skip if path is in exclude_paths
            resolved_path = d_path.resolve()
            skip = False
            for exclude_path in exclude_paths:
                if resolved_path == exclude_path or resolved_path.is_relative_to(exclude_path):
                    logger.info(f"Skipping excluded path in counting: {d_path}")
                    skip = True
                    break

            if not skip:
                dirs_to_count.append(d)

        # Update dirs in-place to affect the walk
        dirs[:] = dirs_to_count
        dir_count += len(dirs_to_count)

        for f in files:
            f_path = Path(root) / f
            if f_path.is_symlink():
                continue
            file_count += 1
    dir_count += 1  # Wurzelverzeichnis mitzählen

    return dir_count, file_count

async def insert_dirs(start_path: Path, session, parent_dir_obj=None):
    """
    Recursively insert the entire directory structure starting from start_path into the database.

    This function creates a Dir object for the given path, adds it to the database session,
    and then recursively processes all subdirectories. It handles the parent-child relationships
    between directories to maintain the hierarchical structure.

    Args:
        start_path (Path): The directory path to insert
        session: The database session to use for the operation
        parent_dir_obj (Dir, optional): The parent Dir object, or None for the root directory

    Returns:
        Dir: The Dir object created for start_path, or None if start_path is a symlink
    """
    if start_path.is_symlink():
        return None

    dir_obj = Dir(
        name=start_path.name,
        full_path_hash=Dir.compute_full_path_hash(start_path),
        parent=parent_dir_obj
    )
    session.add(dir_obj)
    await session.flush()  # id gesetzt
    # Rekursiv für Unterverzeichnisse
    try:
        for entry in await asyncio.to_thread(lambda: list(start_path.iterdir())):
            if entry.is_dir() and not entry.is_symlink():
                await insert_dirs(entry, session, dir_obj)
    except Exception as e:
        logger.error(f"Error reading directory {start_path}: {e}")
    return dir_obj

async def insert_files(session, dir_lookup: dict):
    """
    Insert all files from all directories into the database with xxHash128 and size.

    This function iterates over all non-symlink files in all directories and adds them
    to the database with their xxHash128 hash and size. It uses a dictionary that maps
    directory paths to Dir objects for efficient lookups.

    Args:
        session: The database session to use for the operation
        dir_lookup (dict): A dictionary mapping Path objects to Dir objects
    """
    tasks = []

    async def process_file(file_path: Path, dir_obj):
        """
        Process a single file by calculating its hash and size and adding it to the database.

        This nested function checks if the path is a valid file (not a symlink),
        calculates its size and hash, creates a File object, and adds it to the database session.

        Args:
            file_path (Path): The path to the file to process
            dir_obj (Dir): The Dir object representing the file's parent directory
        """
        if file_path.is_symlink() or not file_path.is_file():
            return
        try:
            size, hash_val = await get_size_and_hash(file_path)
            file_obj = File(
                name=file_path.name,
                dir=dir_obj,
                size=size,
                hash_xx128=hash_val
            )
            session.add(file_obj)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    for dir_path, dir_obj in dir_lookup.items():
        try:
            for entry in await asyncio.to_thread(lambda: list(dir_path.iterdir())):
                if entry.is_file() and not entry.is_symlink():
                    tasks.append(asyncio.create_task(process_file(entry, dir_obj)))
        except Exception as e:
            logger.error(f"Error reading directory {dir_path}: {e}")

    await asyncio.gather(*tasks)

async def scan_and_store(start_path: Path, exclude_paths=None):
    """
    Execute the complete file system scanning and indexing process.

    This function performs the entire scanning and indexing workflow:
    1. Count directories and files (for progress reporting)
    2. Insert all directories into the database
    3. Insert all files into the database (including size and hash)

    Args:
        start_path (Path): The root directory to start scanning from
        exclude_paths (list, optional): List of paths to exclude from scanning
    """
    # Default to empty list if None
    if exclude_paths is None:
        exclude_paths = []

    # Convert all exclude paths to absolute and resolved paths
    exclude_paths = [Path(p).expanduser().resolve() for p in exclude_paths]
    async with get_async_session() as session:
        dir_count, file_count = await count_dirs_and_files(start_path, exclude_paths)
        logger.info(f"Scanning {dir_count} directories and {file_count} files under {start_path}")

        dir_lookup = {}

        async def walk_dirs(path, parent_obj=None):
            """
            Recursively walk directory structure and add directories to the database.

            This nested function traverses the directory structure, creates Dir objects
            for each directory, and maintains the parent-child relationships. It also
            builds a lookup dictionary mapping paths to Dir objects for later use.

            Args:
                path (Path): The directory path to process
                parent_obj (Dir, optional): The parent Dir object, or None for the root
            """
            # Skip if path is a symlink or in exclude_paths
            if path.is_symlink():
                return

            # Skip if path is in exclude_paths
            resolved_path = path.resolve()
            for exclude_path in exclude_paths:
                if resolved_path == exclude_path or resolved_path.is_relative_to(exclude_path):
                    logger.info(f"Skipping excluded path: {path}")
                    return
            dir_obj = Dir(
                name=path.name,
                full_path_hash=Dir.compute_full_path_hash(path),
                parent=parent_obj
            )
            session.add(dir_obj)
            await session.flush()
            dir_lookup[path] = dir_obj
            try:
                for entry in await asyncio.to_thread(lambda: list(path.iterdir())):
                    if entry.is_dir() and not entry.is_symlink():
                        await walk_dirs(entry, dir_obj)
            except Exception as e:
                logger.error(f"Error reading directory {path}: {e}")

        await walk_dirs(start_path)
        await insert_files(session, dir_lookup)
        await session.commit()
