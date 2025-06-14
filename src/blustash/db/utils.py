import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
import logging
from xxhash import xxh3_128_hexdigest
import os

from blustash.db.models import Dir, File, AsyncSession

logger = logging.getLogger("fs_index")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("fs_index.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

@asynccontextmanager
async def get_async_session():
    async with AsyncSession() as session:
        yield session

async def get_size_and_hash(file_path: Path):
    def read_and_hash():
        with open(file_path, "rb") as f:
            data = f.read()
        size = len(data)  # Größe anhand der eingelesenen Bytes
        hash_val = bytes.fromhex(xxh3_128_hexdigest(data))
        return size, hash_val
    return await asyncio.to_thread(read_and_hash)


async def count_dirs_and_files(start_path: Path):
    """
    Ermittelt rekursiv die Gesamtanzahl an Verzeichnissen und Dateien (ohne Symlinks).
    Für Fortschrittsbalken.
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

async def insert_dirs(start_path: Path, session, parent_dir_obj=None):
    """
    Rekursive Funktion, die die gesamte Verzeichnisstruktur ab start_path in die Datenbank einträgt.
    parent_dir_obj ist das Eltern-Dir-Objekt oder None für das Wurzelverzeichnis.
    Gibt Dir-Objekt für start_path zurück.
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
        logger.error(f"Fehler beim Einlesen von {start_path}: {e}")
    return dir_obj

async def insert_files(start_path: Path, session, dir_lookup: dict):
    """
    Iteriert über alle (nicht-symlink) Dateien in allen Verzeichnissen und trägt sie mit xxHash128 und Größe in die Datenbank ein.
    dir_lookup ordnet Path zum Dir-Objekt zu.
    """
    tasks = []

    async def process_file(file_path: Path, dir_obj):
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
            logger.error(f"Fehler beim Lesen {file_path}: {e}")

    for dir_path, dir_obj in dir_lookup.items():
        try:
            for entry in await asyncio.to_thread(lambda: list(dir_path.iterdir())):
                if entry.is_file() and not entry.is_symlink():
                    tasks.append(asyncio.create_task(process_file(entry, dir_obj)))
        except Exception as e:
            logger.error(f"Fehler beim Einlesen von {dir_path}: {e}")

    await asyncio.gather(*tasks)

async def scan_and_store(start_path: Path):
    """
    Führt das Komplettprogramm durch:
    1. Dir/Datei zählen (für Fortschrittsbalken)
    2. Alle Verzeichnisse eintragen
    3. Alle Dateien in die DB schreiben (inkl. Hash)
    """
    async with get_async_session() as session:
        dir_count, file_count = await count_dirs_and_files(start_path)
        logger.info(f"Scanne {dir_count} Verzeichnisse und {file_count} Dateien unter {start_path}")

        dir_lookup = {}

        async def walk_dirs(path, parent_obj=None):
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
            try:
                for entry in await asyncio.to_thread(lambda: list(path.iterdir())):
                    if entry.is_dir() and not entry.is_symlink():
                        await walk_dirs(entry, dir_obj)
            except Exception as e:
                logger.error(f"Fehler beim Einlesen von {path}: {e}")

        await walk_dirs(start_path)
        await insert_files(start_path, session, dir_lookup)
        await session.commit()