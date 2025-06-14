"""Create mapping lists for xorriso based on database state."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List

from sqlalchemy import select, update

from bluestash import setup_logging
from bluestash.db.models import File, Dir, db_path
from bluestash.db.utils import get_async_session

logger = setup_logging(logger_name="bluestash.mapping_creator")


async def create_mapping_from_db(
    base_path: str,
    output_file: str,
    backup_dir: str = "Backup",
    data_dir: str = "data",
) -> None:
    """Generate a xorriso mapping list from unsafed files in the database.

    Parameters
    ----------
    base_path:
        Root directory that was scanned and stored in the database.
    output_file:
        Path to write the mapping list.
    backup_dir:
        Directory name on disc where the database and metadata will be stored.
    data_dir:
        Directory name on disc where file data will be stored.
    """
    base = Path(base_path).resolve()
    mapping_lines: List[str] = []
    file_ids: List[int] = []

    async with get_async_session() as session:
        stmt = select(File).join(Dir).where(File.is_valid == True)
        result = await session.execute(stmt)
        for file_obj in result.scalars():
            if not file_obj.is_safed:
                src = file_obj.path
                try:
                    relative = src.relative_to(base)
                except ValueError:
                    logger.warning("File %s is not under base path %s", src, base)
                    continue
                dest = Path("/") / data_dir / relative
                mapping_lines.append(f"{src} {dest}")
                file_ids.append(file_obj.id)

        # Always include the SQLite database file
        db_file = Path(db_path).resolve()
        dest_db = Path("/") / backup_dir / db_file.name
        mapping_lines.append(f"{db_file} {dest_db}")

        # Create metadata file
        meta = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "file_count": len(file_ids),
        }
        meta_path = Path(output_file).with_suffix(".meta.json")
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        dest_meta = Path("/") / backup_dir / "meta.json"
        mapping_lines.append(f"{meta_path} {dest_meta}")

        Path(output_file).write_text("\n".join(mapping_lines), encoding="utf-8")
        logger.info("Wrote mapping file to %s", output_file)

        if file_ids:
            await session.execute(
                update(File).where(File.id.in_(file_ids)).values(is_safed=True)
            )
            await session.commit()
            logger.info("Marked %d files as safed", len(file_ids))

