"""
Database Models for BluStash File System Indexing

This module defines the SQLAlchemy ORM models used to store file system information.
It includes models for directories and files, with relationships between them to
represent the file system hierarchy. The models use xxHash for efficient path and
content hashing.

Requirements:
- Python ≥3.12
- SQLAlchemy ≥2.0
"""
from __future__ import annotations
import os
from pathlib import Path
from sqlalchemy import (
    BigInteger, Integer, String, LargeBinary, Boolean,
    ForeignKey, Index,
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import (
    Mapped, mapped_column, relationship, registry
)
from dotenv import load_dotenv

from xxhash import xxh32_intdigest, xxh3_128_hexdigest

# Load environment variables from .env file
load_dotenv()

reg = registry()

@reg.mapped_as_dataclass()
class Dir:
    """
    Directory model representing a directory in the file system.

    This class maps to the 'dir' table in the database and stores information
    about directories including their name, path hash, and relationships to
    parent directories and child elements (subdirectories and files).

    The model uses a self-referential relationship to represent the directory
    hierarchy, allowing for efficient traversal of the file system structure.
    """
    __tablename__ = "dir"

    id: Mapped[int]      = mapped_column(Integer, primary_key=True, init=False)

    # ── put *non-default* column first
    name: Mapped[str]    = mapped_column(String, nullable=False)
    full_path_hash: Mapped[int] = mapped_column(Integer, nullable=False)


    # ── defaulted columns may
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("dir.id", ondelete="CASCADE"), default=None
    )
    is_valid:Mapped[bool] = mapped_column(Boolean, default=True, nullable=False) # Hinzugefügt

    # self-referential relationship: use a lambda so Dir.id is defined
    parent: Mapped["Dir | None"] = relationship(
        remote_side=lambda: Dir.id,
        back_populates="children",
        default=None,
        lazy="selectin",
    )
    children: Mapped[list["Dir"]] = relationship(
        back_populates="parent",
        cascade="all, delete-orphan",
        default_factory=list,
        lazy="selectin",
    )
    files: Mapped[list["File"]] = relationship(
        back_populates="dir",
        cascade="all, delete-orphan",
        default_factory=list,
        lazy="selectin",
    )

    __table_args__ = (
        Index("ux_dir_parent_name", "parent_id", "name", unique=True),
        Index("ix_dir_full_path_hash", "full_path_hash"),
    )

    def __repr__(self):
        return f"<Dir(name='{self.name}', full_path_hash={self.full_path_hash})>"

    @property
    def full_path(self) -> Path:
        """
        Calculate and return the full absolute path of this directory.

        This property traverses the directory hierarchy upwards through parent
        relationships, collecting directory names along the way, and then
        constructs a Path object representing the absolute path from the root.

        Returns:
            Path: A Path object representing the absolute path of this directory
        """
        node, parts = self, []
        while node:
            parts.append(node.name)
            node = node.parent
        return Path("/") / Path(*reversed(parts))

    @staticmethod
    def compute_full_path_hash(path: Path) -> int:
        """
        Compute the xxHash32 integer digest value for a given path.

        This method converts the path to a string and calculates a hash value
        that can be used for efficient path lookups in the database.

        Args:
            path (Path): The path to hash

        Returns:
            int: The xxHash32 integer digest of the path string
        """
        return xxh32_intdigest(str(path))

    def set_full_path_hash(self):
        """
        Update the hash value for the current directory path.

        This method should be called after any change to the directory's name
        or parent directory to ensure the hash value remains consistent with
        the actual path.

        The hash value is used for efficient lookups and to verify path integrity.
        """
        self.full_path_hash = self.compute_full_path_hash(self.full_path)


@reg.mapped_as_dataclass()
class File:
    """
    File model representing a file in the file system.

    This class maps to the 'file' table in the database and stores information
    about files including their name, size, content hash, and relationship to
    their parent directory.

    The model uses xxHash128 for efficient content hashing, which allows for
    quick file identification and comparison.
    """
    __tablename__ = "file"

    id:         Mapped[int]   = mapped_column(Integer, primary_key=True, init=False)
    dir_id: Mapped[int] = mapped_column(ForeignKey("dir.id", ondelete="CASCADE"), init=False)
    name:       Mapped[str]   = mapped_column(String, nullable=False)
    size:       Mapped[int]   = mapped_column(BigInteger, nullable=False)
    hash_xx128: Mapped[bytes] = mapped_column(LargeBinary(16), nullable=False)

    dir: Mapped["Dir"] = relationship(back_populates="files")
    is_safed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_valid:Mapped[bool] = mapped_column(Boolean, default=True, nullable=False) # Hinzugefügt

    __table_args__ = (
        Index("ux_file_dir_name", "dir_id", "name", unique=True),
    )

    @property
    def path(self) -> Path:
        """
        Calculate and return the full absolute path of this file.

        This property combines the parent directory's full path with the file's name
        to create a complete path to the file in the file system.

        Returns:
            Path: A Path object representing the absolute path of this file
        """
        return self.dir.full_path / self.name

# ── async engine / session ───────────────────────────────────────────
# Get database path from environment variable or use default
db_path = os.getenv("DB_PATH", "fs_index.db")
# Expand ~ to user's home directory if present
db_path = os.path.expanduser(db_path)
# Create the SQLAlchemy engine with the configured path
engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
AsyncSession = async_sessionmaker(engine, expire_on_commit=False)
