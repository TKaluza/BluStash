# db/models.py  – Python ≥3.12, SQLAlchemy ≥2.0
from __future__ import annotations
from pathlib import Path
from sqlalchemy import (
    BigInteger, Integer, String, LargeBinary, Boolean,
    ForeignKey, Index,
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import (
    Mapped, mapped_column, relationship, registry
)

from xxhash import xxh32_intdigest, xxh3_128_hexdigest

reg = registry()

@reg.mapped_as_dataclass()
class Dir:
    __tablename__ = "dir"

    id: Mapped[int]      = mapped_column(Integer, primary_key=True, init=False)

    # ── put *non-default* column first
    name: Mapped[str]    = mapped_column(String, nullable=False)
    full_path_hash: Mapped[int] = mapped_column(Integer, nullable=False)


    # ── defaulted columns may
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("dir.id", ondelete="CASCADE"), default=None
    )

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

    @property
    def full_path(self) -> Path:
        node, parts = self, []
        while node:
            parts.append(node.name)
            node = node.parent
        return Path("/") / Path(*reversed(parts))

    @staticmethod
    def compute_full_path_hash(path: Path) -> int:
        """
        Berechnet den xxHash64-Wert für einen gegebenen Pfad.
        """
        return xxh32_intdigest(str(path))

    def set_full_path_hash(self):
        """
        Setzt den Hashwert für den aktuellen Verzeichnispfad neu.
        Sollte nach jeder Änderung von Name/Elternverzeichnis aufgerufen werden.
        """
        self.full_path_hash = self.compute_full_path_hash(self.full_path)


@reg.mapped_as_dataclass()
class File:
    __tablename__ = "file"

    id:         Mapped[int]   = mapped_column(Integer, primary_key=True, init=False)
    dir_id: Mapped[int] = mapped_column(ForeignKey("dir.id", ondelete="CASCADE"), init=False)
    name:       Mapped[str]   = mapped_column(String, nullable=False)
    size:       Mapped[int]   = mapped_column(BigInteger, nullable=False)
    hash_xx128: Mapped[bytes] = mapped_column(LargeBinary(16), nullable=False)

    dir: Mapped["Dir"] = relationship(back_populates="files")

    __table_args__ = (
        Index("ux_file_dir_name", "dir_id", "name", unique=True),
    )

    @property
    def path(self) -> Path:
        return self.dir.full_path / self.name

# ── async engine / session ───────────────────────────────────────────
engine = create_async_engine("sqlite+aiosqlite:///fs_index.db", echo=False)
AsyncSession = async_sessionmaker(engine, expire_on_commit=False)
