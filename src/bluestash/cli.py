"""
BluStash CLI - Command Line Interface for File System Indexing

This module provides a command-line interface for the BluStash file system indexing tool.
It allows users to scan directories and index their contents in a database through
simple command-line commands.
"""
import asyncio
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy.exc import IntegrityError

from bluestash.db.models import reg, engine
from bluestash.db.utils import (scan_and_store)

app = typer.Typer(help="Ein CLI-Tool zur Verwaltung des Dateisystemindex.")
console = Console()

@app.command(name="scan")
def scan_command(
    basis_pfad: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="Der Basispfad, ab dem der Scan gestartet werden soll."
    )
):
    """
    Scan the file system starting from a given base path and index it in the database.

    This command:
    1. Initializes the database tables if they don't exist
    2. Recursively scans all directories and files from the specified path
    3. Stores information about directories and files in the database
    4. Calculates file hashes for content identification

    The command handles errors gracefully and provides colorful console output
    to indicate progress and status.
    """
    async def _scan():
        console.print("[bold green]Tabellen werden initialisiert...[/bold green]")
        try:
            async with engine.begin() as conn:
                await conn.run_sync(reg.metadata.create_all)
            console.print("[bold green]Tabellen sind bereit.[/bold green]")
        except Exception as e:
            console.print(f"[bold red]Fehler bei der Initialisierung der Tabellen: {e}[/bold red]")
            raise typer.Exit(code=1)

        console.print(f"[bold blue]Starte Scan ab Pfad: {basis_pfad}[/bold blue]")
        try:
            await scan_and_store(basis_pfad)
            console.print("[bold green]Scan abgeschlossen, Datenbank ist aktuell.[/bold green]")
        except IntegrityError as e:
            console.print(f"[bold yellow]Integritätsfehler (vermutlich doppelte Einträge?): {e}[/bold yellow]")
            raise typer.Exit(code=1)
        except Exception as e:
            console.print(f"[bold red]Allgemeiner Fehler während des Scans: {e}[/bold red]")
            raise typer.Exit(code=1)

    asyncio.run(_scan())

if __name__ == "__main__":
    app()
