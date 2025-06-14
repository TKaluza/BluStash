"""
BluStash CLI - Command Line Interface for File System Indexing

This module provides a command-line interface for the BluStash file system indexing tool.
It allows users to scan directories and index their contents in a database through
simple command-line commands.
"""
import asyncio
import os
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, MofNCompleteColumn, SpinnerColumn, TimeRemainingColumn
from rich.status import Status
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

from bluestash.db.models import reg, engine
from bluestash.db.utils import (count_dirs_and_files, scan_dirs_and_build_lookup, insert_files_with_progress, get_async_session)


# Load environment variables from .env file
load_dotenv()

app = typer.Typer(help="Ein CLI-Tool zur Verwaltung des Dateisystemindex.")
console = Console()

@app.command(name="scan")
def scan_command(
    basis_pfad: Path = typer.Argument(
        None,
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        help="Der Basispfad, ab dem der Scan gestartet werden soll. Standardmäßig wird der Wert aus der .env-Datei verwendet."
    ),
    db_path: str = typer.Option(
        None,
        "--db-path",
        "-d",
        help="Pfad zur Datenbank-Datei. Standardmäßig wird der Wert aus der .env-Datei verwendet."
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

    The database path can be specified using the --db-path option. If not provided,
    the path from the .env file will be used. The default location is in the user's
    home directory.

    The base path for scanning can be provided as an argument. If not provided,
    the value from the FOLDER_ENTRYPOINT variable in the .env file will be used.
    """
    async def _scan():
        # If db_path is provided, set it as an environment variable
        if db_path:
            os.environ["DB_PATH"] = db_path
            # Re-import engine with the updated environment variable
            from bluestash.db.models import engine as updated_engine
            current_engine = updated_engine
        else:
            current_engine = engine

        # If basis_pfad is not provided, get it from the environment variable
        nonlocal basis_pfad
        if basis_pfad is None:
            env_basis_pfad = os.getenv("FOLDER_ENTRYPOINT")
            if env_basis_pfad:
                basis_pfad = Path(env_basis_pfad)
                if not basis_pfad.exists() or not basis_pfad.is_dir() or not os.access(basis_pfad, os.R_OK):
                    console.print(f"[bold red]Fehler: Der in der .env-Datei angegebene Pfad '{env_basis_pfad}' existiert nicht, ist kein Verzeichnis oder ist nicht lesbar.[/bold red]")
                    raise typer.Exit(code=1)
            else:
                console.print("[bold red]Fehler: Kein Basispfad angegeben und keine FOLDER_ENTRYPOINT-Variable in der .env-Datei gefunden.[/bold red]")
                raise typer.Exit(code=1)

        console.print("[bold green]Tabellen werden initialisiert...[/bold green]")
        try:
            async with current_engine.begin() as conn:
                await conn.run_sync(reg.metadata.create_all)
            console.print("[bold green]Tabellen sind bereit.[/bold green]")
        except Exception as e:
            console.print(f"[bold red]Fehler bei der Initialisierung der Tabellen: {e}[/bold red]")
            raise typer.Exit(code=1)

        console.print(f"[bold blue]Starte Scan ab Pfad: {basis_pfad}[/bold blue]")

        total_dirs, total_files = 0, 0

        # PHASE 1: Initial Counting (Spinner)
        with console.status("[bold blue]Zähle Verzeichnisse und Dateien...", spinner="dots") as status:
            total_dirs, total_files = await count_dirs_and_files(basis_pfad)
            status.update("[bold green]Zählung abgeschlossen.[/bold green]")
        console.print(f"[bold green]Gefunden: {total_dirs} Verzeichnisse und {total_files} Dateien.[/bold green]")


        # Use async context manager for database session
        async with get_async_session() as session:
            try:
                # PHASE 2: Directory Scanning (Progress Bar)
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    "eta",
                    TimeElapsedColumn(),
                    console=console,
                    transient=False, # Bleibt sichtbar, bis explizit gestoppt/entfernt
                ) as progress_dir:
                    dir_task = progress_dir.add_task("[cyan]Verzeichnisse scannen...", total=total_dirs)

                    def update_dir_progress(current_dirs: int, total: int):
                        progress_dir.update(dir_task, completed=current_dirs)

                    dir_lookup = await scan_dirs_and_build_lookup(basis_pfad, session, total_dirs, progress_callback=update_dir_progress)
                    progress_dir.update(dir_task, completed=total_dirs, description="[green]Verzeichnisse gescannt.[/green]")
                    progress_dir.stop() # Beendet den Fortschrittsbalken für Verzeichnisse

                # PHASE 3: Intermediate Spinner (Vorbereitung zur Dateiverarbeitung)
                with console.status("[bold magenta]Bereite Dateiverarbeitung vor...", spinner="dots") as status:
                    await asyncio.sleep(0.5) # Simuliert eine kurze Vorbereitungszeit
                    status.update("[bold magenta]Vorbereitung abgeschlossen.[/bold magenta]")


                # PHASE 4: File Processing (Progress Bar)
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    "eta",
                    TimeElapsedColumn(),
                    console=console,
                    transient=False, # Bleibt sichtbar, bis explizit gestoppt/entfernt
                ) as progress_file:
                    file_task = progress_file.add_task("[yellow]Dateien verarbeiten...", total=total_files)

                    def update_file_progress(current_files: int, total: int):
                        progress_file.update(file_task, completed=current_files)

                    await insert_files_with_progress(session, dir_lookup, total_files, progress_callback=update_file_progress)
                    progress_file.update(file_task, completed=total_files, description="[green]Dateien verarbeitet.[/green]")
                    progress_file.stop() # Beendet den Fortschrittsbalken für Dateien

                # PHASE 5: Finalizing (Spinner)
                with console.status("[bold green]Finalisiere Datenbank-Transaktionen...", spinner="dots") as status:
                    await session.commit() # Datenbank-Commit hier
                    status.update("[bold green]Datenbank-Transaktionen abgeschlossen.[/bold green]")
                    await asyncio.sleep(0.5) # Simuliert eine kurze abschließende Verzögerung

                console.print("[bold green]Scan abgeschlossen, Datenbank ist aktuell.[/bold green]")

            except IntegrityError as e:
                await session.rollback() # Rollback im Fehlerfall
                console.print(f"[bold yellow]Integritätsfehler (vermutlich doppelte Einträge?): {e}[/bold yellow]")
                raise typer.Exit(code=1)
            except Exception as e:
                await session.rollback() # Rollback im Fehlerfall
                console.print(f"[bold red]Allgemeiner Fehler während des Scans: {e}[/bold red]")
                raise typer.Exit(code=1)

    asyncio.run(_scan())

if __name__ == "__main__":
    app()
