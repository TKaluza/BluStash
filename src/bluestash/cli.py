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
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    SpinnerColumn,
    TimeRemainingColumn,
)
from rich.status import Status
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

from bluestash.db.models import reg, engine, ScanSession
from bluestash.db.utils import (
    count_dirs_and_files,
    scan_dirs_and_build_lookup,
    insert_files_with_progress,
    get_async_session,
    reset_all_valid_flags,  # Importiert
    delete_invalid_entries,  # Importiert
    get_latest_session_info,
)
from bluestash import setup_logging


# Load environment variables from .env file
load_dotenv()

# Set up logger
logger = setup_logging(logger_name="bluestash.cli")

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
        help="Der Basispfad, ab dem der Scan gestartet werden soll. Standardmäßig wird der Wert aus der .env-Datei verwendet.",
    ),
    db_path: str = typer.Option(
        None,
        "--db-path",
        "-d",
        help="Pfad zur Datenbank-Datei. Standardmäßig wird der Wert aus der .env-Datei verwendet.",
    ),
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
                if (
                    not basis_pfad.exists()
                    or not basis_pfad.is_dir()
                    or not os.access(basis_pfad, os.R_OK)
                ):
                    error_msg = f"Error: The path '{env_basis_pfad}' specified in the .env file does not exist, is not a directory, or is not readable."
                    console.print(f"[bold red]{error_msg}[/bold red]")
                    logger.error(error_msg)
                    raise typer.Exit(code=1)
            else:
                error_msg = "Error: No base path specified and no FOLDER_ENTRYPOINT variable found in the .env file."
                console.print(f"[bold red]{error_msg}[/bold red]")
                logger.error(error_msg)
                raise typer.Exit(code=1)

        console.print("[bold green]Tabellen werden initialisiert...[/bold green]")
        logger.info("Initializing database tables...")
        try:
            async with current_engine.begin() as conn:
                await conn.run_sync(reg.metadata.create_all)
            console.print("[bold green]Tabellen sind bereit.[/bold green]")
            logger.info("Database tables ready.")
        except Exception as e:
            error_msg = f"Error initializing database tables: {e}"
            console.print(f"[bold red]{error_msg}[/bold red]")
            logger.error(error_msg, exc_info=True)
            raise typer.Exit(code=1)

        console.print(f"[bold blue]Starte Scan ab Pfad: {basis_pfad}[/bold blue]")
        logger.info(f"Starting scan from path: {basis_pfad}")

        total_dirs, total_files = 0, 0

        # Use async context manager for database session
        async with get_async_session() as session:
            try:
                # Create a ScanSession but don't add it to the session yet
                scan_session = ScanSession()

                # Vor dem Scan: Alle is_valid-Flags auf False setzen
                await reset_all_valid_flags(session)
                console.print(
                    "[bold blue]Vorhandene Einträge auf Ungültig gesetzt.[/bold blue]"
                )

                # PHASE 1: Initial Counting (Spinner)
                logger.info("Counting directories and files...")
                with console.status(
                    "[bold blue]Zähle Verzeichnisse und Dateien...", spinner="dots"
                ) as status:
                    total_dirs, total_files = await count_dirs_and_files(basis_pfad)
                    status.update("[bold green]Zählung abgeschlossen.[/bold green]")
                console.print(
                    f"[bold green]Gefunden: {total_dirs} Verzeichnisse und {total_files} Dateien.[/bold green]"
                )
                logger.info(f"Found: {total_dirs} directories and {total_files} files.")

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
                    transient=False,  # Bleibt sichtbar, bis explizit gestoppt/entfernt
                ) as progress_dir:
                    dir_task = progress_dir.add_task(
                        "[cyan]Verzeichnisse scannen...", total=total_dirs
                    )
                    logger.info("Scanning directories...")

                    def update_dir_progress(current_dirs: int, total: int):
                        progress_dir.update(dir_task, completed=current_dirs)

                    dir_lookup = await scan_dirs_and_build_lookup(
                        basis_pfad,
                        session,
                        total_dirs,
                        progress_callback=update_dir_progress,
                    )
                    progress_dir.update(
                        dir_task,
                        completed=total_dirs,
                        description="[green]Verzeichnisse gescannt.[/green]",
                    )
                    progress_dir.stop()  # Beendet den Fortschrittsbalken für Verzeichnisse
                    logger.info("Directory scanning completed.")

                # PHASE 3: Intermediate Spinner (Vorbereitung zur Dateiverarbeitung)
                with console.status(
                    "[bold magenta]Bereite Dateiverarbeitung vor...", spinner="dots"
                ) as status:
                    await asyncio.sleep(0.5)  # Simuliert eine kurze Vorbereitungszeit
                    status.update(
                        "[bold magenta]Vorbereitung abgeschlossen.[/bold magenta]"
                    )

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
                    transient=False,  # Bleibt sichtbar, bis explizit gestoppt/entfernt
                ) as progress_file:
                    file_task = progress_file.add_task(
                        "[yellow]Dateien verarbeiten...", total=total_files
                    )
                    logger.info("Processing files...")

                    def update_file_progress(current_files: int, total: int):
                        progress_file.update(file_task, completed=current_files)

                    processed = await insert_files_with_progress(
                        session,
                        dir_lookup,
                        total_files,
                        scan_session,
                        progress_callback=update_file_progress,
                    )

                    # Update the changed_files count if there were changes
                    changed_files = processed
                    if processed > 0:
                        scan_session.changed_files = changed_files
                        # No need to add scan_session to the session again as it's already added
                        # in insert_files_with_progress when the first change is detected
                        logger.info(f"Recorded {changed_files} changed files")

                    progress_file.update(
                        file_task,
                        completed=total_files,
                        description="[green]Dateien verarbeitet.[/green]",
                    )
                    progress_file.stop()  # Beendet den Fortschrittsbalken für Dateien
                    logger.info("File processing completed.")

                # Nach dem Scan: Ungültige Einträge löschen
                deleted_files = await delete_invalid_entries(session)
                console.print(f"[bold blue]Ungültige Einträge aus der Datenbank entfernt: {deleted_files} Dateien.[/bold blue]")

                # If files were deleted, count them as changes
                if deleted_files > 0:
                    # If no files were changed before but some were deleted, add the scan_session to the session
                    if changed_files == 0:
                        session.add(scan_session)
                        await session.flush()  # Ensure scan_session gets its ID

                    # Update the changed_files count to include deleted files
                    changed_files += deleted_files
                    scan_session.changed_files = changed_files
                    logger.info(f"Updated changed_files count to include {deleted_files} deleted files, total: {changed_files}")

                # Log the final status
                if changed_files > 0:
                    logger.info(f"Recorded scan session with {changed_files} total changed files")
                else:
                    logger.info("No changes detected, not recording scan session")

                # PHASE 5: Finalizing (Spinner)
                logger.info("Finalizing database transactions...")
                with console.status(
                    "[bold green]Finalisiere Datenbank-Transaktionen...", spinner="dots"
                ) as status:
                    # Der commit wurde bereits in insert_files_with_progress nach jedem Chunk durchgeführt
                    # Ein finaler commit hier ist redundant, wenn alle Chunks committed wurden
                    # aber kann nicht schaden, falls noch pending changes vom Dir-Scan sind

                    # Commit any pending changes (directory updates, etc.)
                    # If no files were changed, this won't include a ScanSession
                    await session.commit()

                    status.update(
                        "[bold green]Datenbank-Transaktionen abgeschlossen.[/bold green]"
                    )
                    await asyncio.sleep(
                        0.5
                    )  # Simuliert eine kurze abschließende Verzögerung
                logger.info("Database transactions completed.")

                console.print(
                    "[bold green]Scan abgeschlossen, Datenbank ist aktuell.[/bold green]"
                )
                logger.info("Scan completed, database is up to date.")

            except IntegrityError as e:
                await session.rollback()  # Rollback im Fehlerfall
                error_msg = f"Integrity error (possibly duplicate entries): {e}"
                console.print(f"[bold yellow]{error_msg}[/bold yellow]")
                logger.error(error_msg)
                raise typer.Exit(code=1)
            except Exception as e:
                await session.rollback()  # Rollback im Fehlerfall
                error_msg = f"General error during scan: {e}"
                console.print(f"[bold red]{error_msg}[/bold red]")
                logger.error(error_msg, exc_info=True)
                raise typer.Exit(code=1)

    asyncio.run(_scan())


@app.command(name="info")
def info_command(
    db_path: str = typer.Option(
        None,
        "--db-path",
        "-d",
        help="Path to the database file. By default, the value from the .env file is used.",
    ),
):
    """
    Display information about the latest BluStash scan session.

    This command retrieves and displays details about the most recent scan session,
    including:
    - Session UUID
    - Session ID (number)
    - When the session was started
    - Total files processed

    If no sessions exist in the database, an appropriate message is displayed.
    """

    async def _info():
        # If db_path is provided, set it as an environment variable
        if db_path:
            os.environ["DB_PATH"] = db_path
            # Re-import engine with the updated environment variable
            from bluestash.db.models import engine as updated_engine
            current_engine = updated_engine
        else:
            current_engine = engine

        try:
            # Initialize database connection
            async with current_engine.begin() as conn:
                await conn.run_sync(reg.metadata.create_all)

            # Get the latest session information
            async with get_async_session() as session:
                latest_session = await get_latest_session_info(session)

                if latest_session:
                    # Format the timestamp for display
                    timestamp_str = latest_session['timestamp'].strftime('%Y-%m-%d %H:%M:%S %Z')

                    # Display the information using rich formatting
                    console.print("[bold blue]Latest BluStash Session Information:[/bold blue]")
                    console.print(f"[green]Session UUID:[/green] {latest_session['uuid']}")
                    console.print(f"[green]Session Number:[/green] {latest_session['id']}")
                    console.print(f"[green]Started At:[/green] {timestamp_str}")
                    console.print(f"[green]Changed Files Processed:[/green] {latest_session['changed_files']}")
                else:
                    console.print("[yellow]No scan sessions found in the database.[/yellow]")

        except Exception as e:
            error_msg = f"Error retrieving session information: {e}"
            console.print(f"[bold red]{error_msg}[/bold red]")
            logger.error(error_msg, exc_info=True)
            raise typer.Exit(code=1)

    asyncio.run(_info())


if __name__ == "__main__":
    app()
