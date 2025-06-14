"""Wrapper utilities for interacting with xorriso.

This module provides convenient functions to burn data to optical discs
using xorriso through mapping files. It also allows listing sessions and
extracting previous sessions.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterable, List, Dict

from . import setup_logging

logger = setup_logging(logger_name="bluestash.xorriso")


def build_mapping_list(files: Iterable[str], session_dir: str, output_file: str) -> None:
    """Write a mapping list for xorriso.

    Each source file is mapped into the given session directory on disc.
    The mapping list format requires absolute paths for both source and
    destination.
    """
    out_path = Path(output_file)
    lines: List[str] = []
    session_dir = session_dir.strip("/")

    for file_path in files:
        src = Path(file_path).resolve()
        dest = Path("/") / session_dir / src.name
        lines.append(f"{src} {dest}")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    logger.debug("Wrote mapping list to %s", out_path)


def run_xorriso(device: str, mapping_file: str, finalize: bool = False) -> subprocess.CompletedProcess:
    """Invoke xorriso with a mapping file.

    Parameters
    ----------
    device : str
        Device path, e.g. "/dev/sr0".
    mapping_file : str
        Path to mapping list file.
    finalize : bool, optional
        Whether to finalize/close the disc after burning.

    Returns
    -------
    subprocess.CompletedProcess
        Result from ``subprocess.run``.
    """
    cmd = [
        "sudo",
        "xorriso",
        "-dev",
        device,
        "-map_l",
        str(Path(mapping_file).resolve()),
        "-commit",
    ]
    if finalize:
        cmd.extend(["-close", "on"])

    logger.info("Running xorriso: %s", " ".join(cmd))
    return subprocess.run(cmd, text=True, capture_output=True)


def list_sessions(device: str) -> List[Dict[str, str]]:
    """Return a list of sessions present on the disc."""
    cmd = ["sudo", "xorriso", "-indev", device, "-toc"]
    logger.info("Listing sessions via xorriso")
    result = subprocess.run(cmd, text=True, capture_output=True)

    sessions: List[Dict[str, str]] = []
    for line in result.stdout.splitlines():
        if line.startswith("ISO session"):  # e.g. "ISO session 3 :"
            parts = line.split()
            if len(parts) >= 4:
                sessions.append({"number": parts[2].strip("#"), "raw": line})
    return sessions


def extract_session(device: str, session_num: int, output_dir: str) -> subprocess.CompletedProcess:
    """Extract files from a session to the output directory."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    cmd = [
        "sudo",
        "xorriso",
        "-indev",
        device,
        "-load",
        "session_no",
        str(session_num),
        "-osirrox",
        "on",
        "-extract",
        "/",
        str(out.resolve()),
    ]
    logger.info("Extracting session %s to %s", session_num, out)
    return subprocess.run(cmd, text=True, capture_output=True)


def verify_disc(device: str) -> subprocess.CompletedProcess:
    """Run xorriso to verify the disc using MD5 checksums."""
    cmd = ["sudo", "xorriso", "-indev", device, "-check_md5", "FAILURE"]
    logger.info("Verifying disc")
    return subprocess.run(cmd, text=True, capture_output=True)
