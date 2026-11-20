"""Lecture du fichier : taille, charset et séparateur."""

import csv
from pathlib import Path


def format_filesize(size: float) -> str:
    """Formate une taille en octets : 9999.99 [octets | Ko | Mo | Go]."""
    for unit in ("octets", "Ko", "Mo"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} Go"


def detect_charset(path: Path) -> tuple[str, str]:
    """Retourne (libellé du charset, encodage Python pour lire le fichier)."""
    raw = path.read_bytes()
    for label, encoding in (("ASCII", "ascii"), ("UTF-8", "utf-8-sig"), ("Windows-1252", "cp1252")):
        try:
            raw.decode(encoding)
            return label, encoding
        except UnicodeDecodeError:
            pass
    return "ISO-8859-1 (Latin-1)", "latin-1"


def detect_separator(path: Path, encoding: str) -> str:
    """Détecte le séparateur du CSV sur un échantillon du début du fichier."""
    with path.open(encoding=encoding, newline="") as f:
        sample = f.read(64 * 1024)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","
