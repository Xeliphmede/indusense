"""Lecture du fichier : taille, charset, séparateurs ; lecture et nettoyage d'un CSV en texte."""

import csv
from pathlib import Path

import pandas as pd

from indusense.inference import PARSE_THRESHOLD

SAMPLE_SIZE = 64 * 1024  # octets lus pour détecter séparateur et fin de ligne


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
        sample = f.read(SAMPLE_SIZE)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


def detect_row_delimiter(path: Path) -> str:
    """Fin de ligne du fichier : "\\r\\n" (Windows) si elle apparaît dans l'échantillon, sinon "\\n"."""
    with path.open("rb") as f:
        return "\r\n" if b"\r\n" in f.read(SAMPLE_SIZE) else "\n"


NULL_VALUES = {"", "null"}  # valeurs manquantes, casse ignorée (pandas en reconnaît bien plus : NA, None, n/a...)
DECIMAL_PATTERN = r"[+-]?(?:\d+(?:[.,]\d+)?|[.,]\d+)(?:[eE][+-]?\d+)?"  # 12 | 12.5 | 12,5 | -3,25 | .5 | 1e-3
LEADING_ZERO_PATTERN = r"[+-]?0\d"  # 0012 : un code, pas un nombre


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Retire les espaces autour des valeurs texte ; vide ou null (casse ignorée) devient une valeur manquante."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            text = df[col].str.strip()
            df[col] = text.mask(text.str.lower().isin(NULL_VALUES))
    return df


def parse_numbers(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Convertit en nombres (point ou virgule décimale) les colonnes texte dont au moins PARSE_THRESHOLD des
    valeurs sont des nombres ; les autres valeurs deviennent manquantes. Une colonne sans décimale reste entière
    (Int64) même avec des valeurs manquantes. Retourne aussi le nombre de valeurs illisibles par colonne convertie."""
    invalid = {}
    for col in df.columns:
        text = df[col].dropna().astype(str)
        if pd.api.types.is_numeric_dtype(df[col]) or text.empty:
            continue
        is_number = text.str.fullmatch(DECIMAL_PATTERN)
        if is_number.mean() < PARSE_THRESHOLD or text.str.match(LEADING_ZERO_PATTERN).any():
            continue
        numbers = pd.to_numeric(text[is_number].str.replace(",", ".", regex=False))
        if pd.api.types.is_integer_dtype(numbers):
            numbers = numbers.astype("Int64")  # int64 passerait en float à cause des valeurs manquantes
        df[col] = numbers.reindex(df.index)
        invalid[col] = int((~is_number).sum())
    return df, invalid


def read_csv_frame(path: Path, encoding: str, sep: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Lit le CSV entièrement en texte (pandas ne convertit rien), le nettoie et convertit les colonnes
    numériques ; retourne aussi le nombre de valeurs illisibles par colonne numérique."""
    raw = pd.read_csv(path, sep=sep, encoding=encoding, dtype=str, na_filter=False, skipinitialspace=True)
    return parse_numbers(clean_text(raw))
