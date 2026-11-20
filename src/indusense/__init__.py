"""InduSense : cartes d'identité Markdown des fichiers de données (CSV et scripts SQL)."""

from pathlib import Path

from indusense.card import describe_csv, write_card
from indusense.sql import describe_sql

__all__ = ["describer", "write_card"]

CSV_SUFFIXES = {".csv", ".tsv"}


def describer(path: Path):
    """Fonction describe adaptée au fichier : script SQL (`.sql`, `.sql.txt`...) ou CSV (`.csv`, `.tsv`)."""
    suffixes = [suffix.lower() for suffix in Path(path).suffixes]
    if ".sql" in suffixes:
        return describe_sql
    if suffixes and suffixes[-1] in CSV_SUFFIXES:
        return describe_csv
    raise ValueError("format non pris en charge (attendu : .csv, .tsv ou .sql)")
