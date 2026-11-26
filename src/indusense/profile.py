"""Profil d'un fichier de données : tout ce que dit la carte d'identité, sous forme sérialisable en JSON.

Le profil est calculé une fois puis
- rendu en Markdown (carte d'identité, voir card.py),
- écrit en JSON, base du contrat d'ingestion bronze (voir bronze/contract.py).
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from indusense.inference import STATS_TYPES, infer_column_type, type_notes
from indusense.reading import detect_charset, detect_row_delimiter, detect_separator, read_csv_frame
from indusense.sql import SQL_DIALECT, extract_sql_tables, parse_sql
from indusense.stats import column_stats

CSV_SUFFIXES = {".csv", ".tsv"}


def file_format(path: Path) -> str:
    """CSV (`.csv`, `.tsv`) ou SQL (`.sql`, `.sql.txt`...) ; tout autre fichier est refusé."""
    suffixes = [suffix.lower() for suffix in Path(path).suffixes]
    if ".sql" in suffixes:
        return "SQL"
    if suffixes and suffixes[-1] in CSV_SUFFIXES:
        return "CSV"
    raise ValueError("format non pris en charge (attendu : .csv, .tsv ou .sql)")


def column_profile(series: pd.Series, sql_type: str | None, invalid: int) -> dict:
    """Profil d'une colonne présente dans les données : type inféré, remarques de lecture, statistiques."""
    col_type = infer_column_type(series)
    return {
        "name": str(series.name),
        "inferred_type": col_type,
        "notes": type_notes(series, col_type, invalid),
        "sql_type": sql_type,
        "provided": True,
        "invalid": invalid,
        "missing": int(series.isna().sum()) - invalid,  # vides ou null (les illisibles sont comptés à part)
        "stats": column_stats(series, col_type) if col_type in STATS_TYPES else None,
    }


def table_profile(name: str, df: pd.DataFrame, sql_types: dict | None = None, invalid: dict | None = None) -> dict:
    """Profil d'une table. Avec sql_types (script SQL), les colonnes déclarées viennent en premier, y compris
    celles absentes des données (provided: false, remplies par une valeur par défaut)."""
    sql_types, invalid = sql_types or {}, invalid or {}
    names = list(sql_types) + [col for col in df.columns if col not in sql_types]
    columns = []
    for col in names:
        if col in df.columns:
            columns.append(column_profile(df[col], sql_types.get(col), invalid.get(col, 0)))
        else:
            columns.append({"name": col, "inferred_type": None, "notes": [], "sql_type": sql_types[col],
                            "provided": False, "invalid": 0, "missing": None, "stats": None})
    return {"name": name, "rows": len(df), "columns": columns}


def profile_file(path: Path) -> dict:
    """Profil complet d'un fichier CSV ou script SQL."""
    path = Path(path)
    fmt = file_format(path)
    began_at = datetime.now()
    charset, encoding = detect_charset(path)
    profile = {
        "source": str(path.resolve()),
        "name": path.name,
        "format": fmt,
        "filesize": path.stat().st_size,
        "charset": charset,
        "encoding": encoding,
    }
    if fmt == "CSV":
        sep = detect_separator(path, encoding)
        df, invalid = read_csv_frame(path, encoding, sep)
        profile |= {"field_delimiter": sep, "row_delimiter": detect_row_delimiter(path)}
        tables = [table_profile(path.name.split(".")[0], df, invalid=invalid)]
    else:
        statements = parse_sql(path, encoding)
        profile |= {"dialect": SQL_DIALECT, "statements": len(statements)}
        tables = [table_profile(name, df, sql_types) for name, (sql_types, df) in extract_sql_tables(statements).items()]
    return profile | {
        "began_at": began_at.isoformat(timespec="seconds"),
        "ended_at": datetime.now().isoformat(timespec="seconds"),
        "tables": tables,
    }
