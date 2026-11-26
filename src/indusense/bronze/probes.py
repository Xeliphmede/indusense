"""Description de la source réelle d'un contrat, avant toute lecture des données.

- CSV : seul l'en-tête est lu (BOM retiré, doublons détectés).
- SQL : le dump est analysé par sqlglot, jamais exécuté : colonnes déclarées (CREATE TABLE), colonnes réellement
  fournies (INSERT) et nombre de lignes.
"""

import csv
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot

from indusense.sql import declared_types, iter_inserts, parse_sql


class SourceInspectionError(Exception):
    """Source illisible ou introuvable ; le message nomme le fichier."""


@dataclass(frozen=True)
class SourceSchema:
    """Ce que la source contient réellement."""

    label: str                    # telemetry.csv | machine.sql.txt:maintenance
    declared: list[str]           # colonnes connues (en-tête CSV, CREATE TABLE + INSERT)
    provided: list[str]           # colonnes dont les données fournissent la valeur
    rows: int | None = None       # nombre de lignes (SQL ; None pour un CSV, dont seul l'en-tête est lu)
    has_ddl: bool = False         # SQL : la table est déclarée par un CREATE TABLE
    duplicates: list[str] = field(default_factory=list)  # noms de colonnes en double


def probe_csv(path: Path, encoding: str, delimiter: str) -> SourceSchema:
    """En-tête d'un CSV."""
    try:
        with path.open(encoding=encoding, newline="") as f:
            header = next(csv.reader(f, delimiter=delimiter), [])
    except FileNotFoundError as error:
        raise SourceInspectionError(f"{path.name}: file not found") from error
    except UnicodeDecodeError as error:
        raise SourceInspectionError(f"{path.name}: cannot be decoded as {encoding}: {error.reason}") from error
    if header:
        header[0] = header[0].removeprefix("﻿")  # BOM : lu avec utf-8 (et non utf-8-sig)
    duplicates = [name for name, count in Counter(header).items() if count > 1]
    return SourceSchema(path.name, header, header, duplicates=duplicates)


def probe_sql(path: Path, encoding: str, table: str) -> SourceSchema:
    """Colonnes déclarées, colonnes fournies et nombre de lignes d'une table d'un dump SQL."""
    try:
        statements = parse_sql(path, encoding)
    except FileNotFoundError as error:
        raise SourceInspectionError(f"{path.name}: file not found") from error
    except UnicodeDecodeError as error:
        raise SourceInspectionError(f"{path.name}: cannot be decoded as {encoding}: {error.reason}") from error
    except sqlglot.errors.ParseError as error:
        raise SourceInspectionError(f"{path.name}: SQL parse error: {error}") from error

    ddl = declared_types(statements).get(table)
    provided, rows = [], 0
    for name, columns, values in iter_inserts(statements):
        if name == table:
            provided += [col for col in columns if col not in provided]
            rows += len(values)
    if ddl is None and not provided:
        raise SourceInspectionError(f"{path.name}: table '{table}' is neither created nor filled by the dump")
    declared = list(ddl or {}) + [col for col in provided if col not in (ddl or {})]
    return SourceSchema(f"{path.name}:{table}", declared, provided, rows=rows, has_ddl=ddl is not None)


def probe_source(contract: dict, path: Path) -> SourceSchema:
    """Description de la source d'un contrat (format CSV ou SQL)."""
    source = contract["source"]
    if source["format"] == "CSV":
        return probe_csv(path, source["encoding"], source["field_delimiter"])
    return probe_sql(path, source["encoding"], source["table"])
