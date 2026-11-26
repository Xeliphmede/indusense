"""Lecture en flux des lignes d'une source, en texte brut : aucune conversion, aucun nettoyage.

Chaque ligne est un couple (numéro d'enregistrement, {source_name: texte | None}) ; le numéro commence à 1 pour
la première ligne de données. Une valeur None signifie « absente » (NULL SQL, champ manquant d'une ligne CSV
trop courte) ; "" est une valeur vide présente dans le fichier.
"""

import csv
from collections.abc import Iterator
from pathlib import Path

from indusense.sql import iter_inserts, literal_text, parse_sql

ROW_ERRORS = "_row"  # clé de _cast_errors pour un défaut de la ligne entière (nombre de champs)

Row = tuple[int, dict[str, str | None], dict[str, str]]  # (numéro, valeurs brutes, erreurs de structure)


def iter_csv_rows(path: Path, encoding: str, delimiter: str, columns: list[str]) -> Iterator[Row]:
    """Lignes d'un CSV, lues en flux. Les retours à la ligne entre guillemets restent dans la valeur ;
    le numéro d'enregistrement peut donc différer du numéro de ligne physique."""
    with path.open(encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        header[0] = header[0].removeprefix("﻿")
        index = {name: header.index(name) for name in columns}
        for number, fields in enumerate(reader, start=1):
            errors = {}
            if len(fields) != len(header):
                errors[ROW_ERRORS] = f"{len(fields)} fields, {len(header)} expected"
            yield number, {name: fields[i] if i < len(fields) else None for name, i in index.items()}, errors


def iter_sql_rows(path: Path, encoding: str, table: str, columns: list[str]) -> Iterator[Row]:
    """Lignes des INSERT d'une table d'un dump SQL ; chaque littéral est rendu en texte, sans conversion."""
    number = 0
    for name, insert_columns, rows in iter_inserts(parse_sql(path, encoding)):
        if name != table:
            continue
        for values in rows:
            number += 1
            texts = dict(zip(insert_columns, map(literal_text, values)))
            errors = {}
            if len(values) != len(insert_columns):
                errors[ROW_ERRORS] = f"{len(values)} values, {len(insert_columns)} expected"
            yield number, {col: texts.get(col) for col in columns}, errors


def iter_rows(contract: dict, path: Path) -> Iterator[Row]:
    """Lignes de la source d'un contrat (format CSV ou SQL), restreintes aux colonnes du contrat."""
    source = contract["source"]
    columns = [entry["source_name"] for entry in contract["columns"]]
    if source["format"] == "CSV":
        return iter_csv_rows(path, source["encoding"], source["field_delimiter"], columns)
    return iter_sql_rows(path, source["encoding"], source["table"], columns)
