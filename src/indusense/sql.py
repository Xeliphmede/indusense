"""Lecture d'un script SQL avec sqlglot : types déclarés (CREATE TABLE) et données (INSERT ... VALUES)."""

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pandas as pd
import sqlglot
from sqlglot import exp

from indusense.reading import clean_text

SQL_DIALECT = "postgres"


def parse_sql(path: Path, encoding: str) -> list[exp.Expression]:
    """Instructions du script, analysé sans jamais être exécuté."""
    return sqlglot.parse(path.read_text(encoding=encoding), read=SQL_DIALECT)


def sql_value(node: exp.Expression):
    """Valeur Python d'un littéral SQL (les NUMERIC, lus en Decimal, deviennent des float)."""
    value = node.to_py()
    return float(value) if isinstance(value, Decimal) else value


def literal_text(node: exp.Expression) -> str | None:
    """Texte d'origine d'un littéral SQL, sans conversion : '2.24' reste "2.24", 'abc' donne "abc",
    NULL donne None ; une autre expression (-1, NOW()...) est rendue en SQL."""
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Literal):
        return node.this
    return node.sql(SQL_DIALECT)


def declared_types(statements: list[exp.Expression]) -> dict[str, dict[str, str]]:
    """Types déclarés par CREATE TABLE : {table: {colonne: type SQL}}."""
    sql_types = {}
    for statement in statements:
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            schema = statement.this
            sql_types[schema.this.name] = {
                col.name: col.args["kind"].sql(SQL_DIALECT)
                for col in schema.expressions
                if isinstance(col, exp.ColumnDef)
            }
    return sql_types


def iter_inserts(statements: list[exp.Expression]) -> Iterator[tuple[str, list[str], list[list[exp.Expression]]]]:
    """Pour chaque INSERT ... VALUES : (table, colonnes, lignes de littéraux). Sans liste de colonnes
    (INSERT INTO table VALUES ...), toutes les colonnes déclarées par CREATE TABLE."""
    sql_types = declared_types(statements)
    for statement in statements:
        if isinstance(statement, exp.Insert) and isinstance(statement.expression, exp.Values):
            target = statement.this
            if isinstance(target, exp.Schema):  # INSERT INTO table (col1, col2, ...)
                name, columns = target.this.name, [col.name for col in target.expressions]
            else:  # INSERT INTO table VALUES ... : toutes les colonnes déclarées
                name, columns = target.name, list(sql_types.get(target.name, {}))
            yield name, columns, [row.expressions for row in statement.expression.expressions]


def sql_frame(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Réunit les lignes des INSERT d'une table (valeurs Python brutes) et type les colonnes : une colonne
    d'entiers reste entière (Int64) même avec des NULL, que pandas passerait sinon en float."""
    df = pd.concat(frames, ignore_index=True)
    for col in df.columns:
        values = df[col].dropna()
        if len(values) and all(type(v) is int for v in values):  # bool exclu : type(True) is bool
            df[col] = df[col].astype("Int64")
    return clean_text(df.infer_objects())


def extract_sql_tables(statements: list[exp.Expression]) -> dict[str, tuple[dict, pd.DataFrame]]:
    """Pour chaque table : (types déclarés par CREATE TABLE, données des INSERT ... VALUES)."""
    sql_types, frames = declared_types(statements), {}
    for name, columns, rows in iter_inserts(statements):
        values = [[sql_value(v) for v in row] for row in rows]
        frames.setdefault(name, []).append(pd.DataFrame(values, columns=columns, dtype=object))
    return {
        name: (sql_types.get(name, {}), sql_frame(frames[name]) if name in frames else pd.DataFrame())
        for name in dict.fromkeys([*sql_types, *frames])
    }
