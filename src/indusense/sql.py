"""Carte d'identité d'un script SQL : une section par table."""

from decimal import Decimal
from pathlib import Path

import pandas as pd
import sqlglot
from sqlglot import exp

from indusense.card import clean_text, describe_frame

SQL_DIALECT = "postgres"


def sql_value(node: exp.Expression):
    """Valeur Python d'un littéral SQL (les NUMERIC, lus en Decimal, deviennent des float)."""
    value = node.to_py()
    return float(value) if isinstance(value, Decimal) else value


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
    sql_types, frames = {}, {}
    for statement in statements:
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            schema = statement.this
            sql_types[schema.this.name] = {
                col.name: col.args["kind"].sql(SQL_DIALECT)
                for col in schema.expressions
                if isinstance(col, exp.ColumnDef)
            }
        elif isinstance(statement, exp.Insert) and isinstance(statement.expression, exp.Values):
            target = statement.this
            if isinstance(target, exp.Schema):  # INSERT INTO table (col1, col2, ...)
                name, columns = target.this.name, [col.name for col in target.expressions]
            else:  # INSERT INTO table VALUES ... : toutes les colonnes déclarées
                name, columns = target.name, list(sql_types.get(target.name, {}))
            rows = [[sql_value(v) for v in row.expressions] for row in statement.expression.expressions]
            frames.setdefault(name, []).append(pd.DataFrame(rows, columns=columns, dtype=object))
    return {
        name: (sql_types.get(name, {}), sql_frame(frames[name]) if name in frames else pd.DataFrame())
        for name in dict.fromkeys([*sql_types, *frames])
    }


def describe_sql(path: Path, encoding: str) -> list[str]:
    """Corps de la carte d'identité d'un script SQL : une section par table."""
    statements = sqlglot.parse(path.read_text(encoding=encoding), read=SQL_DIALECT)
    tables = extract_sql_tables(statements)
    lines = [
        f"- SQL Dialect: {SQL_DIALECT}",
        f"- Statements Number: {len(statements)}",
        f"- Tables Number: {len(tables)}",
    ]
    for name, (sql_types, df) in tables.items():
        lines += ["", f"## Table `{name}`", "", *describe_frame(df, sql_types, level="###")]
    return lines
