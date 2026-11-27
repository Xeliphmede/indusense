"""Fidélité du silver au bronze : aucune ligne ni aucune valeur perdue par le typage.

Après l'écriture d'un batch, et avant le commit, chaque table silver est relue **telle que PostgreSQL l'a
stockée** et rapprochée de sa table bronze (`LEFT JOIN` sur `_bronze_id = _id`). Un schéma pandera, généré à partir
du contrat, vérifie :

- `_bronze_id` non nul : chaque ligne bronze du batch a sa ligne silver (aucune ligne perdue) ;
- pour chaque colonne, que la valeur silver restitue **exactement** la valeur bronze :
  - texte : identique caractère pour caractère (rien de tronqué ni d'altéré) ;
  - nombre : égal à la valeur décimale exacte du texte (`Decimal`), pas à un flottant qui en serait une
    approximation : un `DOUBLE PRECISION` ou un `NUMERIC` trop court est détecté ;
  - entier, booléen, date, heure, horodatage (étiqueté UTC, ADR-003), UUID : égal à la valeur lue dans le texte ;
  - vide en bronze si et seulement si `NULL` en silver.

Une seule différence et le batch n'est pas gardé en silver (voir loader.py).
"""

import uuid
from datetime import date, time
from decimal import Decimal

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import select

from indusense.bronze.schema import SCHEMA as BRONZE_SCHEMA
from indusense.silver.casting import (FLOAT_TYPES, INTEGER_RANGES, LENGTH_PATTERN, exact_decimal, float_decimal,
                                      to_boolean, to_timestamptz)
from indusense.silver.schema import SCHEMA, table_name

BRONZE_SUFFIX = "__bronze"


def is_text_type(silver_type: str) -> bool:
    return silver_type == "TEXT" or bool(LENGTH_PATTERN.fullmatch(silver_type)) or silver_type.startswith(
        ("VARCHAR", "CHARACTER VARYING"))


def restores(raw: str | None, value, entry: dict) -> bool:
    """La valeur silver `value` restitue-t-elle exactement la valeur bronze `raw` ?"""
    if raw is None or not raw.strip():
        return value is None
    if value is None:
        return False
    silver_type, text = entry["silver_type"].upper(), raw.strip()
    try:
        if silver_type.startswith("CHAR("):  # CHAR(n) complète par des espaces, qui n'ont pas de sens
            return value.rstrip(" ") == raw.rstrip(" ")
        if is_text_type(silver_type):
            return value == raw
        if silver_type in INTEGER_RANGES:
            return value == int(text)
        if silver_type in FLOAT_TYPES:
            return float_decimal(value, silver_type) == exact_decimal(text)
        if silver_type.startswith("NUMERIC"):
            return Decimal(value) == exact_decimal(text)
        if silver_type == "BOOLEAN":
            return value is to_boolean(text)
        if silver_type == "DATE":
            return value == date.fromisoformat(text)
        if silver_type == "TIME":
            return value == time.fromisoformat(text)
        if silver_type in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
            return value == to_timestamptz(text)
        if silver_type == "UUID":
            return value == uuid.UUID(text)
    except (ValueError, TypeError, ArithmeticError):
        return False
    return str(value) == raw


def check_name(column: str) -> str:
    return f"restores:{column}"


def fidelity_schema(contract: dict) -> pa.DataFrameSchema:
    """Schéma pandera du rapprochement bronze/silver d'un contrat : une règle par colonne, plus la présence de
    chaque ligne bronze en silver."""
    def restores_column(entry):
        name = entry["target_name"]
        return pa.Check(lambda df: pd.Series([restores(raw, value, entry) for raw, value in
                                              zip(df[name + BRONZE_SUFFIX], df[name])], index=df.index, dtype=bool),
                        name=check_name(name), error=check_name(name))  # pandera rapporte `error`

    return pa.DataFrameSchema(
        {"_bronze_id": pa.Column(nullable=False, description="ligne silver de chaque ligne bronze")},
        checks=[restores_column(entry) for entry in contract["columns"]],
        name=f"{SCHEMA}.{table_name(contract)}",
    )


def reconciliation_frame(conn, contract: dict, metadata, batch_id: uuid.UUID) -> pd.DataFrame:
    """Lignes bronze du batch, chacune à côté de sa ligne silver telle que stockée (valeurs Python, sans
    conversion pandas : un entier reste un entier, un Decimal reste un Decimal)."""
    table = table_name(contract)
    bronze = metadata.tables[f"{BRONZE_SCHEMA}.{table}"]
    silver = metadata.tables[f"{SCHEMA}.{table}"]
    names = [entry["target_name"] for entry in contract["columns"]]
    query = (select(bronze.c._id, bronze.c._row_number, silver.c._bronze_id,
                    *(bronze.c[n].label(n + BRONZE_SUFFIX) for n in names), *(silver.c[n] for n in names))
             .select_from(bronze.outerjoin(silver, silver.c._bronze_id == bronze.c._id))
             .where(bronze.c._batch_id == batch_id).order_by(bronze.c._id))
    result = conn.execute(query)
    return pd.DataFrame(result.all(), columns=list(result.keys()), dtype=object)


def validate_fidelity(contract: dict, frame: pd.DataFrame) -> list:
    """Erreurs de fidélité (CastError) d'un rapprochement bronze/silver ; liste vide si rien n'est perdu."""
    from indusense.silver.loader import CastError

    try:
        fidelity_schema(contract).validate(frame, lazy=True)
        return []
    except pa.errors.SchemaErrors as failure:
        cases = failure.failure_cases
    table, errors = f"{BRONZE_SCHEMA}.{table_name(contract)}", []
    missing = cases[(cases["column"] == "_bronze_id") & (cases["check"] == "not_nullable")]["index"]
    for index in sorted(set(missing)):
        row = frame.loc[index]
        errors.append(CastError(table, "_bronze_id", int(row["_id"]), int(row["_row_number"]), None,
                                "lost in silver: no silver row for this bronze row"))
    for entry in contract["columns"]:
        name = entry["target_name"]
        for index in sorted(set(cases[cases["check"] == check_name(name)]["index"])):
            row = frame.loc[index]
            if pd.isna(row["_bronze_id"]):
                continue  # ligne absente : déjà signalée
            raw = row[name + BRONZE_SUFFIX]
            errors.append(CastError(table, name, int(row["_id"]), int(row["_row_number"]), raw,
                                    f"lost in silver: stored {row[name]!r} ({entry['silver_type']}) "
                                    f"does not restore {raw!r}"))
    return errors


def check_fidelity(conn, contract: dict, metadata, batch_id: uuid.UUID) -> tuple[int, list]:
    """(nombre de valeurs vérifiées, erreurs) pour la table silver d'un contrat, relue dans la transaction `conn`."""
    frame = reconciliation_frame(conn, contract, metadata, batch_id)
    return len(frame) * len(contract["columns"]), validate_fidelity(contract, frame)
