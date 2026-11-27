"""Fidélité du silver au bronze : aucune ligne ni aucune valeur perdue par le typage.

Après l'écriture d'un batch, et avant le commit, chaque table silver est relue **telle que PostgreSQL l'a
stockée** et rapprochée de sa table bronze (`LEFT JOIN` sur `_bronze_id = _id`). Un schéma pandera, généré à partir
du contrat, vérifie :

- règle `present` : chaque ligne bronze du batch a sa ligne silver, **ou** est consignée dans `silver._duplicate`
  avec une ligne gardée bien présente en silver (dédoublonnage, ADR-004) : aucune ligne perdue sans trace ;
- pour chaque colonne, que la valeur silver restitue **exactement** la valeur bronze :
  - texte : identique caractère pour caractère (rien de tronqué ni d'altéré) ;
  - nombre : égal à la valeur décimale exacte du texte (`Decimal`), pas à un flottant qui en serait une
    approximation : un `DOUBLE PRECISION` ou un `NUMERIC` trop court est détecté ;
  - entier, booléen, date, heure, horodatage (étiqueté UTC, ADR-003), UUID : égal à la valeur lue dans le texte ;
  - vide en bronze si et seulement si `NULL` en silver ;
  - ligne écartée comme doublon : sa clé naturelle doit restituer celle de la ligne gardée (c'était bien un doublon).

Une seule différence et le batch n'est pas gardé en silver (voir loader.py).
"""

import uuid
from datetime import date, time
from decimal import Decimal

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import select, text

from indusense.bronze.schema import SCHEMA as BRONZE_SCHEMA
from indusense.silver.casting import (FLOAT_TYPES, INTEGER_RANGES, LENGTH_PATTERN, exact_decimal, float_decimal,
                                      to_boolean, to_timestamptz)
from indusense.silver.schema import DUPLICATE_TABLE, SCHEMA, table_name

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


PRESENT = "present"


def dropped(frame: pd.DataFrame) -> pd.Series:
    """Lignes bronze écartées comme doublons (absentes du silver, consignées dans silver._duplicate)."""
    return frame["_bronze_id"].isna() & frame["_kept_bronze_id"].notna()


def fidelity_schema(contract: dict) -> pa.DataFrameSchema:
    """Schéma pandera du rapprochement bronze/silver d'un contrat : présence de chaque ligne bronze (en silver ou
    écartée comme doublon), puis une règle par colonne. Pour une ligne écartée, seule la clé naturelle est
    comparée, à celle de la ligne gardée."""
    key = set(contract["target"].get("natural_key") or [])

    def restores_column(entry):
        name = entry["target_name"]
        compared = name in key

        def check(df):
            return pd.Series([True if is_dropped and not compared else restores(raw, value, entry)
                              for raw, value, is_dropped in zip(df[name + BRONZE_SUFFIX], df[name], dropped(df))],
                             index=df.index, dtype=bool)
        return pa.Check(check, name=check_name(name), error=check_name(name))  # pandera rapporte `error`

    def present(df):
        return df["_bronze_id"].notna() | (dropped(df) & df["_kept_row"].notna())

    return pa.DataFrameSchema(
        checks=[pa.Check(present, name=PRESENT, error=PRESENT), *(restores_column(e) for e in contract["columns"])],
        name=f"{SCHEMA}.{table_name(contract)}",
    )


def reconciliation_frame(conn, contract: dict, metadata, batch_id: uuid.UUID) -> pd.DataFrame:
    """Lignes bronze du batch, chacune à côté de sa ligne silver telle que stockée (valeurs Python, sans
    conversion pandas : un entier reste un entier, un Decimal reste un Decimal)."""
    table = table_name(contract)
    bronze = metadata.tables[f"{BRONZE_SCHEMA}.{table}"]
    silver = metadata.tables[f"{SCHEMA}.{table}"]
    kept = silver.alias("kept")
    duplicate = metadata.tables[f"{SCHEMA}.{DUPLICATE_TABLE}"]
    names = [entry["target_name"] for entry in contract["columns"]]
    query = (select(bronze.c._id, bronze.c._row_number, silver.c._bronze_id, duplicate.c.kept_bronze_id.label(
                        "_kept_bronze_id"), kept.c._bronze_id.label("_kept_row"),
                    *(bronze.c[n].label(n + BRONZE_SUFFIX) for n in names), *(silver.c[n] for n in names),
                    *(kept.c[n].label(n + "__kept") for n in names))
             .select_from(bronze.outerjoin(silver, silver.c._bronze_id == bronze.c._id)
                          .outerjoin(duplicate, (duplicate.c.target_table == f"{SCHEMA}.{table}")
                                     & (duplicate.c.bronze_id == bronze.c._id))
                          .outerjoin(kept, kept.c._bronze_id == duplicate.c.kept_bronze_id))
             .where(bronze.c._batch_id == batch_id).order_by(bronze.c._id))
    # Pas de requête parallèle : ses workers échangent par /dev/shm, limité à 64 Mo dans un conteneur Docker par
    # défaut (DiskFull dès que le bronze grossit) ; les jointures passent par les index, le parallélisme n'apporte rien.
    conn.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
    result = conn.execute(query)
    frame = pd.DataFrame(result.all(), columns=list(result.keys()), dtype=object)
    is_dropped = dropped(frame)
    for name in names:  # ligne écartée : on la compare à la ligne gardée à sa place
        frame[name] = frame[name].where(~is_dropped, frame[name + "__kept"])
    return frame.drop(columns=[n + "__kept" for n in names])


def validate_fidelity(contract: dict, frame: pd.DataFrame) -> list:
    """Erreurs de fidélité (CastError) d'un rapprochement bronze/silver ; liste vide si rien n'est perdu."""
    from indusense.silver.loader import CastError

    try:
        fidelity_schema(contract).validate(frame, lazy=True)
        return []
    except pa.errors.SchemaErrors as failure:
        cases = failure.failure_cases
    table, errors = f"{BRONZE_SCHEMA}.{table_name(contract)}", []
    missing = set(cases[cases["check"] == PRESENT]["index"])
    for index in sorted(missing):
        row = frame.loc[index]
        reason = ("lost in silver: no silver row for this bronze row" if pd.isna(row["_kept_bronze_id"]) else
                  f"lost in silver: recorded as duplicate of bronze _id {row['_kept_bronze_id']}, "
                  f"which is not in silver")
        errors.append(CastError(table, "_bronze_id", int(row["_id"]), int(row["_row_number"]), None, reason))
    for entry in contract["columns"]:
        name = entry["target_name"]
        for index in sorted(set(cases[cases["check"] == check_name(name)]["index"]) - missing):
            row = frame.loc[index]
            raw = row[name + BRONZE_SUFFIX]
            if pd.isna(row["_bronze_id"]):
                reason = (f"dropped as duplicate of bronze _id {row['_kept_bronze_id']}, but its key {name} {raw!r} "
                          f"differs from the kept {row[name]!r}")
            else:
                reason = f"lost in silver: stored {row[name]!r} ({entry['silver_type']}) does not restore {raw!r}"
            errors.append(CastError(table, name, int(row["_id"]), int(row["_row_number"]), raw, reason))
    return errors


def check_fidelity(conn, contract: dict, metadata, batch_id: uuid.UUID) -> tuple[int, list]:
    """(nombre de valeurs vérifiées, erreurs) pour la table silver d'un contrat, relue dans la transaction `conn`."""
    frame = reconciliation_frame(conn, contract, metadata, batch_id)
    removed = int(dropped(frame).sum())
    checked = (len(frame) - removed) * len(contract["columns"]) + removed * len(contract["target"].get("natural_key") or [])
    return checked, validate_fidelity(contract, frame)
