"""Schéma silver construit à partir des contrats : une table typée par contrat, à côté du bronze.

Pour chaque colonne du contrat, une colonne `<col>` du type `silver_type` (NOT NULL si `nullable: false`), puis
la traçabilité :
- `_batch_id` : l'exécution d'ingestion bronze dont viennent les lignes ;
- `_bronze_id` : clé primaire, et clé étrangère vers `bronze.<table>._id` : la ligne bronze exacte (fichier,
  numéro de ligne, valeur brute, hash) ;
- `_cast_at` : heure du typage.

Une ligne par clé naturelle (`natural_key`) et par batch : contrainte d'unicité `uq_silver_<table>_natural_key`.
Les lignes bronze écartées par le dédoublonnage (ADR-004) sont consignées dans `silver._duplicate`.

Les tables silver sont ajoutées à la métadonnée bronze (même `MetaData`) : Alembic compare les deux schémas et la
clé étrangère se résout.
"""

import re

from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Double, ForeignKey, Index, Integer, MetaData,
                        Numeric, PrimaryKeyConstraint, REAL, SmallInteger, String, Table, Text, Time, UniqueConstraint,
                        Uuid, CHAR)
from sqlalchemy.types import TypeEngine

from indusense.bronze import schema as bronze

SCHEMA = "silver"
REGISTRY_TABLE = "_cast_run"
DUPLICATE_TABLE = "_duplicate"
TECHNICAL_COLUMNS = ["_batch_id", "_bronze_id", "_cast_at"]

SIMPLE_TYPES = {
    "TEXT": Text,
    "SMALLINT": SmallInteger,
    "INTEGER": Integer,
    "BIGINT": BigInteger,
    "UUID": Uuid,
    "DOUBLE PRECISION": Double,
    "REAL": REAL,
    "BOOLEAN": Boolean,
    "DATE": Date,
    "TIME": Time,
    "TIMESTAMPTZ": lambda: DateTime(timezone=True),
    "TIMESTAMP WITH TIME ZONE": lambda: DateTime(timezone=True),
}


def sql_type(silver_type: str) -> TypeEngine:
    """Type SQLAlchemy d'un `silver_type` du contrat (types acceptés par la règle de validation 6)."""
    name = silver_type.upper().strip()
    if name in SIMPLE_TYPES:
        return SIMPLE_TYPES[name]()
    if match := re.fullmatch(r"(?:VARCHAR|CHARACTER VARYING)(?:\((\d+)\))?", name):
        return String(int(match[1])) if match[1] else String()
    if match := re.fullmatch(r"CHAR\((\d+)\)", name):
        return CHAR(int(match[1]))
    if match := re.fullmatch(r"NUMERIC(?:\((\d+)(?:,(\d+))?\))?", name):
        return Numeric(int(match[1]), int(match[2] or 0)) if match[1] else Numeric()
    raise ValueError(f"unsupported silver_type: {silver_type!r}")


def table_name(contract: dict) -> str:
    return contract["target"]["table"]


def contract_table(contract: dict, metadata: MetaData) -> Table:
    """Table silver d'un contrat : colonnes typées, puis _batch_id, _bronze_id (PK, FK bronze) et _cast_at."""
    table = table_name(contract)
    columns = [Column(entry["target_name"], sql_type(entry["silver_type"]), nullable=entry.get("nullable", True),
                      comment=entry.get("description") or f"'{entry['target_name']}' typé en {entry['silver_type']}")
               for entry in contract["columns"]]
    return Table(
        table,
        metadata,
        *columns,
        Column("_batch_id", Uuid, nullable=False,
               comment="Exécution d'ingestion bronze (UUID) dont vient la ligne (voir bronze._ingestion_batch)"),
        Column("_bronze_id", BigInteger, ForeignKey(f"{bronze.SCHEMA}.{table}._id"), primary_key=True,
               comment=f"Ligne d'origine : bronze.{table}._id"),
        Column("_cast_at", DateTime(timezone=True), nullable=False,
               comment="Instant UTC du typage (exécution silver)"),
        Index(f"ix_silver_{table}_batch_id", "_batch_id"),
        *([UniqueConstraint(*contract["target"]["natural_key"], "_batch_id", name=f"uq_silver_{table}_natural_key")]
          if contract["target"].get("natural_key") else []),
        schema=SCHEMA,
        comment=f"Silver : bronze.{table} typé selon le contrat",
    )


def registry_table(metadata: MetaData) -> Table:
    """Registre des exécutions silver : une ligne par contrat et par exécution (run_id), écrite hors de la
    transaction d'écriture. Un échec (erreurs de cast) y reste consigné."""
    return Table(
        REGISTRY_TABLE,
        metadata,
        Column("run_id", Uuid, nullable=False, comment="Exécution silver (UUID)"),
        Column("batch_id", Uuid, nullable=False, comment="Exécution d'ingestion bronze typée"),
        Column("contract", Text, nullable=False),
        Column("target_table", Text, nullable=False),
        Column("status", Text, nullable=False, comment="RUNNING, SUCCESS, FAILED ou ROLLED_BACK"),
        Column("rows_read", BigInteger),
        Column("rows_inserted", BigInteger),
        Column("cast_errors", BigInteger, comment="Nombre de valeurs non typables"),
        Column("rows_deduplicated", BigInteger, comment="Lignes bronze écartées par le dédoublonnage (ADR-004)"),
        Column("error", Text),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("completed_at", DateTime(timezone=True)),
        PrimaryKeyConstraint("run_id", "contract"),
        schema=SCHEMA,
        comment="Registre des exécutions silver : quel batch bronze a été typé, avec quel résultat",
    )


def duplicate_table(metadata: MetaData) -> Table:
    """Lignes bronze écartées par le dédoublonnage : chacune avec la ligne gardée à sa place (ADR-004)."""
    return Table(
        DUPLICATE_TABLE,
        metadata,
        Column("target_table", Text, nullable=False, comment="Table silver dédoublonnée (silver.telemetry...)"),
        Column("bronze_id", BigInteger, nullable=False, comment="Ligne bronze écartée (bronze.<table>._id)"),
        Column("kept_bronze_id", BigInteger, nullable=False,
               comment="Ligne bronze gardée en silver à sa place (même clé naturelle)"),
        Column("batch_id", Uuid, nullable=False, comment="Exécution d'ingestion bronze"),
        Column("run_id", Uuid, nullable=False, comment="Exécution silver qui a écarté la ligne"),
        Column("natural_key", Text, nullable=False, comment="Valeurs typées de la clé naturelle (JSON)"),
        Column("missing_values", Integer, nullable=False, comment="NULL de la ligne écartée"),
        Column("kept_missing_values", Integer, nullable=False, comment="NULL de la ligne gardée"),
        Column("reason", Text, nullable=False, comment="Règle appliquée"),
        PrimaryKeyConstraint("target_table", "bronze_id"),
        Index("ix_silver__duplicate_batch_id", "batch_id"),
        schema=SCHEMA,
        comment="Lignes bronze écartées par le dédoublonnage silver sur la clé naturelle (ADR-004)",
    )


def build_metadata(contracts: dict[str, dict] | list[dict]) -> MetaData:
    """Métadonnée complète : schéma bronze, puis une table silver par contrat et le registre silver."""
    metadata = bronze.build_metadata(contracts)
    for contract in (contracts.values() if isinstance(contracts, dict) else contracts):
        contract_table(contract, metadata)
    registry_table(metadata)
    duplicate_table(metadata)
    return metadata
