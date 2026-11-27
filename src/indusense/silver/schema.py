"""Schéma silver construit à partir des contrats : une table typée par contrat, à côté du bronze.

Pour chaque colonne du contrat, une colonne `<col>` du type `silver_type` (NOT NULL si `nullable: false`), puis
la traçabilité :
- `_batch_id` : l'exécution d'ingestion bronze dont viennent les lignes ;
- `_bronze_id` : clé primaire, et clé étrangère vers `bronze.<table>._id` : la ligne bronze exacte (fichier,
  numéro de ligne, valeur brute, hash) ;
- `_cast_at` : heure du typage.

Les tables silver sont ajoutées à la métadonnée bronze (même `MetaData`) : Alembic compare les deux schémas et la
clé étrangère se résout.
"""

import re

from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Double, ForeignKey, Index, Integer, MetaData,
                        Numeric, PrimaryKeyConstraint, REAL, SmallInteger, String, Table, Text, Time, Uuid, CHAR)
from sqlalchemy.types import TypeEngine

from indusense.bronze import schema as bronze

SCHEMA = "silver"
REGISTRY_TABLE = "_cast_run"
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
        Column("error", Text),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("completed_at", DateTime(timezone=True)),
        PrimaryKeyConstraint("run_id", "contract"),
        schema=SCHEMA,
        comment="Registre des exécutions silver : quel batch bronze a été typé, avec quel résultat",
    )


def build_metadata(contracts: dict[str, dict] | list[dict]) -> MetaData:
    """Métadonnée complète : schéma bronze, puis une table silver par contrat et le registre silver."""
    metadata = bronze.build_metadata(contracts)
    for contract in (contracts.values() if isinstance(contracts, dict) else contracts):
        contract_table(contract, metadata)
    registry_table(metadata)
    return metadata
