"""Schéma bronze construit à partir des contrats : métadonnée SQLAlchemy, comparée à la base par Alembic.

Chaque colonne du contrat devient une colonne TEXT qui reçoit la valeur d'origine, intacte : le bronze ne
convertit rien. Seule exception, les colonnes `pseudonymize` reçoivent un pseudonyme (voir pseudonymization.py). Le type visé (`type`, `silver_type`) est un contrat pour la couche silver, qui fera le typage.
"""

from pathlib import Path

from alembic.config import Config
from sqlalchemy import BigInteger, Column, DateTime, MetaData, PrimaryKeyConstraint, Table, Text, Uuid, text
from sqlalchemy.engine import URL

from indusense.settings import PROJECT_ROOT, get_settings

SCHEMA = "bronze"
REGISTRY_TABLE = "_ingestion_batch"
TECHNICAL_COLUMNS = ["_batch_id", "_source_file", "_source_hash", "_row_number", "_ingested_at"]

NAMING_CONVENTION = {"pk": "pk_%(table_name)s"}


def column_comment(entry: dict) -> str:
    """Commentaire PostgreSQL d'une colonne bronze : sa colonne source, puis sa description éventuelle."""
    if entry.get("pseudonymize"):
        comment = f"Pseudonyme HMAC-SHA256 salé de '{entry['source_name']}'"
    else:
        comment = f"Valeur d'origine de '{entry['source_name']}'"
    return f"{comment} : {entry['description']}" if entry.get("description") else comment


def contract_table(contract: dict, metadata: MetaData) -> Table:
    """Table bronze d'un contrat : une colonne TEXT par colonne source, puis colonnes techniques et clé primaire."""
    columns = [Column(entry["target_name"], Text, comment=column_comment(entry)) for entry in contract["columns"]]
    return Table(
        contract["target"]["table"],
        metadata,
        *columns,
        Column("_batch_id", Uuid, nullable=False, comment="Lot d'ingestion"),
        Column("_source_file", Text, nullable=False, comment="Fichier source, relatif à la racine du projet"),
        Column("_source_hash", Text, nullable=False, comment="SHA-256 des octets du fichier source"),
        Column("_row_number", BigInteger, nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
        Column("_ingested_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
        PrimaryKeyConstraint("_source_hash", "_row_number"),
        comment=f"Bronze : {contract['source']['path']}" + (f" (table {contract['source']['table']})"
                                                            if contract["source"].get("table") else ""),
    )


def registry_table(metadata: MetaData) -> Table:
    """Registre des lots : une ligne par chargement, écrite hors de la transaction de chargement."""
    return Table(
        REGISTRY_TABLE,
        metadata,
        Column("batch_id", Uuid, primary_key=True),
        Column("contract", Text, nullable=False),
        Column("target_table", Text, nullable=False),
        Column("source_file", Text, nullable=False),
        Column("source_hash", Text, nullable=False),
        Column("status", Text, nullable=False, comment="RUNNING, SUCCESS ou FAILED"),
        Column("rows_read", BigInteger),
        Column("rows_inserted", BigInteger),
        Column("rows_skipped", BigInteger, comment="Lignes déjà présentes (rejeu)"),
        Column("error", Text),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("completed_at", DateTime(timezone=True)),
        comment="Registre des lots d'ingestion bronze",
    )


def build_metadata(contracts: dict[str, dict] | list[dict]) -> MetaData:
    """Métadonnée du schéma bronze : une table par contrat et le registre des lots."""
    metadata = MetaData(schema=SCHEMA, naming_convention=NAMING_CONVENTION)
    for contract in (contracts.values() if isinstance(contracts, dict) else contracts):
        contract_table(contract, metadata)
    registry_table(metadata)
    return metadata


def alembic_config(url: URL | None = None) -> Config:
    """Configuration Alembic du projet ; l'URL vient de .env (via settings), jamais de alembic.ini."""
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(Path(PROJECT_ROOT / "migrations")))
    config.attributes["url"] = url or get_settings().url()
    return config

