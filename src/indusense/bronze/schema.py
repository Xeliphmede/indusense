"""Schéma bronze construit à partir des contrats : métadonnée SQLAlchemy, comparée à la base par Alembic.

Pour chaque colonne du contrat, deux colonnes TEXT, côte à côte :
- `<col>` : la valeur d'origine, intacte (le bronze ne convertit rien) ; seule exception, les colonnes
  `pseudonymize` reçoivent un pseudonyme (voir pseudonymization.py) ;
- `<col>_type` : le type de `<col>` déclaré par le contrat (IDENTIFIER, NUMBER, DATE...), identique sur toutes
  les lignes ; la conversion vers ce type est faite en silver.

Puis `hash`, l'empreinte des colonnes du fichier pour la ligne (voir fingerprint.py), et les colonnes techniques.
"""

from pathlib import Path

from alembic.config import Config
from sqlalchemy import (BigInteger, Column, DateTime, Identity, Index, MetaData, PrimaryKeyConstraint, Table, Text,
                        Uuid, text)
from sqlalchemy.engine import URL

from indusense.settings import PROJECT_ROOT, get_settings

SCHEMA = "bronze"
REGISTRY_TABLE = "_ingestion_batch"
TECHNICAL_COLUMNS = ["_batch_id", "_source_file", "_source_hash", "_row_number", "_ingested_at", "_id"]
HASH_COLUMN = "hash"
TYPE_SUFFIX = "_type"

NAMING_CONVENTION = {"pk": "pk_%(table_name)s"}


def column_comment(entry: dict) -> str:
    """Commentaire PostgreSQL d'une colonne bronze : sa colonne source, puis sa description éventuelle."""
    if entry.get("pseudonymize"):
        comment = f"Pseudonyme HMAC-SHA256 salé de '{entry['source_name']}'"
    else:
        comment = f"Valeur d'origine de '{entry['source_name']}'"
    return f"{comment} : {entry['description']}" if entry.get("description") else comment


def type_column(target_name: str) -> str:
    return f"{target_name}{TYPE_SUFFIX}"


def contract_table(contract: dict, metadata: MetaData) -> Table:
    """Table bronze d'un contrat : paires <col> / <col>_type, hash de la ligne, colonnes techniques.

    Le bronze est un historique en ajout seul : chaque exécution insère toutes les lignes de ses fichiers sous
    son _batch_id (indexé). La clé primaire est un identifiant technique auto-généré, _id."""
    columns = []
    for entry in contract["columns"]:
        name = entry["target_name"]
        columns.append(Column(name, Text, comment=column_comment(entry)))
        columns.append(Column(type_column(name), Text, nullable=False,
                              comment=f"Type de '{name}' déclaré par le contrat"))
    return Table(
        contract["target"]["table"],
        metadata,
        *columns,
        Column(HASH_COLUMN, Text, nullable=False,
               comment="SHA-256 des valeurs des colonnes du fichier pour cette ligne (colonnes ajoutées exclues)"),
        Column("_batch_id", Uuid, nullable=False,
               comment="Exécution d'ingestion (UUID), commun à toutes les tables (voir _ingestion_batch)"),
        Column("_source_file", Text, nullable=False, comment="Fichier source, relatif à la racine du projet"),
        Column("_source_hash", Text, nullable=False, comment="SHA-256 des octets du fichier source"),
        Column("_row_number", BigInteger, nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
        Column("_ingested_at", DateTime(timezone=True), nullable=False, server_default=text("now()"),
               comment="Instant UTC de l'exécution d'ingestion, le même pour toutes les tables"),
        Column("_id", BigInteger, Identity(always=True), primary_key=True,
               comment="Identifiant technique de la ligne, auto-généré"),
        Index(f"ix_{contract['target']['table']}_batch_id", "_batch_id"),
        comment=f"Bronze : {contract['source']['path']}" + (f" (table {contract['source']['table']})"
                                                            if contract["source"].get("table") else ""),
    )


def registry_table(metadata: MetaData) -> Table:
    """Registre des exécutions : une ligne par fichier chargé et par exécution (batch_id), écrite hors de la
    transaction de chargement. Dit quels fichiers (chemin et SHA-256) chaque ingestion a utilisés."""
    return Table(
        REGISTRY_TABLE,
        metadata,
        Column("batch_id", Uuid, nullable=False, comment="Exécution d'ingestion (UUID)"),
        Column("contract", Text, nullable=False),
        Column("target_table", Text, nullable=False),
        Column("source_file", Text, nullable=False),
        Column("source_hash", Text, nullable=False),
        Column("status", Text, nullable=False, comment="RUNNING, SUCCESS, FAILED ou ROLLED_BACK"),
        Column("rows_read", BigInteger),
        Column("rows_inserted", BigInteger),
        Column("error", Text),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("completed_at", DateTime(timezone=True)),
        PrimaryKeyConstraint("batch_id", "contract"),
        comment="Registre des exécutions d'ingestion bronze : fichiers utilisés par chaque batch_id",
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

