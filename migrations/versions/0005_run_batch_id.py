"""bronze en ajout seul : clé primaire _id auto-générée, batch_id commun à toutes les tables d'une exécution.

Avant : clé primaire (_source_hash, _row_number), qui empêchait de réinsérer un fichier déjà chargé ; un
batch_id (UUID) par table et par chargement.
Après : chaque exécution insère toutes ses lignes sous un batch_id (UUID) commun aux quatre tables, avec la même
heure _ingested_at ; la clé primaire est _id, identifiant technique auto-généré (identity) ; _batch_id est indexé ;
le registre a une ligne par (batch_id, contrat) et dit quels fichiers chaque exécution a utilisés.

Reprise de l'existant : les lignes déjà chargées gardent leur batch_id et leur _ingested_at, et reçoivent un _id.
Chargées avant cette migration, elles ont un batch_id par table (elles n'avaient pas d'exécution commune).

Irréversible : l'ancienne clé primaire (_source_hash, _row_number) ne peut pas être rétablie une fois des
exécutions ajoutées.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | Sequence[str] | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "bronze"
TABLES = ["machine", "maintenance", "releves_incidents", "telemetry"]
REGISTRY = "_ingestion_batch"


def upgrade() -> None:
    for table in TABLES:
        op.drop_constraint(f"pk_{table}", table, type_="primary", schema=SCHEMA)
        op.alter_column(table, "_batch_id", comment="Exécution d'ingestion (UUID), commun à toutes les tables "
                                                    "(voir _ingestion_batch)",
                        existing_type=sa.Uuid(), existing_nullable=False, schema=SCHEMA)
        op.alter_column(table, "_ingested_at", comment="Instant UTC de l'exécution d'ingestion, le même pour toutes "
                                                       "les tables",
                        existing_type=sa.DateTime(timezone=True), existing_nullable=False,
                        existing_server_default=sa.text("now()"), schema=SCHEMA)
        op.add_column(table, sa.Column("_id", sa.BigInteger(), sa.Identity(always=True), nullable=False,
                                       comment="Identifiant technique de la ligne, auto-généré"), schema=SCHEMA)
        op.create_primary_key(f"pk_{table}", table, ["_id"], schema=SCHEMA)
        op.create_index(f"ix_{table}_batch_id", table, ["_batch_id"], schema=SCHEMA)

    op.drop_constraint(f"pk_{REGISTRY}", REGISTRY, type_="primary", schema=SCHEMA)
    op.drop_column(REGISTRY, "rows_skipped", schema=SCHEMA)
    op.create_primary_key(f"pk_{REGISTRY}", REGISTRY, ["batch_id", "contract"], schema=SCHEMA)
    op.alter_column(REGISTRY, "batch_id", comment="Exécution d'ingestion (UUID)", existing_type=sa.Uuid(),
                    existing_nullable=False, schema=SCHEMA)
    op.alter_column(REGISTRY, "status", comment="RUNNING, SUCCESS, FAILED ou ROLLED_BACK", existing_type=sa.Text(),
                    existing_nullable=False, schema=SCHEMA)
    op.create_table_comment(REGISTRY, "Registre des exécutions d'ingestion bronze : fichiers utilisés par chaque "
                                      "batch_id", schema=SCHEMA)


def downgrade() -> None:
    raise RuntimeError("0005 is irreversible: the (_source_hash, _row_number) primary key cannot be restored once "
                       "runs have been appended; recreate the database and reload the sources instead")
