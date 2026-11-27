"""silver : une ligne par clé naturelle et par batch (ADR-004).

- `silver._duplicate` consigne chaque ligne bronze écartée, avec la ligne gardée à sa place ;
- `silver._cast_run.rows_deduplicated` compte les lignes écartées par exécution ;
- contrainte d'unicité `(natural_key..., _batch_id)` sur chaque table silver.

Reprise de l'existant : les batches déjà typés contiennent des doublons de télémétrie. Avant de poser la contrainte,
ils sont dédoublonnés en SQL avec la règle d'ADR-004 (la ligne la plus complète, puis le plus petit `_bronze_id`),
et chaque ligne écartée est consignée dans `silver._duplicate`, rattachée à la dernière exécution silver réussie
de son batch.

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-27
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0007'
down_revision: str | Sequence[str] | None = '0006'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

REASON = ("même clé naturelle : gardée la ligne la plus complète (le moins de NULL), puis la première du fichier "
          "(migration 0007, lignes déjà typées)")
# table -> (clé naturelle, colonnes nullables : les seules qui peuvent compter comme NULL)
TABLES = {
    "machine": (["machine_code"], []),
    "maintenance": (["maintenance_id"], ["related_incident_id"]),
    "releves_incidents": (["incident_id"], []),
    "telemetry": (["machine_id", "timestamp"], ["temperature_c", "pressure_bar", "rotation_mean_rpm"]),
}


def deduplicate_existing_rows(table: str, key: list[str], nullable: list[str]) -> None:
    columns = ", ".join(f'"{c}"' for c in key)
    missing = " + ".join(f'("{c}" IS NULL)::int' for c in nullable) or "0"
    natural_key = ", ".join(f"'{c}', \"{c}\"" for c in key)
    op.execute(sa.text(f"""
        WITH ranked AS (
            SELECT _bronze_id, _batch_id, {columns}, {missing} AS missing,
                   row_number() OVER w AS rank, first_value(_bronze_id) OVER w AS kept_bronze_id,
                   first_value({missing}) OVER w AS kept_missing
            FROM silver.{table}
            WINDOW w AS (PARTITION BY _batch_id, {columns} ORDER BY {missing}, _bronze_id)
        )
        INSERT INTO silver._duplicate (target_table, bronze_id, kept_bronze_id, batch_id, run_id, natural_key,
                                       missing_values, kept_missing_values, reason)
        SELECT 'silver.{table}', _bronze_id, kept_bronze_id, _batch_id,
               (SELECT run_id FROM silver._cast_run c WHERE c.batch_id = ranked._batch_id AND c.status = 'SUCCESS'
                ORDER BY started_at DESC LIMIT 1),
               json_build_object({natural_key})::text, missing, kept_missing, :reason
        FROM ranked WHERE rank > 1
    """).bindparams(reason=REASON))
    op.execute(f"DELETE FROM silver.{table} s USING silver._duplicate d "
               f"WHERE d.target_table = 'silver.{table}' AND d.bronze_id = s._bronze_id")


def upgrade() -> None:
    op.create_table('_duplicate',
    sa.Column('target_table', sa.Text(), nullable=False, comment='Table silver dédoublonnée (silver.telemetry...)'),
    sa.Column('bronze_id', sa.BigInteger(), nullable=False, comment='Ligne bronze écartée (bronze.<table>._id)'),
    sa.Column('kept_bronze_id', sa.BigInteger(), nullable=False, comment='Ligne bronze gardée en silver à sa place (même clé naturelle)'),
    sa.Column('batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze"),
    sa.Column('run_id', sa.Uuid(), nullable=False, comment='Exécution silver qui a écarté la ligne'),
    sa.Column('natural_key', sa.Text(), nullable=False, comment='Valeurs typées de la clé naturelle (JSON)'),
    sa.Column('missing_values', sa.Integer(), nullable=False, comment='NULL de la ligne écartée'),
    sa.Column('kept_missing_values', sa.Integer(), nullable=False, comment='NULL de la ligne gardée'),
    sa.Column('reason', sa.Text(), nullable=False, comment='Règle appliquée'),
    sa.PrimaryKeyConstraint('target_table', 'bronze_id', name=op.f('pk__duplicate')),
    schema='silver',
    comment='Lignes bronze écartées par le dédoublonnage silver sur la clé naturelle (ADR-004)'
    )
    op.create_index('ix_silver__duplicate_batch_id', '_duplicate', ['batch_id'], unique=False, schema='silver')
    op.add_column('_cast_run', sa.Column('rows_deduplicated', sa.BigInteger(), nullable=True, comment='Lignes bronze écartées par le dédoublonnage (ADR-004)'), schema='silver')
    for table, (key, nullable) in TABLES.items():
        deduplicate_existing_rows(table, key, nullable)
    op.create_unique_constraint('uq_silver_machine_natural_key', 'machine', ['machine_code', '_batch_id'], schema='silver')
    op.create_unique_constraint('uq_silver_maintenance_natural_key', 'maintenance', ['maintenance_id', '_batch_id'], schema='silver')
    op.create_unique_constraint('uq_silver_releves_incidents_natural_key', 'releves_incidents', ['incident_id', '_batch_id'], schema='silver')
    op.create_unique_constraint('uq_silver_telemetry_natural_key', 'telemetry', ['machine_id', 'timestamp', '_batch_id'], schema='silver')


def downgrade() -> None:
    # les lignes écartées ne sont pas restaurées : retyper le batch (indusense silver cast) les recalcule
    op.drop_constraint('uq_silver_telemetry_natural_key', 'telemetry', schema='silver', type_='unique')
    op.drop_constraint('uq_silver_releves_incidents_natural_key', 'releves_incidents', schema='silver', type_='unique')
    op.drop_constraint('uq_silver_maintenance_natural_key', 'maintenance', schema='silver', type_='unique')
    op.drop_constraint('uq_silver_machine_natural_key', 'machine', schema='silver', type_='unique')
    op.drop_column('_cast_run', 'rows_deduplicated', schema='silver')
    op.drop_index('ix_silver__duplicate_batch_id', table_name='_duplicate', schema='silver')
    op.drop_table('_duplicate', schema='silver')
