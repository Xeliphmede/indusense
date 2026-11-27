"""silver : schéma typé, une table par contrat, et registre des exécutions silver.

Chaque table silver reprend les colonnes du contrat, typées selon `silver_type` (NOT NULL si `nullable: false`),
puis `_batch_id` (exécution d'ingestion bronze), `_bronze_id` (clé primaire, clé étrangère vers
`bronze.<table>._id`) et `_cast_at`. Le registre `silver._cast_run` consigne chaque exécution silver.

Aucune donnée n'est reprise : le silver se remplit par `indusense pipeline` ou `indusense silver cast <batch_id>`.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-27
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0006'
down_revision: str | Sequence[str] | None = '0005'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS silver")
    op.create_table('_cast_run',
    sa.Column('run_id', sa.Uuid(), nullable=False, comment='Exécution silver (UUID)'),
    sa.Column('batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze typée"),
    sa.Column('contract', sa.Text(), nullable=False),
    sa.Column('target_table', sa.Text(), nullable=False),
    sa.Column('status', sa.Text(), nullable=False, comment='RUNNING, SUCCESS, FAILED ou ROLLED_BACK'),
    sa.Column('rows_read', sa.BigInteger(), nullable=True),
    sa.Column('rows_inserted', sa.BigInteger(), nullable=True),
    sa.Column('cast_errors', sa.BigInteger(), nullable=True, comment='Nombre de valeurs non typables'),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('run_id', 'contract', name=op.f('pk__cast_run')),
    schema='silver',
    comment='Registre des exécutions silver : quel batch bronze a été typé, avec quel résultat'
    )
    op.create_table('machine',
    sa.Column('machine_code', sa.String(length=16), nullable=False, comment="'machine_code' typé en VARCHAR(16)"),
    sa.Column('commissioning_date', sa.Date(), nullable=False, comment="'commissioning_date' typé en DATE"),
    sa.Column('max_daily_capacity', sa.Integer(), nullable=False, comment="'max_daily_capacity' typé en INTEGER"),
    sa.Column('max_hourly_capacity_pieces', sa.Integer(), nullable=False, comment="'max_hourly_capacity_pieces' typé en INTEGER"),
    sa.Column('model', sa.String(length=32), nullable=False, comment="'model' typé en VARCHAR(32)"),
    sa.Column('production_line', sa.String(length=16), nullable=False, comment="'production_line' typé en VARCHAR(16)"),
    sa.Column('location', sa.String(length=16), nullable=False, comment="'location' typé en VARCHAR(16)"),
    sa.Column('criticality', sa.String(length=8), nullable=False, comment="'criticality' typé en VARCHAR(8)"),
    sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze (UUID) dont vient la ligne (voir bronze._ingestion_batch)"),
    sa.Column('_bronze_id', sa.BigInteger(), nullable=False, comment="Ligne d'origine : bronze.machine._id"),
    sa.Column('_cast_at', sa.DateTime(timezone=True), nullable=False, comment='Instant UTC du typage (exécution silver)'),
    sa.ForeignKeyConstraint(['_bronze_id'], ['bronze.machine._id'], ),
    sa.PrimaryKeyConstraint('_bronze_id', name=op.f('pk_machine')),
    schema='silver',
    comment='Silver : bronze.machine typé selon le contrat'
    )
    op.create_index('ix_silver_machine_batch_id', 'machine', ['_batch_id'], unique=False, schema='silver')
    op.create_table('maintenance',
    sa.Column('maintenance_id', sa.Integer(), nullable=False, comment="'maintenance_id' typé en INTEGER"),
    sa.Column('machine_code', sa.String(length=16), nullable=False, comment="'machine_code' typé en VARCHAR(16)"),
    sa.Column('maintenance_at', sa.DateTime(timezone=True), nullable=False, comment="Début de l'intervention, horodatage avec fuseau (+00)"),
    sa.Column('maintenance_type', sa.String(length=16), nullable=False, comment="'maintenance_type' typé en VARCHAR(16)"),
    sa.Column('action_type', sa.String(length=32), nullable=False, comment="'action_type' typé en VARCHAR(32)"),
    sa.Column('component', sa.String(length=64), nullable=False, comment="'component' typé en VARCHAR(64)"),
    sa.Column('description', sa.Text(), nullable=False, comment="'description' typé en TEXT"),
    sa.Column('related_incident_id', sa.String(length=16), nullable=True, comment="Incident à l'origine d'une maintenance corrective (NULL sinon)"),
    sa.Column('duration_hours', sa.Numeric(precision=6, scale=2), nullable=False, comment="'duration_hours' typé en NUMERIC(6,2)"),
    sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze (UUID) dont vient la ligne (voir bronze._ingestion_batch)"),
    sa.Column('_bronze_id', sa.BigInteger(), nullable=False, comment="Ligne d'origine : bronze.maintenance._id"),
    sa.Column('_cast_at', sa.DateTime(timezone=True), nullable=False, comment='Instant UTC du typage (exécution silver)'),
    sa.ForeignKeyConstraint(['_bronze_id'], ['bronze.maintenance._id'], ),
    sa.PrimaryKeyConstraint('_bronze_id', name=op.f('pk_maintenance')),
    schema='silver',
    comment='Silver : bronze.maintenance typé selon le contrat'
    )
    op.create_index('ix_silver_maintenance_batch_id', 'maintenance', ['_batch_id'], unique=False, schema='silver')
    op.create_table('releves_incidents',
    sa.Column('incident_id', sa.Text(), nullable=False, comment="'incident_id' typé en TEXT"),
    sa.Column('date', sa.Date(), nullable=False, comment='Date de déclaration (horloge UTC déclarée)'),
    sa.Column('time', sa.Time(), nullable=False, comment='Heure de déclaration HH:MM (horloge UTC déclarée)'),
    sa.Column('operator_name', sa.String(length=64), nullable=False, comment="Nom de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé)"),
    sa.Column('machine_id', sa.Text(), nullable=False, comment="'machine_id' typé en TEXT"),
    sa.Column('severity', sa.Integer(), nullable=False, comment='Gravité déclarée (1 à 5)'),
    sa.Column('operator_badge', sa.String(length=64), nullable=False, comment="Matricule de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé)"),
    sa.Column('comment', sa.Text(), nullable=False, comment='Texte libre : peut contenir des données personnelles'),
    sa.Column('shift', sa.Text(), nullable=False, comment="'shift' typé en TEXT"),
    sa.Column('type_surchauffe', sa.Boolean(), nullable=False, comment="'type_surchauffe' typé en BOOLEAN"),
    sa.Column('type_baisse_pression', sa.Boolean(), nullable=False, comment="'type_baisse_pression' typé en BOOLEAN"),
    sa.Column('type_vibration', sa.Boolean(), nullable=False, comment="'type_vibration' typé en BOOLEAN"),
    sa.Column('type_bruit_mecanique', sa.Boolean(), nullable=False, comment="'type_bruit_mecanique' typé en BOOLEAN"),
    sa.Column('type_surconsommation', sa.Boolean(), nullable=False, comment="'type_surconsommation' typé en BOOLEAN"),
    sa.Column('type_blocage_mecanique', sa.Boolean(), nullable=False, comment="'type_blocage_mecanique' typé en BOOLEAN"),
    sa.Column('type_alarme_capteur', sa.Boolean(), nullable=False, comment="'type_alarme_capteur' typé en BOOLEAN"),
    sa.Column('type_arret_urgence', sa.Boolean(), nullable=False, comment="'type_arret_urgence' typé en BOOLEAN"),
    sa.Column('type_defaut_qualite', sa.Boolean(), nullable=False, comment="'type_defaut_qualite' typé en BOOLEAN"),
    sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze (UUID) dont vient la ligne (voir bronze._ingestion_batch)"),
    sa.Column('_bronze_id', sa.BigInteger(), nullable=False, comment="Ligne d'origine : bronze.releves_incidents._id"),
    sa.Column('_cast_at', sa.DateTime(timezone=True), nullable=False, comment='Instant UTC du typage (exécution silver)'),
    sa.ForeignKeyConstraint(['_bronze_id'], ['bronze.releves_incidents._id'], ),
    sa.PrimaryKeyConstraint('_bronze_id', name=op.f('pk_releves_incidents')),
    schema='silver',
    comment='Silver : bronze.releves_incidents typé selon le contrat'
    )
    op.create_index('ix_silver_releves_incidents_batch_id', 'releves_incidents', ['_batch_id'], unique=False, schema='silver')
    op.create_table('telemetry',
    sa.Column('machine_id', sa.Text(), nullable=False, comment='Code machine (MACH-01...), référence machine.machine_code'),
    sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False, comment='Heure de la mesure, horloge UTC déclarée (ADR-003)'),
    sa.Column('temperature_c', sa.Double(), nullable=True, comment="'temperature_c' typé en DOUBLE PRECISION"),
    sa.Column('pressure_bar', sa.Double(), nullable=True, comment="'pressure_bar' typé en DOUBLE PRECISION"),
    sa.Column('voltage_mean_v', sa.Double(), nullable=False, comment="'voltage_mean_v' typé en DOUBLE PRECISION"),
    sa.Column('rotation_mean_rpm', sa.Double(), nullable=True, comment="'rotation_mean_rpm' typé en DOUBLE PRECISION"),
    sa.Column('pieces_produced', sa.Integer(), nullable=False, comment="Pièces produites sur l'heure"),
    sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Exécution d'ingestion bronze (UUID) dont vient la ligne (voir bronze._ingestion_batch)"),
    sa.Column('_bronze_id', sa.BigInteger(), nullable=False, comment="Ligne d'origine : bronze.telemetry._id"),
    sa.Column('_cast_at', sa.DateTime(timezone=True), nullable=False, comment='Instant UTC du typage (exécution silver)'),
    sa.ForeignKeyConstraint(['_bronze_id'], ['bronze.telemetry._id'], ),
    sa.PrimaryKeyConstraint('_bronze_id', name=op.f('pk_telemetry')),
    schema='silver',
    comment='Silver : bronze.telemetry typé selon le contrat'
    )
    op.create_index('ix_silver_telemetry_batch_id', 'telemetry', ['_batch_id'], unique=False, schema='silver')


def downgrade() -> None:
    op.drop_index('ix_silver_telemetry_batch_id', table_name='telemetry', schema='silver')
    op.drop_table('telemetry', schema='silver')
    op.drop_index('ix_silver_releves_incidents_batch_id', table_name='releves_incidents', schema='silver')
    op.drop_table('releves_incidents', schema='silver')
    op.drop_index('ix_silver_maintenance_batch_id', table_name='maintenance', schema='silver')
    op.drop_table('maintenance', schema='silver')
    op.drop_index('ix_silver_machine_batch_id', table_name='machine', schema='silver')
    op.drop_table('machine', schema='silver')
    op.drop_table('_cast_run', schema='silver')
    op.execute("DROP SCHEMA IF EXISTS silver")
