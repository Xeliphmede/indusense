"""bronze tout en texte : une seule colonne TEXT par colonne source, sans _raw ni _cast_errors.

Les données déjà chargées sont conservées : la colonne typée est supprimée et le miroir <col>_raw, qui porte
la valeur d'origine, est renommé <col>. Le typage est désormais l'affaire de la couche silver.

Les colonnes sont figées ici (et non lues dans les contrats) : une migration décrit l'état de la base à un
instant donné, elle ne doit pas changer quand les contrats évoluent.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "bronze"

# table -> [(colonne, type de la colonne typée supprimée)]
COLUMNS = {
    'machine': [
        ('machine_code', 'TEXT'),
        ('commissioning_date', 'DATE'),
        ('max_daily_capacity', 'BIGINT'),
        ('max_hourly_capacity_pieces', 'BIGINT'),
        ('model', 'TEXT'),
        ('production_line', 'TEXT'),
        ('location', 'TEXT'),
        ('criticality', 'TEXT'),
    ],
    'maintenance': [
        ('maintenance_id', 'TEXT'),
        ('machine_code', 'TEXT'),
        ('maintenance_at', 'TIMESTAMP WITH TIME ZONE'),
        ('maintenance_type', 'TEXT'),
        ('action_type', 'TEXT'),
        ('component', 'TEXT'),
        ('description', 'TEXT'),
        ('related_incident_id', 'TEXT'),
        ('duration_hours', 'DOUBLE PRECISION'),
    ],
    'releves_incidents': [
        ('incident_id', 'TEXT'),
        ('date', 'DATE'),
        ('time', 'TIME'),
        ('operator_name', 'TEXT'),
        ('machine_id', 'TEXT'),
        ('severity', 'BIGINT'),
        ('operator_badge', 'TEXT'),
        ('comment', 'TEXT'),
        ('shift', 'TEXT'),
        ('type_surchauffe', 'BOOLEAN'),
        ('type_baisse_pression', 'BOOLEAN'),
        ('type_vibration', 'BOOLEAN'),
        ('type_bruit_mecanique', 'BOOLEAN'),
        ('type_surconsommation', 'BOOLEAN'),
        ('type_blocage_mecanique', 'BOOLEAN'),
        ('type_alarme_capteur', 'BOOLEAN'),
        ('type_arret_urgence', 'BOOLEAN'),
        ('type_defaut_qualite', 'BOOLEAN'),
    ],
    'telemetry': [
        ('machine_id', 'TEXT'),
        ('timestamp', 'TIMESTAMP WITH TIME ZONE'),
        ('temperature_c', 'DOUBLE PRECISION'),
        ('pressure_bar', 'DOUBLE PRECISION'),
        ('voltage_mean_v', 'DOUBLE PRECISION'),
        ('rotation_mean_rpm', 'DOUBLE PRECISION'),
        ('pieces_produced', 'BIGINT'),
    ],
}

# table -> {colonne: description du contrat} : le commentaire de la colonne TEXT la reprend
DESCRIPTIONS = {
    'maintenance': {
        'maintenance_at': "Début de l'intervention, horodatage avec fuseau (+00)",
        'related_incident_id': "Incident à l'origine d'une maintenance corrective (NULL sinon)",
    },
    'releves_incidents': {
        'date': 'Date de déclaration (horloge UTC déclarée)',
        'time': 'Heure de déclaration HH:MM (horloge UTC déclarée)',
        'operator_name': "Nom de l'opérateur : donnée personnelle, supprimée en silver",
        'severity': 'Gravité déclarée (1 à 5)',
        'operator_badge': "Matricule de l'opérateur : donnée personnelle, pseudonymisé en silver",
        'comment': 'Texte libre : peut contenir des données personnelles',
    },
    'telemetry': {
        'machine_id': 'Code machine (MACH-01...), référence machine.machine_code',
        'timestamp': 'Heure de la mesure, horloge UTC déclarée (ADR-003)',
        'pieces_produced': "Pièces produites sur l'heure",
    },
}


def raw_comment(name: str) -> str:
    return f"Valeur d'origine de '{name}'"


def upgrade() -> None:
    for table, columns in COLUMNS.items():
        for name, _ in columns:
            op.drop_column(table, name, schema=SCHEMA)
            op.alter_column(table, f"{name}_raw", new_column_name=name, schema=SCHEMA)
            if name in DESCRIPTIONS.get(table, {}):
                op.alter_column(table, name, comment=f"{raw_comment(name)} : {DESCRIPTIONS[table][name]}",
                                existing_type=sa.Text(), schema=SCHEMA)
        op.drop_column(table, "_cast_errors", schema=SCHEMA)
    op.drop_column("_ingestion_batch", "cast_error_rows", schema=SCHEMA)
    op.drop_column("_ingestion_batch", "max_cast_error_ratio", schema=SCHEMA)


def typed_expression(name: str, sql_type: str) -> str:
    """Valeur typée recalculée depuis le texte (inverse de upgrade) ; une heure sans fuseau est lue en UTC."""
    value = f'NULLIF(btrim("{name}_raw"), \'\')'
    if sql_type == "TEXT":
        return value
    if sql_type == "TIMESTAMP WITH TIME ZONE":
        return f"CASE WHEN {value} ~ '([+-][0-9]{{2}}(:?[0-9]{{2}})?|Z)$' THEN {value}::timestamptz " \
               f"ELSE {value}::timestamp AT TIME ZONE 'UTC' END"
    return f"({value})::{sql_type}"


def downgrade() -> None:
    op.add_column("_ingestion_batch", sa.Column("max_cast_error_ratio", sa.Double(), nullable=True), schema=SCHEMA)
    op.add_column("_ingestion_batch", sa.Column("cast_error_rows", sa.BigInteger(), nullable=True), schema=SCHEMA)
    for table, columns in COLUMNS.items():
        op.add_column(table, sa.Column("_cast_errors", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"),
                                       nullable=False, comment="Erreurs de conversion par colonne ; {} si tout est bon"),
                      schema=SCHEMA)
        for name, sql_type in columns:
            op.alter_column(table, name, new_column_name=f"{name}_raw", comment=raw_comment(name),
                            existing_type=sa.Text(), schema=SCHEMA)
            op.execute(f'ALTER TABLE {SCHEMA}.{table} ADD COLUMN "{name}" {sql_type}')
            op.execute(f'UPDATE {SCHEMA}.{table} SET "{name}" = {typed_expression(name, sql_type)}')
            if name in DESCRIPTIONS.get(table, {}):
                op.alter_column(table, name, comment=DESCRIPTIONS[table][name], schema=SCHEMA)
