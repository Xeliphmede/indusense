"""colonne <col>_type après chaque colonne source, et colonne hash (empreinte de la ligne).

PostgreSQL ne sait ajouter une colonne qu'en fin de table : pour placer chaque <col>_type juste après sa colonne,
chaque table bronze est recréée dans le bon ordre, puis ses lignes y sont recopiées en calculant <col>_type
(type déclaré par le contrat) et hash (empreinte des colonnes du fichier, voir indusense.bronze.fingerprint) avec
la même fonction que le chargement : une ligne migrée et la même ligne rechargée ont la même empreinte.

Les définitions sont figées ici (et non lues dans les contrats) : une migration décrit l'état de la base à un
instant donné.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from indusense.bronze.fingerprint import row_fingerprint

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "bronze"
TECHNICAL = ["_batch_id", "_source_file", "_source_hash", "_row_number", "_ingested_at"]
BATCH_SIZE = 5_000

# table -> commentaire, colonnes du fichier (ordre du contrat), type déclaré par colonne, définition complète
TABLES = {
    'machine': {
        'comment': 'Bronze : datas/machine.sql.txt (table machine)',
        'source': ['machine_code', 'commissioning_date', 'max_daily_capacity', 'max_hourly_capacity_pieces', 'model', 'production_line', 'location', 'criticality'],
        'types': {'machine_code': 'IDENTIFIER', 'commissioning_date': 'DATE', 'max_daily_capacity': 'INTEGER', 'max_hourly_capacity_pieces': 'INTEGER', 'model': 'CATEGORY', 'production_line': 'CATEGORY', 'location': 'CATEGORY', 'criticality': 'CATEGORY'},
        'columns': lambda: [
            sa.Column('machine_code', sa.Text(), comment="Valeur d'origine de 'machine_code'"),
            sa.Column('machine_code_type', sa.Text(), nullable=False, comment="Type de 'machine_code' déclaré par le contrat"),
            sa.Column('commissioning_date', sa.Text(), comment="Valeur d'origine de 'commissioning_date'"),
            sa.Column('commissioning_date_type', sa.Text(), nullable=False, comment="Type de 'commissioning_date' déclaré par le contrat"),
            sa.Column('max_daily_capacity', sa.Text(), comment="Valeur d'origine de 'max_daily_capacity'"),
            sa.Column('max_daily_capacity_type', sa.Text(), nullable=False, comment="Type de 'max_daily_capacity' déclaré par le contrat"),
            sa.Column('max_hourly_capacity_pieces', sa.Text(), comment="Valeur d'origine de 'max_hourly_capacity_pieces'"),
            sa.Column('max_hourly_capacity_pieces_type', sa.Text(), nullable=False, comment="Type de 'max_hourly_capacity_pieces' déclaré par le contrat"),
            sa.Column('model', sa.Text(), comment="Valeur d'origine de 'model'"),
            sa.Column('model_type', sa.Text(), nullable=False, comment="Type de 'model' déclaré par le contrat"),
            sa.Column('production_line', sa.Text(), comment="Valeur d'origine de 'production_line'"),
            sa.Column('production_line_type', sa.Text(), nullable=False, comment="Type de 'production_line' déclaré par le contrat"),
            sa.Column('location', sa.Text(), comment="Valeur d'origine de 'location'"),
            sa.Column('location_type', sa.Text(), nullable=False, comment="Type de 'location' déclaré par le contrat"),
            sa.Column('criticality', sa.Text(), comment="Valeur d'origine de 'criticality'"),
            sa.Column('criticality_type', sa.Text(), nullable=False, comment="Type de 'criticality' déclaré par le contrat"),
            sa.Column('hash', sa.Text(), nullable=False, comment='SHA-256 des valeurs des colonnes du fichier pour cette ligne (colonnes ajoutées exclues)'),
            sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Lot d'ingestion"),
            sa.Column('_source_file', sa.Text(), nullable=False, comment='Fichier source, relatif à la racine du projet'),
            sa.Column('_source_hash', sa.Text(), nullable=False, comment='SHA-256 des octets du fichier source'),
            sa.Column('_row_number', sa.BigInteger(), nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
            sa.Column('_ingested_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        ],
    },
    'maintenance': {
        'comment': 'Bronze : datas/machine.sql.txt (table maintenance)',
        'source': ['maintenance_id', 'machine_code', 'maintenance_at', 'maintenance_type', 'action_type', 'component', 'description', 'related_incident_id', 'duration_hours'],
        'types': {'maintenance_id': 'IDENTIFIER', 'machine_code': 'IDENTIFIER', 'maintenance_at': 'TIMESTAMPTZ', 'maintenance_type': 'CATEGORY', 'action_type': 'CATEGORY', 'component': 'CATEGORY', 'description': 'TEXT', 'related_incident_id': 'IDENTIFIER', 'duration_hours': 'NUMBER'},
        'columns': lambda: [
            sa.Column('maintenance_id', sa.Text(), comment="Valeur d'origine de 'maintenance_id'"),
            sa.Column('maintenance_id_type', sa.Text(), nullable=False, comment="Type de 'maintenance_id' déclaré par le contrat"),
            sa.Column('machine_code', sa.Text(), comment="Valeur d'origine de 'machine_code'"),
            sa.Column('machine_code_type', sa.Text(), nullable=False, comment="Type de 'machine_code' déclaré par le contrat"),
            sa.Column('maintenance_at', sa.Text(), comment="Valeur d'origine de 'maintenance_at' : Début de l'intervention, horodatage avec fuseau (+00)"),
            sa.Column('maintenance_at_type', sa.Text(), nullable=False, comment="Type de 'maintenance_at' déclaré par le contrat"),
            sa.Column('maintenance_type', sa.Text(), comment="Valeur d'origine de 'maintenance_type'"),
            sa.Column('maintenance_type_type', sa.Text(), nullable=False, comment="Type de 'maintenance_type' déclaré par le contrat"),
            sa.Column('action_type', sa.Text(), comment="Valeur d'origine de 'action_type'"),
            sa.Column('action_type_type', sa.Text(), nullable=False, comment="Type de 'action_type' déclaré par le contrat"),
            sa.Column('component', sa.Text(), comment="Valeur d'origine de 'component'"),
            sa.Column('component_type', sa.Text(), nullable=False, comment="Type de 'component' déclaré par le contrat"),
            sa.Column('description', sa.Text(), comment="Valeur d'origine de 'description'"),
            sa.Column('description_type', sa.Text(), nullable=False, comment="Type de 'description' déclaré par le contrat"),
            sa.Column('related_incident_id', sa.Text(), comment="Valeur d'origine de 'related_incident_id' : Incident à l'origine d'une maintenance corrective (NULL sinon)"),
            sa.Column('related_incident_id_type', sa.Text(), nullable=False, comment="Type de 'related_incident_id' déclaré par le contrat"),
            sa.Column('duration_hours', sa.Text(), comment="Valeur d'origine de 'duration_hours'"),
            sa.Column('duration_hours_type', sa.Text(), nullable=False, comment="Type de 'duration_hours' déclaré par le contrat"),
            sa.Column('hash', sa.Text(), nullable=False, comment='SHA-256 des valeurs des colonnes du fichier pour cette ligne (colonnes ajoutées exclues)'),
            sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Lot d'ingestion"),
            sa.Column('_source_file', sa.Text(), nullable=False, comment='Fichier source, relatif à la racine du projet'),
            sa.Column('_source_hash', sa.Text(), nullable=False, comment='SHA-256 des octets du fichier source'),
            sa.Column('_row_number', sa.BigInteger(), nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
            sa.Column('_ingested_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        ],
    },
    'releves_incidents': {
        'comment': 'Bronze : datas/releves_incidents.csv',
        'source': ['incident_id', 'date', 'time', 'operator_name', 'machine_id', 'severity', 'operator_badge', 'comment', 'shift', 'type_surchauffe', 'type_baisse_pression', 'type_vibration', 'type_bruit_mecanique', 'type_surconsommation', 'type_blocage_mecanique', 'type_alarme_capteur', 'type_arret_urgence', 'type_defaut_qualite'],
        'types': {'incident_id': 'IDENTIFIER', 'date': 'DATE', 'time': 'TIME', 'operator_name': 'CATEGORY', 'machine_id': 'IDENTIFIER', 'severity': 'INTEGER', 'operator_badge': 'IDENTIFIER', 'comment': 'TEXT', 'shift': 'CATEGORY', 'type_surchauffe': 'BINARY', 'type_baisse_pression': 'BINARY', 'type_vibration': 'BINARY', 'type_bruit_mecanique': 'BINARY', 'type_surconsommation': 'BINARY', 'type_blocage_mecanique': 'BINARY', 'type_alarme_capteur': 'BINARY', 'type_arret_urgence': 'BINARY', 'type_defaut_qualite': 'BINARY'},
        'columns': lambda: [
            sa.Column('incident_id', sa.Text(), comment="Valeur d'origine de 'incident_id'"),
            sa.Column('incident_id_type', sa.Text(), nullable=False, comment="Type de 'incident_id' déclaré par le contrat"),
            sa.Column('date', sa.Text(), comment="Valeur d'origine de 'date' : Date de déclaration (horloge UTC déclarée)"),
            sa.Column('date_type', sa.Text(), nullable=False, comment="Type de 'date' déclaré par le contrat"),
            sa.Column('time', sa.Text(), comment="Valeur d'origine de 'time' : Heure de déclaration HH:MM (horloge UTC déclarée)"),
            sa.Column('time_type', sa.Text(), nullable=False, comment="Type de 'time' déclaré par le contrat"),
            sa.Column('operator_name', sa.Text(), comment="Pseudonyme HMAC-SHA256 salé de 'operator_name' : Nom de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé)"),
            sa.Column('operator_name_type', sa.Text(), nullable=False, comment="Type de 'operator_name' déclaré par le contrat"),
            sa.Column('machine_id', sa.Text(), comment="Valeur d'origine de 'machine_id'"),
            sa.Column('machine_id_type', sa.Text(), nullable=False, comment="Type de 'machine_id' déclaré par le contrat"),
            sa.Column('severity', sa.Text(), comment="Valeur d'origine de 'severity' : Gravité déclarée (1 à 5)"),
            sa.Column('severity_type', sa.Text(), nullable=False, comment="Type de 'severity' déclaré par le contrat"),
            sa.Column('operator_badge', sa.Text(), comment="Pseudonyme HMAC-SHA256 salé de 'operator_badge' : Matricule de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé)"),
            sa.Column('operator_badge_type', sa.Text(), nullable=False, comment="Type de 'operator_badge' déclaré par le contrat"),
            sa.Column('comment', sa.Text(), comment="Valeur d'origine de 'comment' : Texte libre : peut contenir des données personnelles"),
            sa.Column('comment_type', sa.Text(), nullable=False, comment="Type de 'comment' déclaré par le contrat"),
            sa.Column('shift', sa.Text(), comment="Valeur d'origine de 'shift'"),
            sa.Column('shift_type', sa.Text(), nullable=False, comment="Type de 'shift' déclaré par le contrat"),
            sa.Column('type_surchauffe', sa.Text(), comment="Valeur d'origine de 'type_surchauffe'"),
            sa.Column('type_surchauffe_type', sa.Text(), nullable=False, comment="Type de 'type_surchauffe' déclaré par le contrat"),
            sa.Column('type_baisse_pression', sa.Text(), comment="Valeur d'origine de 'type_baisse_pression'"),
            sa.Column('type_baisse_pression_type', sa.Text(), nullable=False, comment="Type de 'type_baisse_pression' déclaré par le contrat"),
            sa.Column('type_vibration', sa.Text(), comment="Valeur d'origine de 'type_vibration'"),
            sa.Column('type_vibration_type', sa.Text(), nullable=False, comment="Type de 'type_vibration' déclaré par le contrat"),
            sa.Column('type_bruit_mecanique', sa.Text(), comment="Valeur d'origine de 'type_bruit_mecanique'"),
            sa.Column('type_bruit_mecanique_type', sa.Text(), nullable=False, comment="Type de 'type_bruit_mecanique' déclaré par le contrat"),
            sa.Column('type_surconsommation', sa.Text(), comment="Valeur d'origine de 'type_surconsommation'"),
            sa.Column('type_surconsommation_type', sa.Text(), nullable=False, comment="Type de 'type_surconsommation' déclaré par le contrat"),
            sa.Column('type_blocage_mecanique', sa.Text(), comment="Valeur d'origine de 'type_blocage_mecanique'"),
            sa.Column('type_blocage_mecanique_type', sa.Text(), nullable=False, comment="Type de 'type_blocage_mecanique' déclaré par le contrat"),
            sa.Column('type_alarme_capteur', sa.Text(), comment="Valeur d'origine de 'type_alarme_capteur'"),
            sa.Column('type_alarme_capteur_type', sa.Text(), nullable=False, comment="Type de 'type_alarme_capteur' déclaré par le contrat"),
            sa.Column('type_arret_urgence', sa.Text(), comment="Valeur d'origine de 'type_arret_urgence'"),
            sa.Column('type_arret_urgence_type', sa.Text(), nullable=False, comment="Type de 'type_arret_urgence' déclaré par le contrat"),
            sa.Column('type_defaut_qualite', sa.Text(), comment="Valeur d'origine de 'type_defaut_qualite'"),
            sa.Column('type_defaut_qualite_type', sa.Text(), nullable=False, comment="Type de 'type_defaut_qualite' déclaré par le contrat"),
            sa.Column('hash', sa.Text(), nullable=False, comment='SHA-256 des valeurs des colonnes du fichier pour cette ligne (colonnes ajoutées exclues)'),
            sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Lot d'ingestion"),
            sa.Column('_source_file', sa.Text(), nullable=False, comment='Fichier source, relatif à la racine du projet'),
            sa.Column('_source_hash', sa.Text(), nullable=False, comment='SHA-256 des octets du fichier source'),
            sa.Column('_row_number', sa.BigInteger(), nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
            sa.Column('_ingested_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        ],
    },
    'telemetry': {
        'comment': 'Bronze : datas/telemetry.csv',
        'source': ['machine_id', 'timestamp', 'temperature_c', 'pressure_bar', 'voltage_mean_v', 'rotation_mean_rpm', 'pieces_produced'],
        'types': {'machine_id': 'IDENTIFIER', 'timestamp': 'TIMESTAMPTZ', 'temperature_c': 'NUMBER', 'pressure_bar': 'NUMBER', 'voltage_mean_v': 'NUMBER', 'rotation_mean_rpm': 'NUMBER', 'pieces_produced': 'INTEGER'},
        'columns': lambda: [
            sa.Column('machine_id', sa.Text(), comment="Valeur d'origine de 'machine_id' : Code machine (MACH-01...), référence machine.machine_code"),
            sa.Column('machine_id_type', sa.Text(), nullable=False, comment="Type de 'machine_id' déclaré par le contrat"),
            sa.Column('timestamp', sa.Text(), comment="Valeur d'origine de 'timestamp' : Heure de la mesure, horloge UTC déclarée (ADR-003)"),
            sa.Column('timestamp_type', sa.Text(), nullable=False, comment="Type de 'timestamp' déclaré par le contrat"),
            sa.Column('temperature_c', sa.Text(), comment="Valeur d'origine de 'temperature_c'"),
            sa.Column('temperature_c_type', sa.Text(), nullable=False, comment="Type de 'temperature_c' déclaré par le contrat"),
            sa.Column('pressure_bar', sa.Text(), comment="Valeur d'origine de 'pressure_bar'"),
            sa.Column('pressure_bar_type', sa.Text(), nullable=False, comment="Type de 'pressure_bar' déclaré par le contrat"),
            sa.Column('voltage_mean_v', sa.Text(), comment="Valeur d'origine de 'voltage_mean_v'"),
            sa.Column('voltage_mean_v_type', sa.Text(), nullable=False, comment="Type de 'voltage_mean_v' déclaré par le contrat"),
            sa.Column('rotation_mean_rpm', sa.Text(), comment="Valeur d'origine de 'rotation_mean_rpm'"),
            sa.Column('rotation_mean_rpm_type', sa.Text(), nullable=False, comment="Type de 'rotation_mean_rpm' déclaré par le contrat"),
            sa.Column('pieces_produced', sa.Text(), comment="Valeur d'origine de 'pieces_produced' : Pièces produites sur l'heure"),
            sa.Column('pieces_produced_type', sa.Text(), nullable=False, comment="Type de 'pieces_produced' déclaré par le contrat"),
            sa.Column('hash', sa.Text(), nullable=False, comment='SHA-256 des valeurs des colonnes du fichier pour cette ligne (colonnes ajoutées exclues)'),
            sa.Column('_batch_id', sa.Uuid(), nullable=False, comment="Lot d'ingestion"),
            sa.Column('_source_file', sa.Text(), nullable=False, comment='Fichier source, relatif à la racine du projet'),
            sa.Column('_source_hash', sa.Text(), nullable=False, comment='SHA-256 des octets du fichier source'),
            sa.Column('_row_number', sa.BigInteger(), nullable=False, comment="Numéro d'enregistrement dans la source (1 = première ligne de données)"),
            sa.Column('_ingested_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        ],
    },
}


def upgrade() -> None:
    connection = op.get_bind()
    for name, spec in TABLES.items():
        old = f"{name}_0003"
        op.rename_table(name, old, schema=SCHEMA)
        op.execute(f"ALTER TABLE {SCHEMA}.{old} RENAME CONSTRAINT pk_{name} TO pk_{old}")
        table = op.create_table(name, *spec["columns"](), sa.PrimaryKeyConstraint("_source_hash", "_row_number",
                                name=f"pk_{name}"), schema=SCHEMA, comment=spec["comment"])
        selected = ", ".join(f'"{column}"' for column in spec["source"] + TECHNICAL)
        rows = connection.execute(sa.text(f"SELECT {selected} FROM {SCHEMA}.{old}")).mappings()
        chunk = []
        for row in rows:
            record = dict(row)
            record |= {f"{column}_type": spec["types"][column] for column in spec["source"]}
            record["hash"] = row_fingerprint([row[column] for column in spec["source"]])
            chunk.append(record)
            if len(chunk) >= BATCH_SIZE:
                connection.execute(table.insert(), chunk)
                chunk = []
        if chunk:
            connection.execute(table.insert(), chunk)
        op.drop_table(old, schema=SCHEMA)


def downgrade() -> None:
    for name, spec in TABLES.items():
        op.drop_column(name, "hash", schema=SCHEMA)
        for column in spec["source"]:
            op.drop_column(name, f"{column}_type", schema=SCHEMA)
