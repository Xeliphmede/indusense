"""pseudonymisation de operator_name et operator_badge dans bronze.releves_incidents.

Les lignes déjà chargées en clair sont remplacées par leur pseudonyme HMAC-SHA256(ANONYMISATION_SALT, valeur),
calculé par la même fonction que le chargement : un opérateur a le même pseudonyme dans les lignes migrées et
dans les lignes chargées ensuite.

Irréversible : le downgrade rétablit les commentaires, pas les noms. Pour retrouver les valeurs d'origine, il
faut supprimer les lignes et recharger le fichier source avec un contrat sans `pseudonymize`.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from indusense.bronze.pseudonymization import check_salt, pseudonymize
from indusense.settings import get_settings

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "bronze"
TABLE = "releves_incidents"
# colonne -> (commentaire après pseudonymisation, commentaire d'avant)
COLUMNS = {
    "operator_name": (
        "Pseudonyme HMAC-SHA256 salé de 'operator_name' : Nom de l'opérateur, pseudonymisé à l'ingestion "
        "(HMAC-SHA256 salé)",
        "Valeur d'origine de 'operator_name' : Nom de l'opérateur : donnée personnelle, supprimée en silver",
    ),
    "operator_badge": (
        "Pseudonyme HMAC-SHA256 salé de 'operator_badge' : Matricule de l'opérateur, pseudonymisé à l'ingestion "
        "(HMAC-SHA256 salé)",
        "Valeur d'origine de 'operator_badge' : Matricule de l'opérateur : donnée personnelle, pseudonymisé en silver",
    ),
}
PSEUDONYM_PATTERN = "^[0-9a-f]{64}$"  # valeur déjà pseudonymisée : laissée telle quelle


def upgrade() -> None:
    salt = check_salt(get_settings().anonymisation_salt)
    connection = op.get_bind()
    for column, (comment, _) in COLUMNS.items():
        op.alter_column(TABLE, column, comment=comment, existing_type=sa.Text(), schema=SCHEMA)
        values = connection.execute(sa.text(
            f'SELECT DISTINCT "{column}" FROM {SCHEMA}.{TABLE} '
            f'WHERE "{column}" IS NOT NULL AND btrim("{column}") <> \'\' AND "{column}" !~ :pattern'
        ), {"pattern": PSEUDONYM_PATTERN}).scalars().all()
        for value in values:
            connection.execute(sa.text(f'UPDATE {SCHEMA}.{TABLE} SET "{column}" = :pseudonym WHERE "{column}" = :value'),
                               {"pseudonym": pseudonymize(value, salt), "value": value})


def downgrade() -> None:
    for column, (_, comment) in COLUMNS.items():
        op.alter_column(TABLE, column, comment=comment, existing_type=sa.Text(), schema=SCHEMA)
