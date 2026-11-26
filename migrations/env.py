"""Environnement Alembic : compare la métadonnée construite à partir des contrats bronze à la base."""

from alembic import context
from sqlalchemy import create_engine

from indusense.bronze.contract import load_contracts
from indusense.bronze.schema import SCHEMA, build_metadata
from indusense.settings import get_settings

config = context.config
target_metadata = build_metadata(load_contracts())


def include_name(name, type_, parent_names) -> bool:
    """Alembic ne gère que le schéma bronze : le reste de la base ne le regarde pas."""
    if type_ == "schema":
        return name == SCHEMA
    return True


def url():
    return config.attributes.get("url") or get_settings().url()


def run_migrations_offline() -> None:
    context.configure(url=url().render_as_string(hide_password=False), target_metadata=target_metadata,
                      literal_binds=True, include_schemas=True, include_name=include_name)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, include_schemas=True,
                          include_name=include_name, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
