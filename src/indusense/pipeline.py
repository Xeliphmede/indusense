"""Pipeline d'ingestion : fichiers -> bronze -> silver, sous un même batch_id.

1. le schéma de la base doit correspondre aux contrats (Alembic `check`) : le pipeline ne migre jamais lui-même ;
2. ingestion bronze (`load_contracts_into`) : un batch_id pour l'exécution ;
3. si le bronze a réussi, typage de ce batch en silver (`cast_batch`). Une erreur de cast annule le silver
   seulement : le batch bronze reste, on corrige le contrat puis `indusense silver cast <batch_id>`.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path

from alembic import command
from alembic.util import CommandError
from sqlalchemy import Engine, create_engine

from indusense.bronze.contract import load_contracts
from indusense.bronze.loader import LoadResult, load_contracts_into
from indusense.bronze.schema import alembic_config
from indusense.settings import get_settings
from indusense.silver.loader import CastResult, cast_batch
from indusense.silver.report import REPORTS_DIR


class SchemaOutOfDate(Exception):
    """La base ne correspond pas aux contrats : appliquer ou générer une migration avant de relancer."""


@dataclass
class PipelineResult:
    batch_id: uuid.UUID
    bronze: list[LoadResult]
    silver: list[CastResult] = field(default_factory=list)  # vide si le bronze a échoué

    @property
    def succeeded(self) -> bool:
        return bool(self.silver) and all(r.status == "SUCCESS" for r in self.bronze + self.silver)

    @property
    def cast_errors(self) -> list:
        return [error for result in self.silver for error in result.errors]


def check_schema(engine: Engine) -> None:
    try:
        command.check(alembic_config(engine.url))
    except CommandError as error:
        raise SchemaOutOfDate(f"{error} (run `uv run alembic upgrade head`, or generate a migration if a contract "
                              f"changed)") from error


def run_pipeline(engine: Engine | None = None, contracts: dict[str, dict] | None = None,
                 salt: str | None = None, report_dir: Path | None = REPORTS_DIR) -> PipelineResult:
    """Charge les fichiers en bronze, puis type ce batch en silver (rapport des traitements dans report_dir)."""
    engine = engine or create_engine(get_settings().url())
    contracts = contracts if contracts is not None else load_contracts()
    check_schema(engine)
    bronze = load_contracts_into(contracts, engine, salt=salt)
    result = PipelineResult(batch_id=bronze[0].batch_id, bronze=bronze)
    if all(r.status == "SUCCESS" for r in bronze):
        result.silver = cast_batch(contracts, engine, result.batch_id, report_dir=report_dir)
    return result
