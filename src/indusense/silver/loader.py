"""Typage d'une exécution d'ingestion bronze (batch) dans les tables silver.

- Le batch doit exister et avoir réussi pour chaque contrat (registre `bronze._ingestion_batch`).
- Une exécution silver = un `run_id` (UUID), consigné dans le registre `silver._cast_run`.
- Chaque valeur bronze (texte) est convertie par `cast_column` vers le type du contrat ; **toutes** les lignes de
  **toutes** les tables sont typées avant la moindre écriture, et toutes les erreurs sont collectées.
- Au moins une erreur de cast : chaque erreur est loguée (logger `indusense.silver`), rien n'est écrit en silver,
  le contrat fautif est FAILED, les autres ROLLED_BACK. Le batch bronze n'est pas touché : on corrige le contrat
  puis on retype ce batch, sans recharger les fichiers.
- Sinon, une seule transaction pour toutes les tables : les lignes silver déjà issues de ce batch sont remplacées
  (retyper un batch ne crée pas de doublon), puis insertion par lots de BATCH_SIZE lignes.
- Chaque ligne silver garde `_batch_id` et `_bronze_id` (= `bronze.<table>._id`) : son origine exacte.
"""

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from sqlalchemy import Engine, delete, insert, select, update

from indusense.bronze.schema import REGISTRY_TABLE as BRONZE_REGISTRY
from indusense.bronze.schema import SCHEMA as BRONZE_SCHEMA
from indusense.silver.casting import cast_column
from indusense.silver.schema import REGISTRY_TABLE, SCHEMA, build_metadata, table_name

BATCH_SIZE = 5_000
MAX_LOGGED_ERRORS = 50  # au-delà, un résumé : toutes les erreurs restent dans le résultat

logger = logging.getLogger("indusense.silver")


class BatchNotReady(Exception):
    """Le batch bronze est inconnu ou n'a pas réussi pour tous les contrats : rien n'est typé."""


@dataclass(frozen=True)
class CastError:
    table: str
    column: str
    bronze_id: int
    row_number: int
    value: str | None
    reason: str

    def __str__(self) -> str:
        return f"{self.table}.{self.column}, ligne {self.row_number} (_bronze_id {self.bronze_id}) : {self.reason}"


@dataclass
class CastResult:
    contract: str
    target_table: str
    run_id: uuid.UUID
    batch_id: uuid.UUID
    status: str  # RUNNING, SUCCESS, FAILED (erreurs de cast dans ce contrat), ROLLED_BACK (un autre contrat a échoué)
    rows_read: int = 0
    rows_inserted: int = 0
    errors: list[CastError] = field(default_factory=list)
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def duration_s(self) -> float:
        return (self.completed_at - self.started_at).total_seconds() if self.completed_at else 0.0

    def as_dict(self) -> dict:
        values = asdict(self)
        del values["errors"]
        return values | {"cast_errors": len(self.errors), "duration_s": self.duration_s}


def cast_records(contract: dict, rows) -> tuple[list[dict], list[CastError]]:
    """Type des lignes bronze (mappings avec `_id`, `_row_number`, `_batch_id` et les colonnes du contrat).
    Retourne les enregistrements silver et toutes les erreurs ; n'accède pas à la base."""
    table = f"{BRONZE_SCHEMA}.{table_name(contract)}"
    records, errors = [], []
    for row in rows:
        record = {"_batch_id": row["_batch_id"], "_bronze_id": row["_id"]}
        for entry in contract["columns"]:
            name = entry["target_name"]
            value, reason = cast_column(row[name], entry)
            if reason:
                errors.append(CastError(table, name, row["_id"], row["_row_number"], row[name], reason))
            record[name] = value
        records.append(record)
    return records, errors


def log_errors(errors: list[CastError]) -> None:
    for error in errors[:MAX_LOGGED_ERRORS]:
        logger.error("cast : %s", error)
    if len(errors) > MAX_LOGGED_ERRORS:
        logger.error("cast : ... et %d autre(s) erreur(s)", len(errors) - MAX_LOGGED_ERRORS)


def check_batch(conn, metadata, contracts: dict[str, dict], batch_id: uuid.UUID) -> None:
    registry = metadata.tables[f"{BRONZE_SCHEMA}.{BRONZE_REGISTRY}"]
    statuses = dict(conn.execute(select(registry.c.contract, registry.c.status)
                                 .where(registry.c.batch_id == batch_id)).all())
    problems = [f"{name}: {statuses.get(name, 'absent')}" for name in contracts if statuses.get(name) != "SUCCESS"]
    if problems:
        raise BatchNotReady(f"bronze batch {batch_id} is not loaded for every contract ({', '.join(problems)})")


def cast_batch(contracts: dict[str, dict], engine: Engine, batch_id: uuid.UUID,
               batch_size: int = BATCH_SIZE) -> list[CastResult]:
    """Type le batch bronze `batch_id` dans les tables silver. Retourne un résultat par contrat, dans l'ordre des
    contrats, également consigné dans `silver._cast_run`. Une erreur de cast ne lève pas d'exception : rien n'est
    écrit, les erreurs sont loguées et portées par les résultats (FAILED / ROLLED_BACK)."""
    batch_id = uuid.UUID(str(batch_id))
    metadata = build_metadata(contracts)
    registry = metadata.tables[f"{SCHEMA}.{REGISTRY_TABLE}"]
    with engine.connect() as conn:
        check_batch(conn, metadata, contracts, batch_id)

    run_id, started_at = uuid.uuid4(), datetime.now(UTC)
    results = [CastResult(contract=name, target_table=f"{SCHEMA}.{table_name(contract)}", run_id=run_id,
                          batch_id=batch_id, status="RUNNING", started_at=started_at)
               for name, contract in contracts.items()]
    with engine.begin() as conn:  # registre : transaction séparée
        conn.execute(insert(registry), [
            {"run_id": run_id, "batch_id": batch_id, "contract": r.contract, "target_table": r.target_table,
             "status": "RUNNING", "started_at": started_at} for r in results])

    current = None  # contrat en cours d'écriture, désigné fautif si la base échoue
    try:
        records = {}
        with engine.connect() as conn:  # 1. tout typer, sans rien écrire
            for result, contract in zip(results, contracts.values()):
                source = metadata.tables[f"{BRONZE_SCHEMA}.{table_name(contract)}"]
                columns = ["_id", "_row_number", "_batch_id"] + [e["target_name"] for e in contract["columns"]]
                rows = conn.execute(select(*(source.c[c] for c in columns))
                                    .where(source.c._batch_id == batch_id).order_by(source.c._id)).mappings()
                records[result.contract], result.errors = cast_records(contract, rows)
                result.rows_read = len(records[result.contract])

        failed = [r for r in results if r.errors]
        if failed:  # 2a. au moins une erreur : rien n'est écrit
            for result in results:
                log_errors(result.errors)
                if result.errors:
                    result.status, result.error = "FAILED", f"{len(result.errors)} cast error(s), first: {result.errors[0]}"
                else:
                    result.status = "ROLLED_BACK"
                    result.error = f"silver run cancelled: cast errors in {', '.join(r.contract for r in failed)}"
            logger.error("silver annulé : %d erreur(s) de cast, rien n'a été écrit",
                         sum(len(r.errors) for r in results))
            return results

        cast_at = datetime.now(UTC)
        with engine.begin() as conn:  # 2b. écriture : une transaction pour toutes les tables
            for result, contract in zip(results, contracts.values()):
                current = result
                target = metadata.tables[result.target_table]
                conn.execute(delete(target).where(target.c._batch_id == batch_id))
                rows = records[result.contract]
                for start in range(0, len(rows), batch_size):
                    chunk = [row | {"_cast_at": cast_at} for row in rows[start:start + batch_size]]
                    conn.execute(insert(target), chunk)
                    result.rows_inserted += len(chunk)
                result.completed_at = datetime.now(UTC)
            current = None
        for result in results:
            result.status = "SUCCESS"
            logger.info("silver : %s, %d ligne(s) typée(s)", result.target_table, result.rows_inserted)
        return results
    except Exception as error:  # échec inattendu (base, lecture) : la transaction d'écriture est annulée
        culprit = current or results[0]
        for result in results:
            result.rows_inserted = 0
            if result is culprit:
                result.status, result.error = "FAILED", f"{type(error).__name__}: {error}"
            else:
                result.status, result.error = "ROLLED_BACK", f"silver run cancelled: {culprit.contract} failed"
        logger.exception("silver annulé : %s", culprit.error)
        return results
    finally:
        now = datetime.now(UTC)
        with engine.begin() as conn:
            for result in results:
                result.completed_at = result.completed_at or now
                conn.execute(update(registry).where(registry.c.run_id == run_id,
                                                    registry.c.contract == result.contract).values(
                    status=result.status, rows_read=result.rows_read, rows_inserted=result.rows_inserted,
                    cast_errors=len(result.errors), error=result.error, completed_at=result.completed_at))
