"""Chargement d'un contrat dans sa table bronze.

- Tous les contrats sont validés avant toute lecture ou écriture : un seul problème, et rien n'est écrit.
- Lecture en flux, écriture par lots de BATCH_SIZE lignes, `INSERT ... ON CONFLICT DO NOTHING` sur la clé
  primaire (_source_hash, _row_number) : rejouer un fichier déjà chargé n'insère rien.
- Une transaction par contrat : si la part de lignes en erreur de cast dépasse max_cast_error_ratio, tout est
  annulé (mieux vaut une table vide qu'une table à moitié chargée).
- Le registre des lots est écrit dans des transactions séparées : un échec y reste consigné.
"""

import hashlib
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import Engine, MetaData, insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from indusense.bronze.casting import cast_value
from indusense.bronze.contract import project_path, source_path
from indusense.bronze.readers import iter_rows
from indusense.bronze.schema import REGISTRY_TABLE, build_metadata, raw_name
from indusense.bronze.validation import ValidationIssue, validate_contracts

BATCH_SIZE = 5_000


class ValidationFailed(Exception):
    """Au moins un contrat est invalide : rien n'a été lu ni écrit."""

    def __init__(self, issues: list[ValidationIssue]):
        self.issues = issues
        super().__init__(f"{len(issues)} validation issue(s):\n" + "\n".join(map(str, issues)))


class CastThresholdExceeded(Exception):
    """Trop de lignes en erreur de cast : la transaction de chargement est annulée."""


@dataclass
class LoadResult:
    contract: str
    target_table: str
    batch_id: str
    status: str
    source_file: str
    source_hash: str
    rows_read: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    cast_error_rows: int = 0
    max_cast_error_ratio: float = 0.0
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def cast_error_ratio(self) -> float:
        return self.cast_error_rows / self.rows_read if self.rows_read else 0.0

    @property
    def duration_s(self) -> float:
        return (self.completed_at - self.started_at).total_seconds() if self.completed_at else 0.0

    def as_dict(self) -> dict:
        return asdict(self) | {"cast_error_ratio": self.cast_error_ratio, "duration_s": self.duration_s}


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """SHA-256 des octets du fichier, lu en flux."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def bronze_record(contract: dict, values: dict, row_errors: dict) -> tuple[dict, dict]:
    """Colonnes <col>_raw et <col> d'une ligne, et ses erreurs de cast {target_name: motif}."""
    record, errors = {}, dict(row_errors)
    for entry in contract["columns"]:
        name, raw = entry["target_name"], values[entry["source_name"]]
        value, error = cast_value(raw, entry["type"])
        record[raw_name(name)], record[name] = raw, value
        if error:
            errors[name] = error
    return record, errors


def load_one(name: str, contract: dict, engine: Engine, metadata: MetaData, batch_size: int = BATCH_SIZE) -> LoadResult:
    """Charge un contrat (supposé valide) ; retourne le résultat, également consigné dans le registre.
    Un échec de chargement (seuil de cast dépassé, fichier illisible...) ne lève pas d'exception : la
    transaction est annulée et le résultat porte status FAILED et le motif."""
    path = source_path(contract)
    table = metadata.tables[f"bronze.{contract['target']['table']}"]
    registry = metadata.tables[f"bronze.{REGISTRY_TABLE}"]
    result = LoadResult(
        contract=name,
        target_table=table.fullname,
        batch_id=str(uuid.uuid4()),
        status="RUNNING",
        source_file=project_path(path),
        source_hash=file_sha256(path),
        max_cast_error_ratio=contract.get("max_cast_error_ratio", 0.0),
        started_at=datetime.now(UTC),
    )
    batch_id = uuid.UUID(result.batch_id)
    with engine.begin() as conn:  # registre : transaction séparée
        conn.execute(insert(registry).values(
            batch_id=batch_id, contract=name, target_table=result.target_table, source_file=result.source_file,
            source_hash=result.source_hash, status="RUNNING", max_cast_error_ratio=result.max_cast_error_ratio,
            started_at=result.started_at,
        ))

    statement = pg_insert(table).on_conflict_do_nothing().returning(table.c._row_number)
    technical = {"_batch_id": batch_id, "_source_file": result.source_file, "_source_hash": result.source_hash,
                 "_ingested_at": result.started_at}

    def flush(conn, chunk: list[dict]) -> None:
        inserted = len(conn.execute(statement, chunk).all())
        result.rows_inserted += inserted
        result.rows_skipped += len(chunk) - inserted

    try:
        with engine.begin() as conn:  # chargement : une transaction pour tout le fichier
            chunk = []
            for number, values, row_errors in iter_rows(contract, path):
                record, errors = bronze_record(contract, values, row_errors)
                result.rows_read += 1
                result.cast_error_rows += bool(errors)
                chunk.append(record | technical | {"_row_number": number, "_cast_errors": errors})
                if len(chunk) >= batch_size:
                    flush(conn, chunk)
                    chunk = []
            if chunk:
                flush(conn, chunk)
            if result.cast_error_ratio > result.max_cast_error_ratio:
                raise CastThresholdExceeded(
                    f"{result.cast_error_rows}/{result.rows_read} rows with cast errors "
                    f"({result.cast_error_ratio:.2%} > max {result.max_cast_error_ratio:.2%}): load rolled back")
        result.status = "SUCCESS"
    except Exception as error:  # transaction annulée : rien n'a été écrit, le motif est consigné
        result.status, result.error = "FAILED", f"{type(error).__name__}: {error}"
        result.rows_inserted = result.rows_skipped = 0
    result.completed_at = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(update(registry).where(registry.c.batch_id == batch_id).values(
            status=result.status, rows_read=result.rows_read, rows_inserted=result.rows_inserted,
            rows_skipped=result.rows_skipped, cast_error_rows=result.cast_error_rows, error=result.error,
            completed_at=result.completed_at,
        ))
    return result


def load_contracts_into(contracts: dict[str, dict], engine: Engine, batch_size: int = BATCH_SIZE) -> list[LoadResult]:
    """Valide tous les contrats puis les charge un par un. Un contrat en échec n'empêche pas les suivants ;
    les résultats (y compris les échecs) sont retournés dans l'ordre des contrats."""
    issues = validate_contracts(contracts)
    if issues:
        raise ValidationFailed(issues)
    metadata = build_metadata(contracts)
    return [load_one(name, contract, engine, metadata, batch_size) for name, contract in contracts.items()]
