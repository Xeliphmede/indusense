"""Chargement des contrats dans leurs tables bronze, par exécution (batch).

- Tous les contrats sont validés avant toute lecture ou écriture : un seul problème, et rien n'est écrit.
- Une exécution = un `batch_id` (UUID), commun à toutes les tables, et une heure `_ingested_at`. Chaque exécution
  insère toutes les lignes des fichiers (le bronze est un historique en ajout seul, clé primaire `_id`
  auto-générée) : `WHERE _batch_id = ...` retrouve, dans chaque table, les lignes de cette ingestion, et le
  registre `_ingestion_batch` dit quels fichiers (chemin et SHA-256) elle a utilisés.
- Lecture en flux, écriture par lots de BATCH_SIZE lignes.
- Chaque valeur est copiée telle quelle, en texte : aucune conversion (le typage est fait en silver). Seules
  les colonnes `pseudonymize` du contrat sont remplacées par leur pseudonyme avant écriture : la valeur
  d'origine n'entre jamais en base.
- Une seule transaction pour toute l'exécution : au moindre échec (ligne mal formée, fichier illisible, sel
  absent...), aucune table ne garde ce batch (un batch présent dans une partie des tables serait trompeur).
- Le registre est écrit dans des transactions séparées : un échec y reste consigné.
"""

import hashlib
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import Engine, Table, insert, update

from indusense.bronze.contract import project_path, source_path
from indusense.bronze.fingerprint import row_fingerprint
from indusense.bronze.pseudonymization import check_salt, pseudonymize
from indusense.bronze.readers import iter_rows
from indusense.bronze.schema import HASH_COLUMN, REGISTRY_TABLE, build_metadata, type_column
from indusense.bronze.validation import ValidationIssue, validate_contracts
from indusense.settings import get_settings

BATCH_SIZE = 5_000


class ValidationFailed(Exception):
    """Au moins un contrat est invalide : rien n'a été lu ni écrit."""

    def __init__(self, issues: list[ValidationIssue]):
        self.issues = issues
        super().__init__(f"{len(issues)} validation issue(s):\n" + "\n".join(map(str, issues)))


@dataclass
class LoadResult:
    contract: str
    target_table: str
    batch_id: uuid.UUID  # exécution d'ingestion, commune à toutes les tables
    status: str         # RUNNING, SUCCESS, FAILED (ce contrat a échoué), ROLLED_BACK (un autre contrat a échoué)
    source_file: str
    source_hash: str
    rows_read: int = 0
    rows_inserted: int = 0
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def duration_s(self) -> float:
        return (self.completed_at - self.started_at).total_seconds() if self.completed_at else 0.0

    def as_dict(self) -> dict:
        return asdict(self) | {"duration_s": self.duration_s}


class ContractLoadError(Exception):
    """Échec du chargement d'un contrat : toute l'exécution est annulée."""

    def __init__(self, result: LoadResult, error: Exception):
        self.result = result
        super().__init__(f"{type(error).__name__}: {error}")


def file_sha256(path: Path, chunk_size: int = 1 << 20) -> str:
    """SHA-256 des octets du fichier, lu en flux."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def bronze_record(contract: dict, values: dict, salt: str | None = None) -> dict:
    """Colonnes bronze d'une ligne : <col> (texte d'origine, pseudonyme pour les colonnes `pseudonymize`),
    <col>_type (type déclaré par le contrat) et hash (empreinte des seules colonnes du fichier)."""
    record, stored = {}, []
    for entry in contract["columns"]:
        value = values[entry["source_name"]]
        if entry.get("pseudonymize"):
            value = pseudonymize(value, salt)
        record[entry["target_name"]] = value
        record[type_column(entry["target_name"])] = entry["type"]
        stored.append(value)
    record[HASH_COLUMN] = row_fingerprint(stored)
    return record


def pseudonymizes(contract: dict) -> bool:
    return any(entry.get("pseudonymize") for entry in contract["columns"])


def load_rows(conn, contract: dict, table: Table, result: LoadResult, salt: str | None, batch_size: int,
              ingested_at: datetime) -> None:
    """Insère toutes les lignes du fichier d'un contrat, marquées du batch_id et de l'heure de l'exécution."""
    path = source_path(contract)
    if pseudonymizes(contract):
        salt = check_salt(salt)
    technical = {"_batch_id": result.batch_id, "_source_file": result.source_file, "_source_hash": result.source_hash,
                 "_ingested_at": ingested_at}
    chunk = []
    for number, values in iter_rows(contract, path):
        result.rows_read += 1
        chunk.append(bronze_record(contract, values, salt) | technical | {"_row_number": number})
        if len(chunk) >= batch_size:
            conn.execute(insert(table), chunk)
            result.rows_inserted += len(chunk)
            chunk = []
    if chunk:
        conn.execute(insert(table), chunk)
        result.rows_inserted += len(chunk)


def load_contracts_into(contracts: dict[str, dict], engine: Engine, batch_size: int = BATCH_SIZE,
                        salt: str | None = None) -> list[LoadResult]:
    """Valide tous les contrats, puis les charge tous sous un même batch_id, dans une seule transaction.
    Retourne un résultat par contrat, dans l'ordre des contrats, également consigné dans le registre. Un échec
    ne lève pas d'exception : l'exécution est annulée, le contrat fautif porte status FAILED et son motif, les
    autres ROLLED_BACK. Le sel de pseudonymisation vient de .env (ANONYMISATION_SALT) s'il n'est pas fourni."""
    issues = validate_contracts(contracts)
    if issues:
        raise ValidationFailed(issues)
    metadata = build_metadata(contracts)
    registry = metadata.tables[f"bronze.{REGISTRY_TABLE}"]
    salt = salt if salt is not None else get_settings().anonymisation_salt
    batch_id, ingested_at = uuid.uuid4(), datetime.now(UTC)

    results = []
    for name, contract in contracts.items():
        path = source_path(contract)
        results.append(LoadResult(contract=name, target_table=f"bronze.{contract['target']['table']}",
                                  batch_id=batch_id, status="RUNNING", source_file=project_path(path),
                                  source_hash=file_sha256(path), started_at=ingested_at))
    with engine.begin() as conn:  # registre : transaction séparée
        conn.execute(insert(registry), [
            {"batch_id": batch_id, "contract": r.contract, "target_table": r.target_table, "source_file": r.source_file,
             "source_hash": r.source_hash, "status": "RUNNING", "started_at": r.started_at} for r in results])

    try:
        with engine.begin() as conn:  # chargement : une transaction pour toute l'exécution
            for result, contract in zip(results, contracts.values()):
                result.started_at = datetime.now(UTC)
                try:
                    load_rows(conn, contract, metadata.tables[result.target_table], result, salt, batch_size,
                              ingested_at)
                except Exception as error:
                    raise ContractLoadError(result, error) from error
                result.completed_at = datetime.now(UTC)
        for result in results:
            result.status = "SUCCESS"
    except ContractLoadError as failure:
        for result in results:
            result.rows_inserted = 0
            if result is failure.result:
                result.status, result.error = "FAILED", str(failure)
            else:
                result.status, result.error = "ROLLED_BACK", f"batch rolled back: {failure.result.contract} failed"
    finally:
        now = datetime.now(UTC)
        with engine.begin() as conn:
            for result in results:
                result.completed_at = result.completed_at or now
                conn.execute(update(registry).where(registry.c.batch_id == batch_id,
                                                    registry.c.contract == result.contract).values(
                    status=result.status, rows_read=result.rows_read, rows_inserted=result.rows_inserted,
                    error=result.error, completed_at=result.completed_at))
    return results
