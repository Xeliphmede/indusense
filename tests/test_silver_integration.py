"""Pipeline bronze -> silver de bout en bout dans une base PostgreSQL dédiée (indusense_test_silver), créée puis
supprimée.

Ignorés si PostgreSQL n'est pas joignable (docker compose up -d db).
"""

import copy
import uuid
from datetime import date, datetime, time
from decimal import Decimal

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from indusense.bronze.contract import load_contracts
from indusense.bronze.schema import alembic_config
from indusense.pipeline import run_pipeline
from indusense.settings import get_settings
from indusense.silver.loader import BatchNotReady, cast_batch, cast_records

pytestmark = pytest.mark.integration
TEST_DB = "indusense_test_silver"
EXPECTED_ROWS = {"machine": 15, "maintenance": 115, "releves_incidents": 1245, "telemetry": 135626}


@pytest.fixture(scope="module")
def engine():
    admin = create_engine(get_settings().url(), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)"))
            conn.execute(text(f"CREATE DATABASE {TEST_DB}"))
    except OperationalError as error:
        pytest.skip(f"PostgreSQL injoignable : {error.orig}")
    url = get_settings().url(database=TEST_DB)
    command.upgrade(alembic_config(url), "head")
    engine = create_engine(url)
    yield engine
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)"))
    admin.dispose()


@pytest.fixture(scope="module")
def pipeline(engine):
    return run_pipeline(engine)


def scalar(engine, sql: str, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def test_pipeline_types_every_bronze_row(engine, pipeline):
    assert pipeline.succeeded and not pipeline.cast_errors
    assert {r.target_table: r.rows_inserted for r in pipeline.silver} == {
        f"silver.{table}": rows for table, rows in EXPECTED_ROWS.items()}
    for table, rows in EXPECTED_ROWS.items():
        assert scalar(engine, f"SELECT count(*) FROM silver.{table} WHERE _batch_id = :b", b=pipeline.batch_id) == rows


def test_each_silver_row_points_to_its_bronze_row(engine, pipeline):
    for table in EXPECTED_ROWS:
        assert scalar(engine, f"""
            SELECT count(*) FROM silver.{table} s JOIN bronze.{table} b ON b._id = s._bronze_id
            WHERE s._batch_id = b._batch_id AND s._batch_id = :b""", b=pipeline.batch_id) == EXPECTED_ROWS[table]


def test_values_are_typed(engine, pipeline):
    with engine.connect() as conn:
        telemetry = conn.execute(text("SELECT * FROM silver.telemetry ORDER BY _bronze_id LIMIT 1")).mappings().one()
        maintenance = conn.execute(text("SELECT * FROM silver.maintenance ORDER BY _bronze_id LIMIT 1")).mappings().one()
        incident = conn.execute(text("SELECT * FROM silver.releves_incidents ORDER BY _bronze_id LIMIT 1")).mappings().one()
    assert isinstance(telemetry["timestamp"], datetime) and telemetry["timestamp"].utcoffset().total_seconds() == 0
    assert isinstance(telemetry["pieces_produced"], int)
    assert isinstance(maintenance["maintenance_id"], int) and isinstance(maintenance["duration_hours"], Decimal)
    assert isinstance(incident["date"], date) and isinstance(incident["time"], time)
    assert isinstance(incident["type_surchauffe"], bool)


def test_empty_bronze_values_become_null(engine, pipeline):
    empty = scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE _batch_id = :b AND temperature_c = ''",
                   b=pipeline.batch_id)
    assert empty > 0
    assert scalar(engine, "SELECT count(*) FROM silver.telemetry WHERE _batch_id = :b AND temperature_c IS NULL",
                  b=pipeline.batch_id) == empty


def test_recasting_a_batch_replaces_its_rows(engine, pipeline):
    results = cast_batch(load_contracts(), engine, pipeline.batch_id)
    assert {r.status for r in results} == {"SUCCESS"}
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=pipeline.batch_id) == 15
    assert scalar(engine, "SELECT count(DISTINCT run_id) FROM silver._cast_run WHERE batch_id = :b",
                  b=pipeline.batch_id) == 2


def test_cast_error_cancels_silver_but_keeps_bronze(engine, pipeline):
    contracts = copy.deepcopy(load_contracts())
    for column in contracts["machine.json"]["columns"]:
        if column["target_name"] == "model":
            column["silver_type"] = "VARCHAR(4)"  # 'InduPress-X1' ne tient plus
    before = scalar(engine, "SELECT count(*) FROM silver.machine")
    result = run_pipeline(engine, contracts)

    assert not result.succeeded
    assert {r.status for r in result.bronze} == {"SUCCESS"}
    statuses = {r.contract: r.status for r in result.silver}
    assert statuses.pop("machine.json") == "FAILED" and set(statuses.values()) == {"ROLLED_BACK"}
    assert len(result.cast_errors) == 15
    assert result.cast_errors[0].column == "model" and "longer than VARCHAR(4)" in result.cast_errors[0].reason
    for table in EXPECTED_ROWS:  # rien en silver pour ce batch, le bronze est là
        assert scalar(engine, f"SELECT count(*) FROM silver.{table} WHERE _batch_id = :b", b=result.batch_id) == 0
        assert scalar(engine, f"SELECT count(*) FROM bronze.{table} WHERE _batch_id = :b",
                      b=result.batch_id) == EXPECTED_ROWS[table]
    assert scalar(engine, "SELECT count(*) FROM silver.machine") == before
    assert scalar(engine, "SELECT cast_errors FROM silver._cast_run WHERE batch_id = :b AND contract = 'machine.json'",
                  b=result.batch_id) == 15

    # contrat corrigé : on retype le batch bronze conservé, sans recharger les fichiers
    assert {r.status for r in cast_batch(load_contracts(), engine, result.batch_id)} == {"SUCCESS"}
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=result.batch_id) == 15


def test_unknown_batch_is_refused(engine, pipeline):
    with pytest.raises(BatchNotReady, match="absent"):
        cast_batch(load_contracts(), engine, uuid.uuid4())


def test_cast_records_collects_every_error():
    contract = load_contracts()["telemetry.json"]
    rows = [{"_id": 1, "_row_number": 1, "_batch_id": None, "machine_id": "MACH-01", "timestamp": "hier",
             "temperature_c": "n/a", "pressure_bar": "", "voltage_mean_v": "", "rotation_mean_rpm": "1500",
             "pieces_produced": "12"}]
    records, errors = cast_records(contract, rows)
    assert [(e.column, e.reason) for e in errors] == [
        ("timestamp", "not a timestamp (YYYY-MM-DD HH:MM[:SS][+HH]): 'hier'"),
        ("temperature_c", "not a number: 'n/a'"),
        ("voltage_mean_v", "missing value (column is not nullable)"),
    ]
    assert records[0]["rotation_mean_rpm"] == 1500.0 and records[0]["pressure_bar"] is None


def test_every_silver_value_is_checked_against_bronze(pipeline):
    checked = {r.target_table: r.values_checked for r in pipeline.silver}
    assert checked["silver.telemetry"] == EXPECTED_ROWS["telemetry"] * 7


def test_value_lost_after_cast_cancels_silver(engine, pipeline, monkeypatch):
    """Filet de sécurité : si le cast laissait passer une valeur altérée, la relecture la détecte avant commit."""
    import indusense.silver.loader as loader

    real = loader.cast_column

    def truncating(raw, entry):
        value, error = real(raw, entry)
        return (value[:4] if entry["target_name"] == "model" else value), error

    monkeypatch.setattr(loader, "cast_column", truncating)
    before = scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=pipeline.batch_id)
    results = cast_batch(load_contracts(), engine, pipeline.batch_id)

    statuses = {r.contract: r.status for r in results}
    assert statuses.pop("machine.json") == "FAILED" and set(statuses.values()) == {"ROLLED_BACK"}
    machine = next(r for r in results if r.contract == "machine.json")
    assert len(machine.errors) == 15 and machine.errors[0].column == "model"
    assert "does not restore" in machine.errors[0].reason
    # la transaction est annulée : les lignes silver précédentes de ce batch sont intactes
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=pipeline.batch_id) == before
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b AND length(model) = 4",
                  b=pipeline.batch_id) == 0
