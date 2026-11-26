"""Ingestion bronze de bout en bout dans une base PostgreSQL dédiée (indusense_test), créée puis supprimée.

Ignorés si PostgreSQL n'est pas joignable (docker compose up -d db).
"""

import copy
import csv
import json

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from indusense.bronze.contract import load_contracts, source_path
from indusense.bronze.loader import ValidationFailed, load_contracts_into
from indusense.bronze.reports import write_reports
from indusense.bronze.schema import alembic_config
from indusense.settings import get_settings

pytestmark = pytest.mark.integration
TEST_DB = "indusense_test"
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
def first_load(engine):
    return {r.target_table.split(".")[1]: r for r in load_contracts_into(load_contracts(), engine)}


def scalar(engine, sql: str, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def test_row_counts(engine, first_load):
    assert {table: r.status for table, r in first_load.items()} == dict.fromkeys(EXPECTED_ROWS, "SUCCESS")
    assert {table: r.rows_inserted for table, r in first_load.items()} == EXPECTED_ROWS
    for table, expected in EXPECTED_ROWS.items():
        assert scalar(engine, f"SELECT count(*) FROM bronze.{table}") == expected


def test_raw_column_is_identical_to_the_source(engine, first_load):
    path = source_path(load_contracts()["telemetry.json"])
    with path.open(encoding="utf-8", newline="") as f:
        expected = [row["temperature_c"] for row in csv.DictReader(f)]
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT temperature_c_raw FROM bronze.telemetry ORDER BY _row_number")).scalars().all()
    assert stored == expected


def test_typed_null_means_blank_source(engine, first_load):
    for column in ("temperature_c", "pressure_bar", "voltage_mean_v", "rotation_mean_rpm", "pieces_produced"):
        mismatches = scalar(engine, f"SELECT count(*) FROM bronze.telemetry "
                                    f"WHERE ({column} IS NULL) <> (btrim({column}_raw) = '')")
        assert mismatches == 0, column
    assert scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE temperature_c IS NULL") > 0


def test_cast_errors_are_empty_on_the_reference_dataset(engine, first_load):
    for table in EXPECTED_ROWS:
        assert scalar(engine, f"SELECT count(*) FROM bronze.{table} WHERE _cast_errors <> '{{}}'::jsonb") == 0


def test_timestamps_are_labelled_utc(engine, first_load):
    first = scalar(engine, "SELECT timestamp FROM bronze.telemetry WHERE _row_number = 1")
    assert first.isoformat() == "2025-06-01T00:00:00+00:00"


def test_replaying_duplicates_nothing(engine, first_load):
    replay = load_contracts_into(load_contracts(), engine)
    assert all(r.status == "SUCCESS" and r.rows_inserted == 0 for r in replay)
    assert {r.target_table.split(".")[1]: r.rows_skipped for r in replay} == EXPECTED_ROWS
    for table, expected in EXPECTED_ROWS.items():
        assert scalar(engine, f"SELECT count(*) FROM bronze.{table}") == expected


def test_registry_records_every_batch(engine, first_load):
    assert scalar(engine, "SELECT count(*) FROM bronze._ingestion_batch WHERE status = 'RUNNING'") == 0
    assert scalar(engine, "SELECT count(DISTINCT contract) FROM bronze._ingestion_batch WHERE status = 'SUCCESS'") == 4


def test_cast_error_beyond_threshold_rolls_back(engine, first_load, tmp_path):
    source = tmp_path / "telemetry_bad.csv"
    source.write_text("machine_id,timestamp,temperature_c,pressure_bar,voltage_mean_v,rotation_mean_rpm,pieces_produced\n"
                      "MACH-99,2030-01-01 00:00:00,n/a,1,1,1,1\n"
                      "MACH-99,2030-01-01 01:00:00,40,1,1,1,1\n")
    contract = copy.deepcopy(load_contracts()["telemetry.json"])
    contract["source"]["path"] = str(source)
    [result] = load_contracts_into({"telemetry_bad.json": contract}, engine)
    assert (result.status, result.rows_read, result.cast_error_rows) == ("FAILED", 2, 1)
    assert "CastThresholdExceeded" in result.error
    assert scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE machine_id = 'MACH-99'") == 0
    assert scalar(engine, "SELECT status FROM bronze._ingestion_batch WHERE batch_id = :b",
                  b=result.batch_id) == "FAILED"


def test_cast_error_within_threshold_is_recorded_per_row(engine, first_load, tmp_path):
    source = tmp_path / "telemetry_tolerated.csv"
    source.write_text("machine_id,timestamp,temperature_c,pressure_bar,voltage_mean_v,rotation_mean_rpm,pieces_produced\n"
                      "MACH-98,2030-01-01 00:00:00,n/a,1,1,1,1\n")
    contract = copy.deepcopy(load_contracts()["telemetry.json"])
    contract["source"]["path"] = str(source)
    contract["max_cast_error_ratio"] = 1.0
    [result] = load_contracts_into({"telemetry_tolerated.json": contract}, engine)
    assert result.status == "SUCCESS"
    with engine.connect() as conn:
        raw, typed, errors = conn.execute(text(
            "SELECT temperature_c_raw, temperature_c, _cast_errors FROM bronze.telemetry WHERE machine_id = 'MACH-98'"
        )).one()
    assert (raw, typed, errors) == ("n/a", None, {"temperature_c": "not a number: 'n/a'"})


def test_invalid_contract_writes_nothing(engine, first_load):
    contract = copy.deepcopy(load_contracts()["telemetry.json"])
    contract["columns"][0]["source_name"] = "machin_id"
    before = scalar(engine, "SELECT count(*) FROM bronze._ingestion_batch")
    with pytest.raises(ValidationFailed, match="Did you mean 'machine_id'"):
        load_contracts_into({"telemetry.json": contract}, engine)
    assert scalar(engine, "SELECT count(*) FROM bronze._ingestion_batch") == before


def test_reports_carry_transformations(engine, first_load, tmp_path):
    contract = load_contracts()["releves_incidents.json"]
    report_path, dictionary_path = write_reports(contract, first_load["releves_incidents"], engine, tmp_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    dictionary = json.loads(dictionary_path.read_text(encoding="utf-8"))
    assert report["rows_inserted"] == 1245 and report["transformations"]
    merge = next(t for t in report["transformations"] if t["code"] == "MERGE")
    assert merge == {"code": "MERGE", "target": "date_time", "source": ["date", "time"], "layer": "silver",
                     "executed": False, "detail": "Concaténation 'date time', horloge UTC déclarée, aucune conversion"}
    severity = next(c for c in dictionary["columns"] if c["target_name"] == "severity")
    assert (severity["min"], severity["max"], severity["null_rate"], severity["cast_failures"]) == (2, 5, 0.0, 0)
    assert next(c for c in dictionary["columns"] if c["target_name"] == "operator_badge")["personal_data"] is True


def test_schema_matches_the_contracts(engine, first_load):
    """Pas de dérive : la migration appliquée correspond exactement aux contrats."""
    command.check(alembic_config(engine.url))
