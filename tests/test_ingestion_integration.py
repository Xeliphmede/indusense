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


def read_source_column(contract_name: str, column: str) -> list[str]:
    path = source_path(load_contracts()[contract_name])
    with path.open(encoding="utf-8-sig", newline="") as f:
        return [row[column] for row in csv.DictReader(f)]


def test_every_column_is_text(engine, first_load):
    with engine.connect() as conn:
        types = conn.execute(text(
            "SELECT DISTINCT data_type FROM information_schema.columns WHERE table_schema = 'bronze' "
            "AND table_name <> '_ingestion_batch' AND column_name NOT LIKE '\\_%'"
        )).scalars().all()
    assert types == ["text"]


@pytest.mark.parametrize(("contract", "column"), [("telemetry.json", "temperature_c"), ("telemetry.json", "timestamp"),
                                                  ("releves_incidents.json", "comment")])
def test_column_is_identical_to_the_source(engine, first_load, contract, column):
    table = load_contracts()[contract]["target"]["table"]
    with engine.connect() as conn:
        stored = conn.execute(text(f'SELECT "{column}" FROM bronze.{table} ORDER BY _row_number')).scalars().all()
    assert stored == read_source_column(contract, column)


def test_blank_values_are_kept_as_empty_text(engine, first_load):
    blanks = read_source_column("telemetry.json", "temperature_c").count("")
    assert blanks > 0
    assert scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE temperature_c = ''") == blanks
    assert scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE temperature_c IS NULL") == 0


def test_sql_null_stays_null(engine, first_load):
    assert scalar(engine, "SELECT count(*) FROM bronze.maintenance WHERE related_incident_id IS NULL") > 0
    assert scalar(engine, "SELECT duration_hours FROM bronze.maintenance WHERE maintenance_id = '1'") == "2.24"


def test_replaying_duplicates_nothing(engine, first_load):
    replay = load_contracts_into(load_contracts(), engine)
    assert all(r.status == "SUCCESS" and r.rows_inserted == 0 for r in replay)
    assert {r.target_table.split(".")[1]: r.rows_skipped for r in replay} == EXPECTED_ROWS
    for table, expected in EXPECTED_ROWS.items():
        assert scalar(engine, f"SELECT count(*) FROM bronze.{table}") == expected


def test_registry_records_every_batch(engine, first_load):
    assert scalar(engine, "SELECT count(*) FROM bronze._ingestion_batch WHERE status = 'RUNNING'") == 0
    assert scalar(engine, "SELECT count(DISTINCT contract) FROM bronze._ingestion_batch WHERE status = 'SUCCESS' "
                          "AND contract = ANY(:names)", names=list(load_contracts())) == 4


def test_malformed_row_rolls_back_the_whole_file(engine, first_load, tmp_path):
    source = tmp_path / "telemetry_bad.csv"
    source.write_text("machine_id,timestamp,temperature_c,pressure_bar,voltage_mean_v,rotation_mean_rpm,pieces_produced\n"
                      "MACH-99,2030-01-01 00:00:00,n/a,1,1,1,1\n"
                      "MACH-99,2030-01-01 01:00:00,40,1,1,1\n")
    contract = copy.deepcopy(load_contracts()["telemetry.json"])
    contract["source"]["path"] = str(source)
    [result] = load_contracts_into({"telemetry_bad.json": contract}, engine)
    assert result.status == "FAILED"
    assert "SourceFormatError" in result.error and "record 2" in result.error
    assert scalar(engine, "SELECT count(*) FROM bronze.telemetry WHERE machine_id = 'MACH-99'") == 0
    assert scalar(engine, "SELECT status FROM bronze._ingestion_batch WHERE batch_id = :b",
                  b=result.batch_id) == "FAILED"


def test_unconvertible_value_is_stored_as_is(engine, first_load, tmp_path):
    source = tmp_path / "telemetry_text.csv"
    source.write_text("machine_id,timestamp,temperature_c,pressure_bar,voltage_mean_v,rotation_mean_rpm,pieces_produced\n"
                      "MACH-98,2030-01-01 00:00:00,n/a,1,1,1,1\n")
    contract = copy.deepcopy(load_contracts()["telemetry.json"])
    contract["source"]["path"] = str(source)
    [result] = load_contracts_into({"telemetry_text.json": contract}, engine)
    assert result.status == "SUCCESS"
    assert scalar(engine, "SELECT temperature_c FROM bronze.telemetry WHERE machine_id = 'MACH-98'") == "n/a"


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
    assert (severity["bronze_type"], severity["silver_type"], severity["empty_rate"], severity["distinct_values"]) == (
        "TEXT", "INTEGER", 0.0, 4)
    assert {t["layer"] for t in report["transformations"] if t["code"] == "COPY"} == {"bronze"}
    assert all(not t["executed"] for t in report["transformations"] if t["layer"] == "silver")
    assert next(c for c in dictionary["columns"] if c["target_name"] == "operator_badge")["personal_data"] is True


def test_schema_matches_the_contracts(engine, first_load):
    """Pas de dérive : la migration appliquée correspond exactement aux contrats."""
    command.check(alembic_config(engine.url))


def test_migration_0002_keeps_loaded_data():
    """Une base chargée avec le schéma 0001 (colonnes _raw + typées) garde ses valeurs d'origine en 0002,
    et le retour en 0001 recalcule les colonnes typées."""
    admin = create_engine(get_settings().url(), isolation_level="AUTOCOMMIT")
    database = "indusense_test_migration"
    try:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            conn.execute(text(f"CREATE DATABASE {database}"))
    except OperationalError as error:
        pytest.skip(f"PostgreSQL injoignable : {error.orig}")
    url = get_settings().url(database=database)
    config = alembic_config(url)
    engine = create_engine(url)
    try:
        command.upgrade(config, "0001")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO bronze.telemetry (machine_id_raw, machine_id, timestamp_raw, timestamp, temperature_c_raw, "
                "temperature_c, pressure_bar_raw, voltage_mean_v_raw, rotation_mean_rpm_raw, pieces_produced_raw, "
                "pieces_produced, _batch_id, _source_file, _source_hash, _row_number) VALUES ('MACH-01', 'MACH-01', "
                "'2025-06-01 00:00:00', '2025-06-01 00:00:00+00', '', NULL, '198.2', '227.5', '1541.7', '4', 4, "
                "gen_random_uuid(), 'datas/telemetry.csv', 'abc', 1)"))
        command.upgrade(config, "head")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT machine_id, timestamp, temperature_c, pieces_produced "
                                    "FROM bronze.telemetry")).one()
        assert tuple(row) == ("MACH-01", "2025-06-01 00:00:00", "", "4")
        command.check(config)

        command.downgrade(config, "0001")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT timestamp_raw, timestamp, temperature_c_raw, temperature_c, pieces_produced "
                                    "FROM bronze.telemetry")).one()
        assert row.timestamp_raw == "2025-06-01 00:00:00" and row.timestamp.isoformat() == "2025-06-01T00:00:00+00:00"
        assert (row.temperature_c_raw, row.temperature_c, row.pieces_produced) == ("", None, 4)
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
        admin.dispose()
