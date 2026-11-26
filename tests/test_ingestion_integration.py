"""Ingestion bronze de bout en bout dans une base PostgreSQL dédiée (indusense_test), créée puis supprimée.

Ignorés si PostgreSQL n'est pas joignable (docker compose up -d db).
"""

import copy
import csv
import json
import uuid
import re
from contextlib import contextmanager

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from indusense.bronze.contract import load_contracts, source_path
from indusense.bronze.fingerprint import row_fingerprint
from indusense.bronze.loader import ValidationFailed, bronze_record, file_sha256, load_contracts_into
from indusense.bronze.readers import iter_rows
from indusense.bronze.pseudonymization import pseudonymize
from indusense.bronze.reports import write_reports
from indusense.bronze.schema import alembic_config
from indusense.settings import get_settings

pytestmark = pytest.mark.integration
TEST_DB = "indusense_test"
SALT = get_settings().anonymisation_salt
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


def test_no_operator_is_stored_in_clear(engine, first_load):
    names = set(read_source_column("releves_incidents.json", "operator_name"))
    badges = set(read_source_column("releves_incidents.json", "operator_badge"))
    with engine.connect() as conn:
        stored = set(conn.execute(text("SELECT operator_name FROM bronze.releves_incidents UNION "
                                       "SELECT operator_badge FROM bronze.releves_incidents")).scalars())
    assert not stored & (names | badges)
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in stored)


def test_pseudonyms_are_consistent_with_the_salt(engine, first_load):
    with engine.connect() as conn:
        name, badge = conn.execute(text("SELECT operator_name, operator_badge FROM bronze.releves_incidents "
                                        "WHERE incident_id = 'INC-000001'")).one()
    assert (name, badge) == (pseudonymize("Lucas Bernard", SALT), pseudonymize("OP1002", SALT))


def test_missing_salt_writes_nothing(engine, first_load, tmp_path):
    source = tmp_path / "incidents.csv"
    lines = source_path(load_contracts()["releves_incidents.json"]).read_text(encoding="utf-8-sig").splitlines()
    source.write_text("\n".join(lines[:3]).replace("INC-0000", "INC-9990") + "\n", encoding="utf-8")
    contract = copy.deepcopy(load_contracts()["releves_incidents.json"])
    contract["source"] |= {"path": str(source), "encoding": "utf-8"}
    [result] = load_contracts_into({"incidents_no_salt.json": contract}, engine, salt="change-me")
    assert result.status == "FAILED" and "MissingSalt" in result.error
    assert scalar(engine, "SELECT count(*) FROM bronze.releves_incidents WHERE incident_id LIKE 'INC-9990%'") == 0


def test_type_column_follows_each_source_column(engine, first_load):
    for contract in load_contracts().values():
        table = contract["target"]["table"]
        with engine.connect() as conn:
            columns = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema = "
                                        "'bronze' AND table_name = :t ORDER BY ordinal_position"), {"t": table}).scalars().all()
        expected = [name for e in contract["columns"] for name in (e["target_name"], f"{e['target_name']}_type")]
        assert columns[:len(expected) + 1] == expected + ["hash"], table
        for entry in contract["columns"]:
            types = scalar(engine, f'SELECT array_agg(DISTINCT "{entry["target_name"]}_type") FROM bronze.{table}')
            assert types == [entry["type"]], (table, entry["target_name"])


def test_hash_is_the_fingerprint_of_the_file_columns(engine, first_load):
    for name in ("releves_incidents.json", "maintenance.json"):
        contract = load_contracts()[name]
        columns = [e["target_name"] for e in contract["columns"]]
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT hash, {', '.join(f'\"{c}\"' for c in columns)} "
                                     f"FROM bronze.{contract['target']['table']}")).all()
        assert all(row[0] == row_fingerprint(row[1:]) for row in rows), name


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
    assert (severity["min"], severity["max"], severity["null_rate"], severity["cast_failures"]) == (2, 5, 0.0, 0)
    date = next(c for c in dictionary["columns"] if c["target_name"] == "date")
    assert (date["min"], date["max"]) == ("2025-06-01", "2026-06-08")
    operator = next(c for c in dictionary["columns"] if c["target_name"] == "operator_name")
    assert (operator["min"], operator["max"]) == (None, None)
    assert {t["layer"] for t in report["transformations"] if t["code"] == "COPY"} == {"bronze"}
    assert all(not t["executed"] for t in report["transformations"] if t["layer"] == "silver")
    assert next(c for c in dictionary["columns"] if c["target_name"] == "operator_badge")["personal_data"] is True


def test_dictionary_counts_values_silver_cannot_type(engine, first_load, tmp_path):
    _, dictionary_path = write_reports(load_contracts()["telemetry.json"], first_load["telemetry"], engine, tmp_path)
    dictionary = json.loads(dictionary_path.read_text(encoding="utf-8"))
    temperature = next(c for c in dictionary["columns"] if c["target_name"] == "temperature_c")
    blanks = read_source_column("telemetry.json", "temperature_c").count("")
    assert temperature["cast_failures"] == 0
    assert temperature["null_rate"] == temperature["empty_rate"] == blanks / 135626
    assert (temperature["min"], temperature["max"]) == (32.0, 80.0)


def test_schema_matches_the_contracts(engine, first_load):
    """Pas de dérive : la migration appliquée correspond exactement aux contrats."""
    command.check(alembic_config(engine.url))


@contextmanager
def scratch_database(database: str):
    """Base vide créée pour un test puis supprimée : (configuration Alembic, engine)."""
    admin = create_engine(get_settings().url(), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            conn.execute(text(f"CREATE DATABASE {database}"))
    except OperationalError as error:
        pytest.skip(f"PostgreSQL injoignable : {error.orig}")
    url = get_settings().url(database=database)
    engine = create_engine(url)
    try:
        yield alembic_config(url), engine
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
        admin.dispose()


def test_migration_0002_keeps_loaded_data():
    """Une base chargée avec le schéma 0001 (colonnes _raw + typées) garde ses valeurs d'origine en 0002,
    et le retour en 0001 recalcule les colonnes typées."""
    with scratch_database("indusense_test_migration") as (config, engine):
        command.upgrade(config, "0001")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO bronze.telemetry (machine_id_raw, machine_id, timestamp_raw, timestamp, temperature_c_raw, "
                "temperature_c, pressure_bar_raw, voltage_mean_v_raw, rotation_mean_rpm_raw, pieces_produced_raw, "
                "pieces_produced, _batch_id, _source_file, _source_hash, _row_number) VALUES ('MACH-01', 'MACH-01', "
                "'2025-06-01 00:00:00', '2025-06-01 00:00:00+00', '', NULL, '198.2', '227.5', '1541.7', '4', 4, "
                "gen_random_uuid(), 'datas/telemetry.csv', 'abc', 1)"))
        command.upgrade(config, "0002")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT machine_id, timestamp, temperature_c, pieces_produced "
                                    "FROM bronze.telemetry")).one()
        assert tuple(row) == ("MACH-01", "2025-06-01 00:00:00", "", "4")

        command.downgrade(config, "0001")
        with engine.connect() as conn:
            row = conn.execute(text("SELECT timestamp_raw, timestamp, temperature_c_raw, temperature_c, pieces_produced "
                                    "FROM bronze.telemetry")).one()
        assert row.timestamp_raw == "2025-06-01 00:00:00" and row.timestamp.isoformat() == "2025-06-01T00:00:00+00:00"
        assert (row.temperature_c_raw, row.temperature_c, row.pieces_produced) == ("", None, 4)


def insert_as_before(engine, contract: dict, salt: str | None = None) -> None:
    """Insère les lignes du fichier comme le faisait le chargement avant 0004 : colonnes du fichier (pseudonymisées
    si salt est donné) et colonnes techniques, sans <col>_type ni hash."""
    path = source_path(contract)
    table = contract["target"]["table"]
    names = [entry["target_name"] for entry in contract["columns"]]
    rows = []
    for number, values in iter_rows(contract, path):
        row = {}
        for entry in contract["columns"]:
            value = values[entry["source_name"]]
            row[entry["target_name"]] = pseudonymize(value, salt) if salt and entry.get("pseudonymize") else value
        rows.append(row | {"_batch_id": uuid.uuid4(), "_source_file": "datas/x", "_source_hash": file_sha256(path),
                           "_row_number": number})
    columns = names + ["_batch_id", "_source_file", "_source_hash", "_row_number"]
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO bronze.{table} ({', '.join(f'\"{c}\"' for c in columns)}) "
                          f"VALUES ({', '.join(f':{c}' for c in columns)})"), rows)


def test_migration_0003_pseudonymizes_loaded_operators():
    """Des incidents chargés en clair (schéma 0002) sont pseudonymisés par 0003, avec le même pseudonyme
    que le chargement : recharger le fichier ensuite ne crée ni doublon ni second pseudonyme."""
    with scratch_database("indusense_test_pseudonymization") as (config, engine):
        command.upgrade(config, "0002")
        contract = load_contracts()["releves_incidents.json"]
        insert_as_before(engine, contract)
        assert scalar(engine, "SELECT count(*) FROM bronze.releves_incidents WHERE operator_name = 'Lucas Bernard'") > 0

        command.upgrade(config, "head")
        command.check(config)
        assert scalar(engine, "SELECT count(*) FROM bronze.releves_incidents WHERE operator_name = :p",
                      p=pseudonymize("Lucas Bernard", SALT)) > 0
        assert scalar(engine, "SELECT count(*) FROM bronze.releves_incidents "
                              "WHERE operator_name !~ '^[0-9a-f]{64}$' OR operator_badge !~ '^[0-9a-f]{64}$'") == 0

        [replay] = load_contracts_into({"releves_incidents.json": contract}, engine)
        assert (replay.rows_inserted, replay.rows_skipped) == (0, 1245)
        assert scalar(engine, "SELECT count(DISTINCT operator_name) FROM bronze.releves_incidents") == len(
            {name.strip().casefold() for name in read_source_column("releves_incidents.json", "operator_name")})


def test_migration_0004_adds_types_and_the_same_hash_as_the_loader():
    """Les lignes chargées avant 0004 reçoivent <col>_type et hash, avec l'empreinte que calcule le chargement."""
    with scratch_database("indusense_test_type_hash") as (config, engine):
        command.upgrade(config, "0003")
        contract = load_contracts()["maintenance.json"]
        insert_as_before(engine, contract)

        command.upgrade(config, "head")
        command.check(config)
        expected = {number: bronze_record(contract, values)
                    for number, values in iter_rows(contract, source_path(contract))}
        with engine.connect() as conn:
            stored = conn.execute(text("SELECT * FROM bronze.maintenance ORDER BY _row_number")).mappings().all()
        assert len(stored) == 115
        for row in stored:
            assert {key: row[key] for key in expected[row["_row_number"]]} == expected[row["_row_number"]]

        command.downgrade(config, "0003")
        with engine.connect() as conn:
            columns = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema = "
                                        "'bronze' AND table_name = 'maintenance' ORDER BY ordinal_position")).scalars().all()
        assert columns == [e["target_name"] for e in contract["columns"]] + [
            "_batch_id", "_source_file", "_source_hash", "_row_number", "_ingested_at"]
