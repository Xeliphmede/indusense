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
DUPLICATES = {"machine": 0, "maintenance": 0, "releves_incidents": 0, "telemetry": 1346}
SILVER_ROWS = {table: rows - DUPLICATES[table] for table, rows in EXPECTED_ROWS.items()}


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
def reports(tmp_path_factory):
    return tmp_path_factory.mktemp("silver_reports")


@pytest.fixture(scope="module")
def pipeline(engine, reports):
    return run_pipeline(engine, report_dir=reports)


def scalar(engine, sql: str, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def test_pipeline_types_every_bronze_row(engine, pipeline):
    assert pipeline.succeeded and not pipeline.cast_errors
    assert {r.target_table: r.rows_inserted for r in pipeline.silver} == {
        f"silver.{table}": rows for table, rows in SILVER_ROWS.items()}
    assert {r.target_table: r.rows_deduplicated for r in pipeline.silver} == {
        f"silver.{table}": rows for table, rows in DUPLICATES.items()}
    for table, rows in SILVER_ROWS.items():
        assert scalar(engine, f"SELECT count(*) FROM silver.{table} WHERE _batch_id = :b", b=pipeline.batch_id) == rows


def test_each_silver_row_points_to_its_bronze_row(engine, pipeline):
    for table in EXPECTED_ROWS:
        assert scalar(engine, f"""
            SELECT count(*) FROM silver.{table} s JOIN bronze.{table} b ON b._id = s._bronze_id
            WHERE s._batch_id = b._batch_id AND s._batch_id = :b""", b=pipeline.batch_id) == SILVER_ROWS[table]


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
    empty = scalar(engine, "SELECT count(*) FROM bronze.telemetry b JOIN silver.telemetry s ON s._bronze_id = b._id "
                           "WHERE b._batch_id = :b AND b.temperature_c = ''", b=pipeline.batch_id)
    assert empty > 0
    assert scalar(engine, "SELECT count(*) FROM silver.telemetry WHERE _batch_id = :b AND temperature_c IS NULL",
                  b=pipeline.batch_id) == empty


def test_recasting_a_batch_replaces_its_rows(engine, pipeline):
    results = cast_batch(load_contracts(), engine, pipeline.batch_id, report_dir=None)
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
    result = run_pipeline(engine, contracts, report_dir=None)

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
    assert {r.status for r in cast_batch(load_contracts(), engine, result.batch_id, report_dir=None)} == {"SUCCESS"}
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=result.batch_id) == 15


def test_unknown_batch_is_refused(engine, pipeline):
    with pytest.raises(BatchNotReady, match="absent"):
        cast_batch(load_contracts(), engine, uuid.uuid4(), report_dir=None)


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
    assert checked["silver.telemetry"] == SILVER_ROWS["telemetry"] * 7 + DUPLICATES["telemetry"] * 2  # clé seule


def test_value_lost_after_cast_cancels_silver(engine, pipeline, monkeypatch):
    """Filet de sécurité : si le cast laissait passer une valeur altérée, la relecture la détecte avant commit."""
    import indusense.silver.loader as loader

    real = loader.cast_column

    def truncating(raw, entry):
        value, error = real(raw, entry)
        return (value[:4] if entry["target_name"] == "model" else value), error

    monkeypatch.setattr(loader, "cast_column", truncating)
    before = scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=pipeline.batch_id)
    results = cast_batch(load_contracts(), engine, pipeline.batch_id, report_dir=None)

    statuses = {r.contract: r.status for r in results}
    assert statuses.pop("machine.json") == "FAILED" and set(statuses.values()) == {"ROLLED_BACK"}
    machine = next(r for r in results if r.contract == "machine.json")
    assert len(machine.errors) == 15 and machine.errors[0].column == "model"
    assert "does not restore" in machine.errors[0].reason
    # la transaction est annulée : les lignes silver précédentes de ce batch sont intactes
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b", b=pipeline.batch_id) == before
    assert scalar(engine, "SELECT count(*) FROM silver.machine WHERE _batch_id = :b AND length(model) = 4",
                  b=pipeline.batch_id) == 0


def test_report_lists_every_column_treatment_with_batch_size(pipeline, reports):
    [path] = {r.report for r in pipeline.silver}
    assert path.parent.parent == reports and path.suffix == ".md"
    report = path.read_text(encoding="utf-8")
    assert f"`{pipeline.batch_id}`" in report and "| Taille des lots d'écriture (`batch_size`) | 5000 lignes |" in report
    assert "**SUCCESS**" in report
    for contract in load_contracts().values():
        for entry in contract["columns"]:
            assert f"### `{entry['target_name']}` : {entry['type']} -> {entry['silver_type']}" in report
    assert "| `silver.telemetry` | `telemetry.json` | SUCCESS | 135626 | 1346 | 134280 |" in report
    assert "### Doublons écartés (1346, 100 premiers)" in report
    assert "décimal exact NUMERIC(6,2) : au plus 2 décimale(s)" in report
    assert "`date_time` : MERGE de date, time" in report


def test_failed_run_also_writes_its_report(engine, pipeline, tmp_path):
    contracts = copy.deepcopy(load_contracts())
    next(c for c in contracts["machine.json"]["columns"] if c["target_name"] == "model")["silver_type"] = "VARCHAR(4)"
    results = cast_batch(contracts, engine, pipeline.batch_id, batch_size=100, report_dir=tmp_path)
    report = results[0].report.read_text(encoding="utf-8")
    assert "**FAILED**" in report and "Exécution annulée" in report and "100 lignes" in report
    assert "longer than VARCHAR(4): 'InduPress-X" in report


def test_telemetry_has_one_row_per_natural_key(engine, pipeline):
    assert scalar(engine, """SELECT count(*) FROM (SELECT 1 FROM silver.telemetry WHERE _batch_id = :b
                             GROUP BY machine_id, timestamp HAVING count(*) > 1) d""", b=pipeline.batch_id) == 0


def test_every_dropped_row_is_recorded_with_the_kept_one(engine, pipeline):
    assert scalar(engine, "SELECT count(*) FROM silver._duplicate WHERE batch_id = :b", b=pipeline.batch_id) == 1346
    # chaque ligne bronze est soit en silver, soit écartée au profit d'une ligne silver de même clé
    assert scalar(engine, """
        SELECT count(*) FROM bronze.telemetry b
        LEFT JOIN silver.telemetry s ON s._bronze_id = b._id
        LEFT JOIN silver._duplicate d ON d.target_table = 'silver.telemetry' AND d.bronze_id = b._id
        LEFT JOIN silver.telemetry k ON k._bronze_id = d.kept_bronze_id
        WHERE b._batch_id = :b AND s._bronze_id IS NULL
          AND (k._bronze_id IS NULL OR k.machine_id <> b.machine_id)""", b=pipeline.batch_id) == 0
    # la ligne gardée est la plus complète
    assert scalar(engine, "SELECT count(*) FROM silver._duplicate WHERE batch_id = :b "
                          "AND kept_missing_values > missing_values", b=pipeline.batch_id) == 0


def test_unique_constraint_on_natural_key(engine, pipeline):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError, match="uq_silver_telemetry_natural_key"), engine.begin() as conn:
        conn.execute(text("""INSERT INTO silver.telemetry (machine_id, timestamp, voltage_mean_v, pieces_produced,
                             _batch_id, _bronze_id, _cast_at)
                             SELECT machine_id, timestamp, voltage_mean_v, pieces_produced, _batch_id,
                                    (SELECT bronze_id FROM silver._duplicate WHERE batch_id = :b LIMIT 1), now()
                             FROM silver.telemetry WHERE _batch_id = :b LIMIT 1"""), {"b": pipeline.batch_id})


def test_data_dictionary(engine, pipeline, tmp_path):
    from indusense.silver.dictionary import write_dictionary

    text_ = write_dictionary(load_contracts(), engine, tmp_path / "dictionary.md", pipeline.batch_id).read_text(
        encoding="utf-8")
    assert f"Batch décrit : `{pipeline.batch_id}`" in text_
    assert ("| `temperature_c` | Température moyenne de la machine sur l'heure | DOUBLE PRECISION | °C | non |  | "
            "0.7 % | 32 | 80 |  |") in text_
    assert "- **Lignes** (batch décrit) : 134280, après 1346 doublon(s) écarté(s)" in text_
    assert "| `shift` |" in text_ and "matin (427), apres-midi (410), nuit (408)" in text_
    assert "- **Source** : `datas/machine.sql.txt` (table `machine`), via `bronze.machine`" in text_
