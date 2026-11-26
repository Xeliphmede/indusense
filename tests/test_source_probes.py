"""Description des sources réelles : en-tête CSV, dump SQL analysé par sqlglot."""

import pytest

from indusense.bronze.probes import SourceInspectionError, probe_csv, probe_sql


def test_telemetry_header_is_exact(data_dir):
    schema = probe_csv(data_dir / "telemetry.csv", "utf-8", ",")
    assert schema.provided == ["machine_id", "timestamp", "temperature_c", "pressure_bar", "voltage_mean_v",
                               "rotation_mean_rpm", "pieces_produced"]


def test_incidents_header_is_exact(data_dir):
    schema = probe_csv(data_dir / "releves_incidents.csv", "utf-8-sig", ",")
    assert len(schema.provided) == 18
    assert schema.provided[:3] == ["incident_id", "date", "time"]
    assert schema.duplicates == []


def test_bom_is_removed_and_duplicates_detected(tmp_path):
    path = tmp_path / "bom.csv"
    path.write_bytes("﻿a,b,a\n1,2,3\n".encode())
    schema = probe_csv(path, "utf-8", ",")
    assert schema.declared[0] == "a"
    assert schema.duplicates == ["a"]


def test_multi_row_insert_is_fully_read(data_dir):
    machine = probe_sql(data_dir / "machine.sql.txt", "utf-8", "machine")
    maintenance = probe_sql(data_dir / "machine.sql.txt", "utf-8", "maintenance")
    assert (machine.rows, maintenance.rows) == (15, 115)


def test_ddl_only_columns_are_declared_but_not_provided(data_dir):
    schema = probe_sql(data_dir / "machine.sql.txt", "utf-8", "machine")
    assert {"is_active", "created_at", "updated_at"} <= set(schema.declared)
    assert not {"is_active", "created_at", "updated_at"} & set(schema.provided)


def test_insert_values_containing_separators_are_parsed(tmp_path):
    path = tmp_path / "dump.sql"
    path.write_text("CREATE TABLE t (a INT, b TEXT);\n"
                    "INSERT INTO t (a, b) VALUES (1, 'x, (y)'), (2, 'z);');\n")
    assert probe_sql(path, "utf-8", "t").rows == 2


def test_unknown_table_is_an_inspection_error(data_dir):
    with pytest.raises(SourceInspectionError, match="'ghost'"):
        probe_sql(data_dir / "machine.sql.txt", "utf-8", "ghost")


def test_missing_file_is_an_inspection_error(tmp_path):
    with pytest.raises(SourceInspectionError, match="file not found"):
        probe_csv(tmp_path / "absent.csv", "utf-8", ",")
