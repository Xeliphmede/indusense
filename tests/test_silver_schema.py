"""Schéma silver : colonnes typées selon silver_type, NOT NULL selon nullable, traçabilité vers le bronze."""

import pytest
from sqlalchemy import CHAR, BigInteger, Boolean, Date, DateTime, Double, Integer, Numeric, String, Text, Time, Uuid

from indusense.silver.schema import TECHNICAL_COLUMNS, build_metadata, sql_type


@pytest.mark.parametrize(("silver_type", "expected", "attributes"), [
    ("TEXT", Text, {}),
    ("VARCHAR(16)", String, {"length": 16}),
    ("CHARACTER VARYING(8)", String, {"length": 8}),
    ("CHAR(2)", CHAR, {"length": 2}),
    ("INTEGER", Integer, {}),
    ("BIGINT", BigInteger, {}),
    ("DOUBLE PRECISION", Double, {}),
    ("NUMERIC(6,2)", Numeric, {"precision": 6, "scale": 2}),
    ("BOOLEAN", Boolean, {}),
    ("DATE", Date, {}),
    ("TIME", Time, {}),
    ("TIMESTAMPTZ", DateTime, {"timezone": True}),
    ("UUID", Uuid, {}),
])
def test_sql_type(silver_type, expected, attributes):
    result = sql_type(silver_type)
    assert type(result) is expected
    assert {name: getattr(result, name) for name in attributes} == attributes


def test_unknown_silver_type_is_refused():
    with pytest.raises(ValueError, match="unsupported silver_type"):
        sql_type("GEOMETRY")


def test_silver_tables_follow_the_contracts(contracts):
    metadata = build_metadata(contracts)
    for contract in contracts.values():
        table = metadata.tables[f"silver.{contract['target']['table']}"]
        assert table.c.keys() == [c["target_name"] for c in contract["columns"]] + TECHNICAL_COLUMNS
        for entry in contract["columns"]:
            assert table.c[entry["target_name"]].nullable is entry.get("nullable", True)
        assert [c.name for c in table.primary_key] == ["_bronze_id"]
        [fk] = table.c._bronze_id.foreign_keys
        assert fk.column is metadata.tables[f"bronze.{contract['target']['table']}"].c._id
        assert table.c._batch_id.nullable is False


def test_silver_types(contracts):
    maintenance = build_metadata(contracts).tables["silver.maintenance"]
    assert type(maintenance.c.maintenance_id.type) is Integer
    assert (maintenance.c.duration_hours.type.precision, maintenance.c.duration_hours.type.scale) == (6, 2)
    assert maintenance.c.machine_code.type.length == 16
    assert "silver._cast_run" in build_metadata(contracts).tables
