"""Du profil JSON au contrat bronze."""

import pytest

from indusense import profile_file
from indusense.bronze.contract import (ContractError, draft_contract, load_contract, normalize_sql_type,
                                       write_contract)
from indusense.bronze.validation import check_schema


def test_draft_from_csv_profile_is_schema_valid(data_dir):
    contract = draft_contract(profile_file(data_dir / "telemetry.csv"), "telemetry", {"natural_key": ["machine_id"]})
    assert check_schema("telemetry.json", contract) == []
    assert contract["source"] == {"path": "datas/telemetry.csv", "format": "CSV", "encoding": "utf-8",
                                  "field_delimiter": ",", "row_delimiter": "\n", "has_header": True}
    units = {c["target_name"]: c.get("unit") for c in contract["columns"] if c["type"] == "NUMBER"}
    assert units == {"temperature_c": "CELSIUS", "pressure_bar": "BAR", "voltage_mean_v": "VOLT",
                     "rotation_mean_rpm": "RPM"}


def test_draft_from_sql_profile_keeps_only_provided_columns(data_dir):
    contract = draft_contract(profile_file(data_dir / "machine.sql.txt"), "machine", {"natural_key": ["machine_code"]})
    names = [c["source_name"] for c in contract["columns"]]
    assert "is_active" not in names and "created_at" not in names
    assert contract["source"]["table"] == "machine"
    assert check_schema("machine.json", contract) == []


def test_draft_takes_silver_type_from_the_ddl(data_dir):
    contract = draft_contract(profile_file(data_dir / "machine.sql.txt"), "maintenance", {"natural_key": ["maintenance_id"]})
    silver = {c["target_name"]: c["silver_type"] for c in contract["columns"]}
    assert silver["duration_hours"] == "NUMERIC(6,2)"
    assert silver["maintenance_id"] == "INTEGER"


def test_decisions_on_unknown_columns_are_rejected(data_dir):
    with pytest.raises(KeyError, match="ghost"):
        draft_contract(profile_file(data_dir / "telemetry.csv"), "telemetry", {"columns": {"ghost": {}}})


@pytest.mark.parametrize(("declared", "expected"), [("INT", "INTEGER"), ("DECIMAL(6, 2)", "NUMERIC(6,2)"),
                                                    ("VARCHAR(16)", "VARCHAR(16)")])
def test_normalize_sql_type(declared, expected):
    assert normalize_sql_type(declared) == expected


def test_write_then_load_roundtrip(contracts, tmp_path):
    contract = contracts["machine.json"]
    assert load_contract(write_contract(contract, tmp_path)) == contract


def test_syntax_error_is_located(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text('{\n  "version": "2.0.0",\n  "kind" "BRONZE"\n}\n')
    with pytest.raises(ContractError, match=r"broken.json: line 3, column 10"):
        load_contract(path)
