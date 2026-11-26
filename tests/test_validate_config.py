"""Les 10 règles de validation des contrats (docs/01-INGESTION_PROCESS.md, section 3)."""

import copy

from indusense.bronze.contract import CONFIG_DIR
from indusense.bronze.validation import validate_contract, validate_contracts, validate_directory


def messages(issues) -> list[str]:
    return [issue.message for issue in issues]


def test_repository_configs_are_valid():
    assert validate_directory(CONFIG_DIR) == []


def test_rule1_non_utc_timezone_is_rejected(contracts):
    contract = contracts["telemetry.json"] | {"timezone": "Europe/Paris"}
    [issue] = validate_contract("telemetry.json", contract)
    assert (issue.rule, issue.pointer) == (1, "timezone")
    assert "'UTC' was expected" in issue.message


def test_v1_keys_are_rejected(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["columns"][0]["sql_type"] = "TEXT"
    issues = validate_contract("telemetry.json", contract)
    assert [i.pointer for i in issues] == ["columns[0]"]
    assert "'sql_type' was unexpected" in issues[0].message


def test_rule1_number_requires_a_unit(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    del contract["columns"][2]["unit"]
    assert [i.pointer for i in validate_contract("telemetry.json", contract)] == ["columns[2]"]


def test_rule2_duplicate_target_name(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["columns"][1]["target_name"] = "machine_id"
    assert "duplicate target_name 'machine_id' (already used at columns[0].target_name)" in messages(
        validate_contract("telemetry.json", contract))


def test_rule3_operate_on_unknown_column(contracts):
    contract = copy.deepcopy(contracts["releves_incidents.json"])
    contract["derived_columns"][0]["operate_on_columns"] = ["date", "nowhere"]
    assert "'nowhere' is not a declared target_name in columns" in messages(
        validate_contract("releves_incidents.json", contract))


def test_rule4_raw_mirror_collision(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["columns"][1]["target_name"] = "machine_id_raw"
    assert "physical column name 'machine_id_raw' collides with columns[1].target_name" in messages(
        validate_contract("telemetry.json", contract))


def test_rule5_natural_key_references_declared_columns(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["target"]["natural_key"] = ["ghost"]
    [issue] = validate_contract("telemetry.json", contract)
    assert (issue.pointer, issue.message) == ("target.natural_key[0]", "'ghost' is not a declared target_name")


def test_rule6_type_and_silver_type_are_compatible(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["columns"][1]["silver_type"] = "TIMESTAMP"
    assert "'TIMESTAMP' is not compatible with type 'TIMESTAMPTZ'" in messages(
        validate_contract("telemetry.json", contract))


def test_unknown_column_names_column_and_file(contracts):
    contract = copy.deepcopy(contracts["maintenance.json"])
    contract["columns"][-1]["source_name"] = "duration_hour"
    assert ("column 'duration_hour' does not exist in machine.sql.txt:maintenance. Did you mean 'duration_hours'?"
            in messages(validate_contract("maintenance.json", contract)))


def test_ddl_only_column_is_rejected_with_an_actionable_message(contracts):
    contract = copy.deepcopy(contracts["machine.json"])
    contract["columns"].append({"source_name": "is_active", "target_name": "is_active", "type": "BINARY",
                                "silver_type": "BOOLEAN"})
    assert ("column 'is_active' exists in the DDL of machine.sql.txt:machine but is never provided by the INSERT"
            in messages(validate_contract("machine.json", contract)))


def test_rule9_sql_table_without_insert(contracts, tmp_path):
    dump = tmp_path / "ghost.sql"
    dump.write_text("CREATE TABLE ghost (id INT);\n")
    contract = copy.deepcopy(contracts["machine.json"])
    contract["source"] |= {"path": str(dump), "table": "ghost"}
    contract["columns"] = [{"source_name": "id", "target_name": "id", "type": "IDENTIFIER", "silver_type": "INTEGER"}]
    contract["target"]["natural_key"] = ["id"]
    issues = validate_contract("ghost.json", contract)
    assert "ghost.sql:ghost has no INSERT: no data to ingest" in messages(issues)


def test_two_configs_cannot_write_the_same_table(contracts):
    duplicate = copy.deepcopy(contracts["telemetry.json"])
    issues = validate_contracts({"telemetry.json": contracts["telemetry.json"], "copy.json": duplicate})
    assert messages(issues) == ["bronze.telemetry is already produced by telemetry.json"]


def test_all_issues_are_reported_not_only_the_first(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["columns"][0]["source_name"] = "machin_id"
    contract["columns"][1]["silver_type"] = "TIMESTAMP"
    contract["target"]["natural_key"] = ["ghost"]
    assert {i.rule for i in validate_contract("telemetry.json", contract)} == {5, 6, 7}


def test_unreadable_source_is_an_issue_not_an_exception(contracts):
    contract = copy.deepcopy(contracts["telemetry.json"])
    contract["source"]["path"] = "datas/absent.csv"
    [issue] = validate_contract("telemetry.json", contract)
    assert issue.pointer == "source" and "file not found" in issue.message
