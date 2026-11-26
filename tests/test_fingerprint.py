"""Empreinte d'une ligne : SHA-256 du tableau JSON des valeurs des colonnes du fichier."""

import hashlib
import re

from indusense.bronze.fingerprint import row_fingerprint
from indusense.bronze.loader import bronze_record


def test_fingerprint_is_the_sha256_of_the_json_array():
    expected = hashlib.sha256('["MACH-01","46.348",null]'.encode()).hexdigest()
    assert row_fingerprint(["MACH-01", "46.348", None]) == expected


def test_fingerprint_is_unambiguous():
    assert row_fingerprint(["a,b"]) != row_fingerprint(["a", "b"])
    assert row_fingerprint([""]) != row_fingerprint([None])
    assert row_fingerprint(["a", "b"]) != row_fingerprint(["b", "a"])


def test_record_has_a_type_column_after_each_column_and_a_hash(contracts):
    contract = contracts["telemetry.json"]
    values = {"machine_id": "MACH-01", "timestamp": "2025-06-01 00:00:00", "temperature_c": "",
              "pressure_bar": "198.203", "voltage_mean_v": "227.568", "rotation_mean_rpm": "1541.787",
              "pieces_produced": "4"}
    record = bronze_record(contract, values)
    assert list(record) == ["machine_id", "machine_id_type", "timestamp", "timestamp_type", "temperature_c",
                            "temperature_c_type", "pressure_bar", "pressure_bar_type", "voltage_mean_v",
                            "voltage_mean_v_type", "rotation_mean_rpm", "rotation_mean_rpm_type", "pieces_produced",
                            "pieces_produced_type", "hash"]
    assert (record["temperature_c_type"], record["pieces_produced_type"]) == ("NUMBER", "INTEGER")
    assert record["hash"] == row_fingerprint(list(values.values()))


def test_hash_covers_stored_pseudonyms_not_clear_values(contracts):
    contract = contracts["releves_incidents.json"]
    values = {entry["source_name"]: "x" for entry in contract["columns"]} | {"operator_name": "Lucas Bernard"}
    record = bronze_record(contract, values, "test-salt")
    stored = [record[entry["target_name"]] for entry in contract["columns"]]
    assert re.fullmatch(r"[0-9a-f]{64}", record["operator_name"])
    assert record["hash"] == row_fingerprint(stored)
    assert record["hash"] != row_fingerprint(list(values.values()))
