"""Dictionnaire de données : chaque colonne est décrite, chaque unité a un symbole et une signification."""

import json

from indusense.bronze.contract import SCHEMA_PATH, UNIT_LABELS
from indusense.silver.dictionary import UNIT_NAMES


def test_every_contract_column_is_described(contracts):
    missing = [f"{name}:{entry['target_name']}" for name, contract in contracts.items()
               for entry in contract["columns"] if not entry.get("description")]
    assert not missing, f"colonnes sans description : {missing}"


def test_measures_and_counts_have_a_unit(contracts):
    units = {entry["target_name"]: entry.get("unit") for contract in contracts.values()
             for entry in contract["columns"]}
    assert units["temperature_c"] == "CELSIUS" and units["rotation_mean_rpm"] == "RPM"
    assert units["pieces_produced"] == units["max_hourly_capacity_pieces"] == "PIECE_PER_HOUR"
    assert units["max_daily_capacity"] == "PIECE_PER_DAY"


def test_every_contract_unit_is_labelled():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    enum = schema["$defs"]["column"]["properties"]["unit"]["enum"]
    assert set(enum) == set(UNIT_LABELS) == set(UNIT_NAMES)
