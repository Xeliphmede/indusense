"""Pseudonymisation HMAC-SHA256 salée des données personnelles."""

import copy
import re

import pytest

from indusense.bronze.loader import bronze_record
from indusense.bronze.pseudonymization import MissingSalt, check_salt, pseudonymize
from indusense.bronze.validation import validate_contract

SALT = "test-salt"


def test_pseudonym_is_a_64_char_hex_digest():
    assert re.fullmatch(r"[0-9a-f]{64}", pseudonymize("Lucas Bernard", SALT))


def test_same_value_same_pseudonym_whatever_case_and_spaces():
    assert pseudonymize("Lucas Bernard", SALT) == pseudonymize("  lucas BERNARD ", SALT)


def test_different_values_or_salts_give_different_pseudonyms():
    assert pseudonymize("Lucas Bernard", SALT) != pseudonymize("Hugo Thomas", SALT)
    assert pseudonymize("Lucas Bernard", SALT) != pseudonymize("Lucas Bernard", "other-salt")


@pytest.mark.parametrize("value", [None, "", "   "])
def test_blank_and_null_are_kept(value):
    assert pseudonymize(value, SALT) == value


@pytest.mark.parametrize("salt", [None, "", "change-me", " change-me "])
def test_missing_or_placeholder_salt_is_refused(salt):
    with pytest.raises(MissingSalt):
        check_salt(salt)


def test_only_pseudonymized_columns_are_hashed(contracts):
    contract = contracts["releves_incidents.json"]
    values = {entry["source_name"]: f"v-{entry['source_name']}" for entry in contract["columns"]}
    values |= {"operator_name": "Lucas Bernard", "operator_badge": "OP1002"}
    record = bronze_record(contract, values, SALT)
    assert record["operator_name"] == pseudonymize("Lucas Bernard", SALT)
    assert record["operator_badge"] == pseudonymize("OP1002", SALT)
    assert record["incident_id"] == "v-incident_id" and record["comment"] == "v-comment"


def test_pseudonymize_requires_personal_data(contracts):
    contract = copy.deepcopy(contracts["releves_incidents.json"])
    contract["columns"][3]["personal_data"] = False
    assert [i.pointer for i in validate_contract("releves_incidents.json", contract)] == ["columns[3].personal_data"]


def test_pseudonymize_is_only_for_text_columns(contracts):
    contract = copy.deepcopy(contracts["releves_incidents.json"])
    contract["columns"][5] |= {"personal_data": True, "pseudonymize": True}  # severity : INTEGER
    assert [i.pointer for i in validate_contract("releves_incidents.json", contract)] == ["columns[5].type"]
