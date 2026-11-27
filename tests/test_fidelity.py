"""Fidélité silver/bronze : une valeur silver doit restituer exactement la valeur bronze, chaque ligne doit exister."""

import uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pandas as pd
import pytest

from indusense.silver.fidelity import BRONZE_SUFFIX, restores, validate_fidelity


def entry(silver_type: str) -> dict:
    return {"target_name": "c", "type": "TEXT", "silver_type": silver_type}


@pytest.mark.parametrize(("raw", "value", "silver_type"), [
    ("MACH-01", "MACH-01", "VARCHAR(16)"),
    (" nuit ", " nuit ", "TEXT"),
    ("46.348", 46.348, "DOUBLE PRECISION"),
    ("12,5", 12.5, "DOUBLE PRECISION"),
    ("2.76", Decimal("2.76"), "NUMERIC(6,2)"),
    ("8.0", Decimal("8.00"), "NUMERIC(6,2)"),
    ("42", 42, "INTEGER"),
    ("1", True, "BOOLEAN"),
    ("2025-06-01", date(2025, 6, 1), "DATE"),
    ("05:42", time(5, 42), "TIME"),
    ("2025-06-01 00:00:00", datetime(2025, 6, 1, tzinfo=UTC), "TIMESTAMPTZ"),
    ("", None, "DOUBLE PRECISION"),
    (None, None, "TEXT"),
    ("6f1c5b8e-6a51-4c1a-9a8f-2d3f1e0a7b11", uuid.UUID("6f1c5b8e-6a51-4c1a-9a8f-2d3f1e0a7b11"), "UUID"),
])
def test_faithful_values(raw, value, silver_type):
    assert restores(raw, value, entry(silver_type))


@pytest.mark.parametrize(("raw", "value", "silver_type"), [
    ("InduPress-X1", "Indu", "VARCHAR(4)"),                                  # tronqué
    (" nuit ", "nuit", "TEXT"),                                              # espaces perdus
    ("0.12345678901234567891", 0.12345678901234568, "DOUBLE PRECISION"),     # précision insuffisante
    ("2.765", Decimal("2.77"), "NUMERIC(6,2)"),                              # arrondi
    ("46.348", 46.0, "DOUBLE PRECISION"),
    ("4", 5, "INTEGER"),
    ("0", True, "BOOLEAN"),
    ("2025-06-01 00:00:00", datetime(2025, 6, 1, 2, tzinfo=UTC), "TIMESTAMPTZ"),  # converti, pas étiqueté
    ("05:42:30", time(5, 42), "TIME"),
    ("46.348", None, "DOUBLE PRECISION"),                                    # valeur perdue
    ("", 0.0, "DOUBLE PRECISION"),                                           # valeur inventée
])
def test_lossy_values(raw, value, silver_type):
    assert not restores(raw, value, entry(silver_type))


CONTRACT = {"target": {"table": "telemetry", "natural_key": ["machine_id"]}, "columns": [
    {"target_name": "machine_id", "type": "IDENTIFIER", "silver_type": "VARCHAR(4)"},
    {"target_name": "temperature_c", "type": "NUMBER", "silver_type": "DOUBLE PRECISION"},
]}


def frame(*rows) -> pd.DataFrame:
    """Lignes (_id, _row_number, _bronze_id, raw machine_id, raw temperature, machine_id, temperature
    [, _kept_bronze_id, _kept_row]) : les deux dernières pour une ligne écartée comme doublon."""
    columns = ["_id", "_row_number", "_bronze_id", "machine_id" + BRONZE_SUFFIX, "temperature_c" + BRONZE_SUFFIX,
               "machine_id", "temperature_c", "_kept_bronze_id", "_kept_row"]
    return pd.DataFrame([row + (None,) * (len(columns) - len(row)) for row in rows], columns=columns, dtype=object)


def test_faithful_frame_has_no_error():
    assert validate_fidelity(CONTRACT, frame((1, 1, 1, "M-01", "46.348", "M-01", 46.348),
                                             (2, 2, 2, "M-02", "", "M-02", None))) == []


def test_duplicate_dropped_with_the_same_key_is_not_a_loss():
    # ligne 2 écartée : comparée à la ligne gardée (_id 1) sur la clé seulement, sa température peut différer
    assert validate_fidelity(CONTRACT, frame((1, 1, 1, "M-01", "46.348", "M-01", 46.348),
                                             (2, 2, None, "M-01", "46.332", "M-01", 46.348, 1, 1))) == []


def test_duplicate_errors():
    errors = validate_fidelity(CONTRACT, frame(
        (1, 1, 1, "M-01", "46.348", "M-01", 46.348),
        (2, 2, None, "M-02", "46.3", "M-01", 46.348, 1, 1),   # clé différente de la ligne gardée
        (3, 3, None, "M-03", "46.3", None, None, 9, None),    # ligne gardée absente du silver
    ))
    assert [(e.column, e.bronze_id) for e in errors] == [("_bronze_id", 3), ("machine_id", 2)]
    assert errors[0].reason == "lost in silver: recorded as duplicate of bronze _id 9, which is not in silver"
    assert errors[1].reason.startswith("dropped as duplicate of bronze _id 1, but its key machine_id 'M-02'")


def test_every_loss_is_reported_with_its_bronze_row():
    errors = validate_fidelity(CONTRACT, frame(
        (10, 1, 10, "MACH-01", "46.348", "MACH", 46.348),                              # tronqué
        (11, 2, 11, "M-02", "0.12345678901234567891", "M-02", 0.12345678901234568),  # précision
        (12, 3, None, "M-03", "40.1", None, None),                                    # ligne absente
    ))
    assert [(e.column, e.bronze_id, e.row_number) for e in errors] == [
        ("_bronze_id", 12, 3), ("machine_id", 10, 1), ("temperature_c", 11, 2)]
    assert errors[0].reason == "lost in silver: no silver row for this bronze row"
    assert errors[1].reason == "lost in silver: stored 'MACH' (VARCHAR(4)) does not restore 'MACH-01'"
