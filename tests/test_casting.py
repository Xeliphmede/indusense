"""Conversion au mieux : une absence n'est pas une erreur, un échec garde son motif, l'heure est étiquetée UTC."""

from datetime import UTC, date, datetime, time, timedelta, timezone

import pytest

from indusense.silver.casting import cast_value


@pytest.mark.parametrize("col_type", ["NUMBER", "INTEGER", "BINARY", "DATE", "TIME", "TIMESTAMPTZ", "TEXT"])
@pytest.mark.parametrize("raw", [None, "", "  "])
def test_blank_is_null_without_error(raw, col_type):
    assert cast_value(raw, col_type) == (None, None)


@pytest.mark.parametrize(("raw", "col_type", "expected"), [
    ("46.348", "NUMBER", 46.348),
    ("12,5", "NUMBER", 12.5),
    ("4", "INTEGER", 4),
    ("1", "BINARY", True),
    ("0", "BINARY", False),
    ("2025-06-01", "DATE", date(2025, 6, 1)),
    ("05:42", "TIME", time(5, 42)),
    ("MACH-01", "IDENTIFIER", "MACH-01"),
    (" nuit ", "CATEGORY", " nuit "),
])
def test_valid_values_are_cast(raw, col_type, expected):
    assert cast_value(raw, col_type) == (expected, None)


@pytest.mark.parametrize(("raw", "col_type", "reason"), [
    ("n/a", "NUMBER", "not a number: 'n/a'"),
    ("nan", "NUMBER", "not a number: 'nan'"),
    ("4.5", "INTEGER", "not an integer: '4.5'"),
    ("2", "BINARY", "not a boolean: '2'"),
    ("31/12/2025", "DATE", "not a date (YYYY-MM-DD): '31/12/2025'"),
    ("25:00", "TIME", "not a time (HH:MM[:SS]): '25:00'"),
])
def test_invalid_values_are_null_with_a_reason(raw, col_type, reason):
    assert cast_value(raw, col_type) == (None, reason)


def test_naive_timestamp_is_labelled_utc_not_converted():
    value, error = cast_value("2025-06-01 00:00:00", "TIMESTAMPTZ")
    assert error is None and value == datetime(2025, 6, 1, tzinfo=UTC)


def test_timestamp_with_offset_is_kept():
    value, _ = cast_value("2025-06-15 10:00:00+02", "TIMESTAMPTZ")
    assert value.utcoffset() == timedelta(hours=2)
    assert value == datetime(2025, 6, 15, 10, tzinfo=timezone(timedelta(hours=2)))


# cast_column : type du contrat, puis contraintes du silver_type et de nullable

from decimal import Decimal  # noqa: E402
import uuid  # noqa: E402

from indusense.silver.casting import cast_column  # noqa: E402


def entry(col_type: str, silver_type: str, nullable: bool | None = None) -> dict:
    return {"target_name": "c", "type": col_type, "silver_type": silver_type} | (
        {} if nullable is None else {"nullable": nullable})


@pytest.mark.parametrize(("raw", "column", "expected"), [
    ("MACH-01", entry("IDENTIFIER", "VARCHAR(16)"), "MACH-01"),
    ("42", entry("IDENTIFIER", "INTEGER"), 42),
    ("2.76", entry("NUMBER", "NUMERIC(6,2)"), Decimal("2.76")),
    ("1,5", entry("NUMBER", "NUMERIC(6,2)"), Decimal("1.5")),
    ("8.0", entry("NUMBER", "NUMERIC(6,2)"), Decimal("8.0")),
    ("123.456", entry("NUMBER", "NUMERIC"), Decimal("123.456")),
    ("46.348", entry("NUMBER", "DOUBLE PRECISION"), 46.348),
    ("1", entry("BINARY", "BOOLEAN"), True),
    ("114", entry("INTEGER", "INTEGER"), 114),
    ("6f1c5b8e-6a51-4c1a-9a8f-2d3f1e0a7b11", entry("IDENTIFIER", "UUID"),
     uuid.UUID("6f1c5b8e-6a51-4c1a-9a8f-2d3f1e0a7b11")),
])
def test_cast_column_valid(raw, column, expected):
    assert cast_column(raw, column) == (expected, None)


@pytest.mark.parametrize(("raw", "column", "reason"), [
    ("MACHINE-0000000001", entry("IDENTIFIER", "VARCHAR(16)"), "longer than VARCHAR(16): 'MACHINE-0000000001'"),
    ("HIGHEST", entry("CATEGORY", "CHAR(4)"), "longer than CHAR(4): 'HIGHEST'"),
    ("MACH-01", entry("IDENTIFIER", "INTEGER"), "not an integer: 'MACH-01'"),
    ("2.765", entry("NUMBER", "NUMERIC(6,2)"), "not a NUMERIC(6,2): '2.765'"),
    ("10000", entry("NUMBER", "NUMERIC(6,2)"), "not a NUMERIC(6,2): '10000'"),
    ("40000", entry("INTEGER", "SMALLINT"), "out of SMALLINT range: '40000'"),
    ("n/a", entry("NUMBER", "NUMERIC(6,2)"), "not a number: 'n/a'"),
    ("abc", entry("IDENTIFIER", "UUID"), "not a UUID: 'abc'"),
])
def test_cast_column_rejects_what_postgresql_would_refuse_or_round(raw, column, reason):
    assert cast_column(raw, column) == (None, reason)


@pytest.mark.parametrize("raw", [None, ""])
def test_missing_value_is_an_error_only_when_not_nullable(raw):
    assert cast_column(raw, entry("NUMBER", "DOUBLE PRECISION", nullable=True)) == (None, None)
    assert cast_column(raw, entry("NUMBER", "DOUBLE PRECISION")) == (None, None)
    assert cast_column(raw, entry("NUMBER", "DOUBLE PRECISION", nullable=False)) == (
        None, "missing value (column is not nullable)")


@pytest.mark.parametrize(("raw", "silver_type"), [
    ("0.12345678901234567891", "DOUBLE PRECISION"),
    ("12345678901234567890", "DOUBLE PRECISION"),
    ("123456.789", "REAL"),
])
def test_float_that_cannot_hold_the_value_is_an_error(raw, silver_type):
    assert cast_column(raw, entry("NUMBER", silver_type)) == (None, f"loses precision in {silver_type}: {raw!r}")


@pytest.mark.parametrize(("raw", "silver_type"), [("1665.172", "DOUBLE PRECISION"), ("1,50", "DOUBLE PRECISION"), ("46.348", "REAL"),
                                                  ("1.5", "REAL")])
def test_float_that_holds_the_value_is_kept(raw, silver_type):
    assert cast_column(raw, entry("NUMBER", silver_type))[1] is None
