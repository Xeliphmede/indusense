"""Conversion au mieux : une absence n'est pas une erreur, un échec garde son motif, l'heure est étiquetée UTC."""

from datetime import UTC, date, datetime, time, timedelta, timezone

import pytest

from indusense.bronze.casting import cast_value


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
