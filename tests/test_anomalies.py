"""Indicateurs d'anomalies : bornes de Tukey, valeurs aux bornes, doublons de clé."""

import pandas as pd
import pytest

from indusense.silver.anomalies import anomaly_summary, duplicate_keys, tukey_bounds


def test_tukey_bounds():
    assert tukey_bounds(pd.Series([1, 2, 3, 4, 5])) == (-1.0, 7.0)  # Q1 = 2, Q3 = 4, IQR = 2


def test_summary_counts_outliers_nulls_and_values_stuck_at_bounds():
    frame = pd.DataFrame({"rpm": [1500, 1510, 1520, 1530, 1540, 1900, 1900, 1900, None]})
    row = anomaly_summary(frame, ["rpm"]).loc["rpm"]
    assert (row["valeurs"], row["NULL"]) == (8, 1)
    assert row["aberrantes"] == 0  # 3 valeurs sur 8 à 1900 : Q3 = 1900, ce n'est plus « isolé »
    assert (row["au minimum"], row["au maximum"]) == (1, 3)
    frame = pd.DataFrame({"t": [10.0] * 10 + [11.0] * 10 + [80.0]})
    row = anomaly_summary(frame, ["t"]).loc["t"]
    assert row["aberrantes"] == 1 and row["aberrantes %"] == pytest.approx(4.76)


def test_decimal_values_are_accepted():
    from decimal import Decimal

    frame = pd.DataFrame({"d": [Decimal("1.5"), Decimal("2.25"), None]}, dtype=object)
    assert anomaly_summary(frame, ["d"]).loc["d", "max"] == 2.25


def test_duplicate_keys():
    frame = pd.DataFrame({"m": ["A", "A", "A", "B"], "t": [1, 1, 2, 1]})
    assert duplicate_keys(frame, ["m", "t"]) == 1
