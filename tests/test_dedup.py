"""Dédoublonnage sur la clé naturelle (ADR-004) : la ligne la plus complète, puis la première du fichier."""

from datetime import UTC, datetime

from indusense.silver.dedup import RULE, deduplicate

CONTRACT = {"target": {"natural_key": ["machine_id", "timestamp"]},
            "columns": [{"target_name": c} for c in ("machine_id", "timestamp", "temperature_c", "pressure_bar")]}
T0, T1 = datetime(2025, 6, 1, tzinfo=UTC), datetime(2025, 6, 1, 1, tzinfo=UTC)


def record(bronze_id, machine, ts, temperature, pressure):
    return {"_bronze_id": bronze_id, "_batch_id": None, "machine_id": machine, "timestamp": ts,
            "temperature_c": temperature, "pressure_bar": pressure}


def test_first_row_is_kept_when_equally_complete():
    kept, dropped = deduplicate(CONTRACT, [record(1, "M", T0, 46.348, 198.2), record(2, "M", T0, 46.332, 198.2),
                                           record(3, "M", T1, 40.0, 199.0)])
    assert [r["_bronze_id"] for r in kept] == [1, 3]
    assert dropped == [{"bronze_id": 2, "kept_bronze_id": 1, "natural_key": '{"machine_id": "M", "timestamp": '
                        '"2025-06-01 00:00:00+00:00"}', "missing_values": 0, "kept_missing_values": 0, "reason": RULE}]


def test_most_complete_row_wins_over_the_first():
    kept, dropped = deduplicate(CONTRACT, [record(1, "M", T0, None, 198.2), record(2, "M", T0, 46.3, 198.2),
                                           record(3, "M", T0, None, None)])
    assert [r["_bronze_id"] for r in kept] == [2]
    assert [(d["bronze_id"], d["kept_bronze_id"], d["missing_values"]) for d in dropped] == [(1, 2, 1), (3, 2, 2)]


def test_kept_row_is_copied_unchanged():
    rows = [record(1, "M", T0, 46.348, None), record(2, "M", T0, None, 198.2)]
    kept, _ = deduplicate(CONTRACT, rows)
    assert kept == [rows[0]]  # rien n'est fusionné : la pression du doublon n'est pas reprise


def test_null_key_and_no_key_are_never_deduplicated():
    rows = [record(1, None, T0, 1.0, 1.0), record(2, None, T0, 1.0, 1.0)]
    assert deduplicate(CONTRACT, rows) == (rows, [])
    assert deduplicate({"target": {}, "columns": CONTRACT["columns"]}, rows) == (rows, [])
