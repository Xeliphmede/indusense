"""Profil JSON et carte Markdown : la carte est rendue à partir du profil, à l'identique de la version Sprint 1."""

import json
from pathlib import Path

import pytest

from indusense import profile_file, render_card, write_card
from indusense.profile import file_format

FIXTURES = Path(__file__).parent / "fixtures" / "cards"
SOURCES = ["telemetry.csv", "releves_incidents.csv", "machine.sql.txt"]


def stable_lines(card: str) -> list[str]:
    """Lignes de la carte hors horaires de traitement et chemin absolu (propres à la machine et à l'exécution)."""
    return [line for line in card.splitlines() if not line.startswith(("- Process", "- Source"))]


@pytest.mark.parametrize("name", SOURCES)
def test_card_is_identical_to_the_sprint1_card(data_dir, name):
    expected = (FIXTURES / f"{name.split('.')[0]}.md").read_text(encoding="utf-8")
    assert stable_lines(render_card(profile_file(data_dir / name))) == stable_lines(expected)


@pytest.mark.parametrize("name", SOURCES)
def test_profile_is_json_serialisable(data_dir, name):
    profile = profile_file(data_dir / name)
    assert json.loads(json.dumps(profile)) == profile


def test_csv_profile_describes_how_to_read_the_file(data_dir):
    profile = profile_file(data_dir / "telemetry.csv")
    assert profile["format"] == "CSV"
    assert (profile["field_delimiter"], profile["row_delimiter"]) == (",", "\n")
    [table] = profile["tables"]
    assert (table["name"], table["rows"], len(table["columns"])) == ("telemetry", 135626, 7)


def test_sql_profile_flags_columns_absent_from_the_insert(data_dir):
    profile = profile_file(data_dir / "machine.sql.txt")
    machine = {table["name"]: table for table in profile["tables"]}["machine"]
    absent = [col["name"] for col in machine["columns"] if not col["provided"]]
    assert machine["rows"] == 15
    assert absent == ["is_active", "created_at", "updated_at"]


def test_write_card_writes_markdown_and_json_side_by_side(data_dir, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    card = write_card(data_dir / "releves_incidents.csv")
    profile = json.loads(card.with_suffix(".json").read_text(encoding="utf-8"))
    assert card.read_text(encoding="utf-8") == render_card(profile)


def test_unsupported_format_is_rejected():
    with pytest.raises(ValueError, match="format non pris en charge"):
        file_format(Path("data.xlsx"))
