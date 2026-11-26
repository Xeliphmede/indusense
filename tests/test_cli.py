"""Ligne de commande : cartes d'identité et validation des contrats bronze."""

import json
import subprocess
import sys

from indusense.cli import main


def test_bronze_validate_accepts_the_repository_contracts(capsys):
    assert main(["bronze", "validate"]) == 0
    assert "4 contrat(s) valide(s)" in capsys.readouterr().out


def test_bronze_validate_reports_every_issue_and_fails(contracts, tmp_path, capsys):
    contract = contracts["maintenance.json"]
    contract["timezone"] = "Europe/Paris"
    path = tmp_path / "maintenance.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    broken = tmp_path / "broken.json"
    broken.write_text("{", encoding="utf-8")
    assert main(["bronze", "validate", str(path), str(broken)]) == 1
    err = capsys.readouterr().err
    assert "maintenance.json: timezone: 'UTC' was expected" in err
    assert "broken.json: (file): broken.json: line 1, column 2" in err
    assert "2 problème(s) dans 2 contrat(s)" in err


def test_identity_cards_still_work(data_dir, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main([str(data_dir / "machine.sql.txt")]) == 0
    assert capsys.readouterr().out.strip().endswith(".md")


def test_import_loads_neither_driver_nor_parser():
    code = ("import sys, indusense, indusense.bronze; "
            "print(sorted(m for m in ('pandas', 'sqlglot', 'psycopg', 'sqlalchemy') if m in sys.modules))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout
    assert out.strip() == "[]"
