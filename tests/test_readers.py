"""Lecture en flux des sources, en texte brut."""

import pytest

from indusense.bronze.readers import SourceFormatError, iter_csv_rows, iter_sql_rows


def test_quoted_newline_stays_in_the_value(tmp_path):
    path = tmp_path / "quoted.csv"
    path.write_text('id,comment\n1,"two\nlines"\n2,plain\n', newline="")
    rows = list(iter_csv_rows(path, "utf-8", ",", ["id", "comment"]))
    assert rows == [(1, {"id": "1", "comment": "two\nlines"}), (2, {"id": "2", "comment": "plain"})]


def test_values_are_raw_text_and_blanks_are_kept(tmp_path):
    path = tmp_path / "raw.csv"
    path.write_text("a;b\n 0012 ;\n", newline="")
    [(_, values)] = iter_csv_rows(path, "utf-8", ";", ["a", "b"])
    assert values == {"a": " 0012 ", "b": ""}


@pytest.mark.parametrize("line", ["1", "1,2,3"])
def test_malformed_row_is_an_error_not_a_silent_loss(tmp_path, line):
    path = tmp_path / "malformed.csv"
    path.write_text(f"a,b\n1,2\n{line}\n", newline="")
    with pytest.raises(SourceFormatError, match=r"malformed.csv: record 2: \d fields, 2 expected"):
        list(iter_csv_rows(path, "utf-8", ",", ["a", "b"]))


def test_crlf_rows(tmp_path):
    path = tmp_path / "crlf.csv"
    path.write_bytes(b"a,b\r\n1,2\r\n3,4\r\n")
    assert [v for _, v in iter_csv_rows(path, "utf-8", ",", ["b"])] == [{"b": "2"}, {"b": "4"}]


def test_both_tables_of_the_dump_are_read_completely(data_dir):
    path = data_dir / "machine.sql.txt"
    machine = list(iter_sql_rows(path, "utf-8-sig", "machine", ["machine_code"]))
    maintenance = list(iter_sql_rows(path, "utf-8-sig", "maintenance", ["maintenance_id", "duration_hours",
                                                                         "related_incident_id"]))
    assert len(machine) + len(maintenance) == 130
    assert maintenance[0][1] == {"maintenance_id": "1", "duration_hours": "2.24", "related_incident_id": None}
    assert [n for n, _ in machine] == list(range(1, 16))


def test_sql_literals_containing_separators(tmp_path):
    path = tmp_path / "dump.sql"
    path.write_text("CREATE TABLE t (a INT, b TEXT);\nINSERT INTO t (a, b) VALUES (-1, 'x, (y)'), (2, 'it''s');\n")
    assert [v for _, v in iter_sql_rows(path, "utf-8", "t", ["a", "b"])] == [
        {"a": "-1", "b": "x, (y)"}, {"a": "2", "b": "it's"}]
