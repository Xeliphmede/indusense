"""Carte d'identité : rendu Markdown d'un profil (voir profile.py) et écriture de la carte et du profil JSON."""

import json
from datetime import datetime
from pathlib import Path

from indusense.inference import NUMERIC_TYPES
from indusense.profile import profile_file
from indusense.reading import format_filesize

ARTIFACTS_DIR = Path("artifacts/ingestion/identities")


def md_table(headers: list[str], rows: list[list]) -> list[str]:
    """Lignes d'un tableau Markdown."""
    return [
        f"| {' | '.join(headers)} |",
        "|" + " --- |" * len(headers),
        *(f"| {' | '.join(map(str, row))} |" for row in rows),
    ]


def type_label(column: dict) -> str:
    """Type inféré, complété des remarques de lecture ; "-" pour une colonne absente des données."""
    if not column["provided"]:
        return "-"
    notes = column["notes"]
    return f"{column['inferred_type']} ({', '.join(notes)})" if notes else column["inferred_type"]


def stats_row(column: dict) -> list[str]:
    """[colonne, min, max, mean, median] formatés ; mean et median valent "-" hors numériques."""
    col_type, stats = column["inferred_type"], column["stats"]
    if col_type in NUMERIC_TYPES:
        fmt = "{}" if col_type == "INTEGER" else "{:.3f}"
        return [column["name"], fmt.format(stats["min"]), fmt.format(stats["max"]),
                f"{stats['mean']:.3f}", f"{stats['median']:.3f}"]
    suffix = " UTC" if "UTC" in column["notes"] else ""
    return [column["name"], stats["min"] + suffix, stats["max"] + suffix, "-", "-"]


def render_table(table: dict, level: str) -> list[str]:
    """Dimensions, types inférés (et types SQL déclarés) et statistiques d'une table."""
    columns = table["columns"]
    if any(col["sql_type"] for col in columns):
        column_list = md_table(
            ["Colname", "SQL Type", "Inferred Type"],
            [[col["name"], col["sql_type"] or "-", type_label(col)] for col in columns],
        )
    else:
        column_list = md_table(["Colname", "Inferred Type"], [[col["name"], type_label(col)] for col in columns])
    stats = [stats_row(col) for col in columns if col["stats"]]
    return [
        f"- Rows Number: {table['rows']}",
        f"- Cols Number: {len(columns)}",
        "", f"{level} Column list", "", *column_list,
        "", f"{level} Numeric and date columns", "",
        *md_table(["Colname", "Min value", "Max value", "Mean value", "Median value"], stats),
    ]


def render_card(profile: dict) -> str:
    """Carte d'identité Markdown d'un profil."""
    lines = [
        f"# Carte d'identité : {profile['name']}",
        "",
        f"- Source: {profile['source']}",
        f"- Filesize: {format_filesize(profile['filesize'])}",
        f"- ProcessBeginAt: {datetime.fromisoformat(profile['began_at']):%H:%M:%S}",
        f"- ProcessEndAt: {datetime.fromisoformat(profile['ended_at']):%H:%M:%S}",
        f"- Charset: {profile['charset']}",
    ]
    if profile["format"] == "SQL":
        lines += [
            f"- SQL Dialect: {profile['dialect']}",
            f"- Statements Number: {profile['statements']}",
            f"- Tables Number: {len(profile['tables'])}",
        ]
        for table in profile["tables"]:
            lines += ["", f"## Table `{table['name']}`", "", *render_table(table, level="###")]
    else:
        lines += render_table(profile["tables"][0], level="##")
    return "\n".join(lines) + "\n"


def write_card(path: Path) -> Path:
    """Écrit la carte Markdown et le profil JSON du fichier dans ARTIFACTS_DIR/<AAAAMMJJ>/<nom_fichier>_<HHMMSS>
    (.md et .json) ; retourne le chemin de la carte."""
    profile = profile_file(path)
    began_at = datetime.fromisoformat(profile["began_at"])
    output = ARTIFACTS_DIR / f"{began_at:%Y%m%d}" / f"{Path(path).name.split('.')[0]}_{began_at:%H%M%S}.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_card(profile), encoding="utf-8")
    output.with_suffix(".json").write_text(json.dumps(profile, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output
