"""Dictionnaire de données silver : `docs/silver_dictionary.md`, généré, versionné.

Pour chaque table silver : sa source, sa clé naturelle, ses volumes ; pour chaque colonne : description, type
PostgreSQL, unité, caractère obligatoire, donnée personnelle, et des statistiques mesurées sur un batch silver
(part de NULL, min / max, valeurs possibles).

Tout vient des contrats (`config/bronze/*.json`, eux-mêmes générés depuis les décisions de `bronze.ipynb`) et de
la base : le fichier ne se modifie pas à la main. Pour changer une description ou une unité, corriger la décision
dans `bronze.ipynb`, régénérer les contrats, puis `uv run indusense silver dictionary`.
"""

from datetime import UTC, date, datetime, time
from pathlib import Path

from sqlalchemy import Engine, text

from indusense.bronze.contract import UNIT_LABELS
from indusense.settings import PROJECT_ROOT
from indusense.silver.dedup import RULE as DEDUP_RULE
from indusense.silver.schema import SCHEMA, table_name

DICTIONARY_PATH = PROJECT_ROOT / "docs" / "silver_dictionary.md"
ORDERED_TYPES = {"NUMBER", "INTEGER", "DATE", "TIME", "TIMESTAMPTZ"}  # min / max ont un sens
MAX_LISTED_VALUES = 12

UNIT_NAMES = {"CELSIUS": "degré Celsius", "BAR": "bar (pression)", "VOLT": "volt", "RPM": "tours par minute",
              "HOUR": "heure (durée)", "PIECE_PER_HOUR": "pièces produites par heure",
              "PIECE_PER_DAY": "pièces produites par jour"}

# Observations sur les données, à confirmer par le métier : elles ne sont pas des définitions.
NOTES = {
    "machine": "`max_daily_capacity` vaut environ 16 fois `max_hourly_capacity_pieces` sur toutes les machines "
               "(une journée de 16 h ?), alors que les incidents sont déclarés sur trois équipes : à confirmer.",
    "telemetry": "27,4 % des heures dépassent `machine.max_hourly_capacity_pieces` ; des valeurs collées aux bornes "
                 "(1 900 et 1 100 tr/min, 80 °C) suggèrent des capteurs saturés (voir `silver.ipynb`, section 8).",
}


def latest_batch(engine: Engine):
    """Dernier batch bronze typé avec succès en silver."""
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT batch_id FROM {SCHEMA}._cast_run WHERE status = 'SUCCESS' "
                                 f"GROUP BY batch_id, run_id ORDER BY max(completed_at) DESC LIMIT 1")).scalar_one()


def fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return f"{value.astimezone(UTC):%Y-%m-%d %H:%M} UTC"
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def column_stats(conn, contract: dict, entry: dict, batch_id) -> dict:
    table, name = f"{SCHEMA}.{table_name(contract)}", f'"{entry["target_name"]}"'
    ordered = entry["type"] in ORDERED_TYPES
    rows, nulls, distinct, low, high = conn.execute(text(
        f"SELECT count(*), count(*) FILTER (WHERE {name} IS NULL), count(DISTINCT {name}), "
        f"{f'min({name}), max({name})' if ordered else 'NULL, NULL'} FROM {table} WHERE _batch_id = :b"),
        {"b": batch_id}).one()
    values = ""
    if entry["type"] in ("CATEGORY", "BINARY") and distinct <= MAX_LISTED_VALUES:
        counts = conn.execute(text(f"SELECT {name}, count(*) FROM {table} WHERE _batch_id = :b AND {name} IS NOT NULL "
                                   f"GROUP BY 1 ORDER BY 2 DESC, 1"), {"b": batch_id}).all()
        values = ", ".join(f"{'vrai' if v is True else 'faux' if v is False else v} ({n})" for v, n in counts)
    elif entry.get("pseudonymize"):
        values = f"{distinct} pseudonymes distincts"
    elif not ordered:
        values = f"{distinct} valeurs distinctes"
    return {"rows": rows, "nulls": nulls, "min": fmt(low), "max": fmt(high), "values": values}


def personal(entry: dict) -> str:
    if entry.get("pseudonymize"):
        return "oui, pseudonymisée"
    return "oui" if entry.get("personal_data") else ""


def render_dictionary(contracts: dict[str, dict], engine: Engine, batch_id=None) -> str:
    batch_id = batch_id or latest_batch(engine)
    lines = [
        "# Dictionnaire de données silver",
        "",
        "> Fichier **généré** par `uv run indusense silver dictionary` (ou `silver.ipynb`) : ne pas le modifier à la "
        "main. Les descriptions et les unités viennent des contrats `config/bronze/*.json`, générés depuis les "
        "décisions de `bronze.ipynb` ; les statistiques, du batch silver indiqué.",
        "",
        f"Batch décrit : `{batch_id}`, généré le {datetime.now(UTC):%Y-%m-%d à %H:%M} UTC.",
        "",
        "## Unités",
        "",
        "| Unité (contrat) | Symbole | Signification |",
        "| --- | --- | --- |",
        *(f"| `{unit}` | {UNIT_LABELS[unit]} | {UNIT_NAMES[unit]} |" for unit in UNIT_LABELS),
        "",
        "Les horodatages sont en UTC (horloge déclarée, ADR-003).",
        "",
        "## Colonnes techniques (toutes les tables)",
        "",
        "| Colonne | Type | Description |",
        "| --- | --- | --- |",
        "| `_batch_id` | UUID | Exécution d'ingestion bronze dont vient la ligne (voir `bronze._ingestion_batch`) |",
        "| `_bronze_id` | BIGINT | Clé primaire ; ligne d'origine `bronze.<table>._id` (fichier, numéro de ligne, "
        "valeur brute) |",
        "| `_cast_at` | TIMESTAMPTZ | Instant UTC du typage |",
    ]
    with engine.connect() as conn:
        for contract in contracts.values():
            table = table_name(contract)
            key = contract["target"].get("natural_key") or []
            rows = conn.execute(text(f"SELECT count(*) FROM {SCHEMA}.{table} WHERE _batch_id = :b"),
                                {"b": batch_id}).scalar_one()
            dropped = conn.execute(text(f"SELECT count(*) FROM {SCHEMA}._duplicate WHERE batch_id = :b "
                                        f"AND target_table = :t"), {"b": batch_id, "t": f"{SCHEMA}.{table}"}).scalar_one()
            source = f"`{contract['source']['path']}`" + (f" (table `{contract['source']['table']}`)"
                                                          if contract["source"].get("table") else "")
            lines += ["", f"## `{SCHEMA}.{table}`", "",
                      f"- **Source** : {source}, via `bronze.{table}`",
                      f"- **Clé naturelle** : {', '.join(f'`{k}`' for k in key) or 'aucune'}"
                      + (" (une ligne par clé et par batch, ADR-004)" if key else ""),
                      f"- **Lignes** (batch décrit) : {rows}"
                      + (f", après {dropped} doublon(s) écarté(s) ({DEDUP_RULE})" if dropped else ""),
                      "",
                      "| Colonne | Description | Type | Unité | Obligatoire | Donnée personnelle | NULL | Min | Max "
                      "| Valeurs |",
                      "| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |"]
            for entry in contract["columns"]:
                stats = column_stats(conn, contract, entry, batch_id)
                null_rate = f"{100 * stats['nulls'] / stats['rows']:.1f} %" if stats["rows"] else ""
                lines.append(
                    f"| `{entry['target_name']}` | {cell(entry.get('description', ''))} | {entry['silver_type']} | "
                    f"{UNIT_LABELS.get(entry.get('unit'), '')} | {'non' if entry.get('nullable', True) else 'oui'} | "
                    f"{personal(entry)} | {null_rate} | {cell(stats['min'])} | {cell(stats['max'])} | "
                    f"{cell(stats['values'])} |")
            if contract.get("derived_columns"):
                lines += ["", "Colonnes déclarées par le contrat, pas encore produites :", ""]
                lines += [f"- `{d['target_name']}` ({d['silver_type']}) : {d['operation']} de "
                          f"{', '.join(f'`{c}`' for c in d['operate_on_columns'])}, {d.get('description', '')}"
                          for d in contract["derived_columns"]]
            if table in NOTES:
                lines += ["", f"Observation (à confirmer par le métier) : {NOTES[table]}"]
    return "\n".join(lines) + "\n"


def write_dictionary(contracts: dict[str, dict], engine: Engine, path: Path = DICTIONARY_PATH,
                     batch_id=None) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_dictionary(contracts, engine, batch_id), encoding="utf-8")
    return path
