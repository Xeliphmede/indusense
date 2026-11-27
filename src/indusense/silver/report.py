"""Rapport Markdown d'une exécution silver : ce qui a été fait, à quelle date, à chaque colonne.

Un fichier par exécution, `artifacts/ingestion/silver/<AAAAMMJJ>/silver_<HHMMSS>_<run_id>.md`, écrit que
l'exécution réussisse ou non. Il dit :
- quand (début, fin, UTC), sur quel batch bronze, avec quelle taille de lot d'écriture (`batch_size`), avec quel
  code (commit git) et quels contrats (version, empreinte SHA-256) ;
- pour chaque table : le résultat et les traitements de la table ;
- pour chaque colonne : les volumes (valeurs typées, NULL, erreurs) et la liste ordonnée des traitements
  appliqués, générée par `column_treatments` à partir des mêmes règles que le cast ;
- les erreurs (cast ou fidélité) et ce qui n'a pas été exécuté (colonnes dérivées).
"""

import hashlib
import json
import subprocess
from collections import Counter
from datetime import datetime
from pathlib import Path

from indusense.settings import PROJECT_ROOT
from indusense.silver.casting import column_treatments
from indusense.silver.schema import table_name

REPORTS_DIR = Path("artifacts/ingestion/silver")
MAX_REPORTED_ERRORS = 100  # par table


def contract_fingerprint(contract: dict) -> str:
    return hashlib.sha256(json.dumps(contract, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def code_version() -> str:
    """Commit git du code qui a fait le traitement, `+modifications` si l'arbre de travail n'est pas propre."""
    try:
        sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT_ROOT, capture_output=True,
                             text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=PROJECT_ROOT,
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "inconnu (git indisponible)"
    return f"{sha}{' +modifications non committées' if dirty else ''}"


def utc(moment: datetime | None) -> str:
    return f"{moment:%Y-%m-%d %H:%M:%S.%f}"[:-3] + " UTC" if moment else "-"


def table_treatments(contract: dict, result, batch_size: int) -> list[str]:
    table = table_name(contract)
    return [
        f"lecture des lignes de `bronze.{table}` du batch `{result.batch_id}`, dans l'ordre de `_id`",
        "typage de toutes les lignes en mémoire, toutes les erreurs collectées, avant la moindre écriture",
        f"suppression des lignes de `silver.{table}` déjà issues de ce batch (retypage sans doublon)",
        f"insertion par lots de {batch_size:,} lignes".replace(",", " ")
        + ", avec `_batch_id`, `_bronze_id` (= `bronze._id`) et `_cast_at`",
        "relecture de la table telle que stockée et rapprochement avec le bronze (pandera), avant le commit",
        "une seule transaction pour toutes les tables : au moindre écart, rien n'est gardé",
        f"`ANALYZE silver.{table}` après le commit",
    ]


def render(contracts: dict[str, dict], results: list, batch_size: int) -> str:
    first = results[0]
    status = "SUCCESS" if all(r.status == "SUCCESS" for r in results) else "FAILED"
    completed = max((r.completed_at for r in results if r.completed_at), default=None)
    lines = [
        f"# Traitement silver du {first.started_at:%Y-%m-%d à %H:%M:%S} UTC",
        "",
        "| | |",
        "| --- | --- |",
        f"| Exécution silver (`run_id`) | `{first.run_id}` |",
        f"| Batch bronze typé (`batch_id`) | `{first.batch_id}` |",
        f"| Début | {utc(first.started_at)} |",
        f"| Fin | {utc(completed)} |",
        f"| Taille des lots d'écriture (`batch_size`) | {batch_size} lignes |",
        f"| Statut | **{status}** |",
        f"| Code | commit `{code_version()}` |",
        "",
    ]
    if status != "SUCCESS":
        lines += ["> **Exécution annulée** : aucune ligne n'a été gardée en silver pour ce batch. Le batch bronze est "
                  "conservé ; corriger le contrat puis `uv run indusense silver cast " f"{first.batch_id}`.", ""]
    lines += ["## Tables", "",
              "| Table | Contrat | Statut | Lignes lues | Lignes écrites | Valeurs comparées au bronze | Erreurs |",
              "| --- | --- | --- | ---: | ---: | ---: | ---: |"]
    for r in results:
        lines.append(f"| `{r.target_table}` | `{r.contract}` | {r.status} | {r.rows_read} | {r.rows_inserted} | "
                     f"{r.values_checked} | {len(r.errors)} |")

    for result, contract in zip(results, contracts.values()):
        errors_by_column = Counter(e.column for e in result.errors)
        lines += ["", f"## `{result.target_table}`", "",
                  f"Source : `bronze.{table_name(contract)}`, contrat `{result.contract}` (version "
                  f"{contract.get('version', '?')}, SHA-256 `{contract_fingerprint(contract)[:16]}`).", ""]
        if result.error:
            lines += [f"Résultat : **{result.status}** — {result.error}", ""]
        lines += ["Traitements de la table :", ""]
        lines += [f"{i}. {step}" for i, step in enumerate(table_treatments(contract, result, batch_size), 1)]
        for entry in contract["columns"]:
            name = entry["target_name"]
            counts = result.column_counts.get(name, {})
            steps = [f"lecture de `bronze.{table_name(contract)}.{name}` (TEXT)", *column_treatments(entry),
                     f"écriture dans `silver.{table_name(contract)}.{name}` ({entry['silver_type']})",
                     f"relecture et comparaison au bronze (règle pandera `restores:{name}`) : la valeur stockée doit "
                     f"restituer exactement le texte bronze"]
            lines += ["", f"### `{name}` : {entry['type']} -> {entry['silver_type']}", "",
                      f"{counts.get('values', 0)} valeur(s) typée(s), {counts.get('nulls', 0)} NULL, "
                      f"{errors_by_column.get(name, 0)} erreur(s).", ""]
            lines += [f"{i}. {step}" for i, step in enumerate(steps, 1)]
        if errors_by_column.get("_bronze_id"):
            lines += ["", f"{errors_by_column['_bronze_id']} ligne(s) bronze sans ligne silver."]
        if result.errors:
            shown = result.errors[:MAX_REPORTED_ERRORS]
            lines += ["", f"### Erreurs ({len(result.errors)}"
                          + (f", {len(shown)} premières" if len(shown) < len(result.errors) else "") + ")", "",
                      "| Colonne | Ligne source | `_bronze_id` | Valeur bronze | Motif |", "| --- | ---: | ---: | --- | --- |"]
            lines += [f"| `{e.column}` | {e.row_number} | {e.bronze_id} | `{e.value!r}` | {e.reason.replace('|', '\\|')} |"
                      for e in shown]
        if contract.get("derived_columns"):
            lines += ["", "Non exécuté (déclaré par le contrat, à venir) :", ""]
            lines += [f"- `{d['target_name']}` : {d['operation']} de {', '.join(d['operate_on_columns'])} "
                      f"({d.get('description', '')})" for d in contract["derived_columns"]]
    return "\n".join(lines) + "\n"


def write_report(contracts: dict[str, dict], results: list, batch_size: int,
                 report_dir: Path = REPORTS_DIR) -> Path:
    first = results[0]
    path = Path(report_dir) / f"{first.started_at:%Y%m%d}" / f"silver_{first.started_at:%H%M%S}_{str(first.run_id)[:8]}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(contracts, results, batch_size), encoding="utf-8")
    return path
