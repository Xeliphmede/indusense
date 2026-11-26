"""Rapport d'ingestion et dictionnaire de données d'un lot bronze (artefacts JSON).

Les deux portent une section `transformations` obligatoire (ADR-003) : ce qui a été fait à la donnée, pas
seulement combien de lignes. Un rapport sans cette section est invalide, pas incomplet.
"""

import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import Engine, select

from indusense.bronze.loader import LoadResult
from indusense.bronze.schema import HASH_COLUMN, TECHNICAL_COLUMNS, build_metadata, type_column

REPORTS_DIR = Path("artifacts/ingestion/bronze")


class InvalidReport(Exception):
    """Rapport sans section transformations."""


def transformations(contract: dict) -> list[dict]:
    """Ce que le bronze a fait à la donnée (copie en texte, traçabilité), puis ce que silver devra faire
    (typage vers silver_type, opérations dérivées), marqué non exécuté."""
    steps = []
    for entry in contract["columns"]:
        if entry.get("pseudonymize"):
            steps.append({"code": "ANO", "target": entry["target_name"], "source": [entry["source_name"]],
                          "layer": "bronze", "detail": "pseudonymisation HMAC-SHA256(ANONYMISATION_SALT, valeur "
                                                       "normalisée) ; la valeur d'origine n'est pas conservée"})
        else:
            steps.append({"code": "COPY", "target": entry["target_name"], "source": [entry["source_name"]],
                          "layer": "bronze", "detail": "valeur d'origine copiée telle quelle en TEXT, aucune conversion"})
    steps.append({"code": "TYPE", "target": [type_column(e["target_name"]) for e in contract["columns"]],
                  "source": ["contrat : columns[].type"], "layer": "bronze",
                  "detail": "ajout, après chaque colonne, de son type déclaré par le contrat (identique sur toutes les lignes)"})
    steps.append({"code": "HASH", "target": HASH_COLUMN, "source": [e["target_name"] for e in contract["columns"]],
                  "layer": "bronze", "detail": "SHA-256 du tableau JSON des valeurs stockées des colonnes du fichier "
                                               "(pseudonymes inclus, colonnes ajoutées exclues)"})
    steps.append({"code": "TRACE", "target": TECHNICAL_COLUMNS, "source": [contract["source"]["path"]],
                  "layer": "bronze", "detail": "ajout des colonnes de traçabilité (lot, fichier, SHA-256, numéro "
                                               "d'enregistrement, horodatage d'ingestion)"})
    for entry in contract["columns"]:
        detail = f"conversion en {entry['silver_type']} (type {entry['type']})"
        if entry["type"] == "TIMESTAMPTZ":
            detail += f", horloge {contract['timezone']} déclarée : valeur sans fuseau étiquetée, jamais convertie"
        steps.append({"code": "CAST", "target": entry["target_name"], "source": [entry["target_name"]],
                      "layer": "silver", "executed": False, "detail": detail})
    for entry in contract.get("derived_columns", []):
        steps.append({"code": entry["operation"], "target": entry["target_name"], "source": entry["operate_on_columns"],
                      "layer": "silver", "executed": False, "detail": entry.get("description", "")})
    return steps


ORDERED_TYPES = {"NUMBER", "INTEGER", "DATE", "TIME", "TIMESTAMPTZ"}  # min / max ont un sens


def column_statistics(contract: dict, result: LoadResult, engine: Engine) -> dict[str, dict]:
    """Par colonne, sur les lignes du fichier chargé (_source_hash). Le bronze étant en texte, les statistiques
    de type sont calculées en convertissant chaque valeur vers le type du contrat (conversion silver), sans rien
    écrire :

    - empty_rate : part de valeurs vides (NULL ou blanc) ;
    - cast_failures : nombre de valeurs non vides que silver ne saura pas convertir ;
    - null_rate : part de valeurs qui seront NULL une fois typées (vides + non convertibles) ;
    - min / max : sur les valeurs converties (types ordonnés uniquement) ;
    - distinct_values, max_length : sur le texte stocké."""
    from indusense.silver.casting import cast_value

    table = build_metadata([contract]).tables[f"bronze.{contract['target']['table']}"]
    stats = {}
    with engine.connect() as conn:
        for entry in contract["columns"]:
            col = table.c[entry["target_name"]]
            values = conn.execute(select(col).where(table.c._source_hash == result.source_hash)).scalars().all()
            total = len(values)
            empty = sum(1 for v in values if v is None or not v.strip())
            casts = [cast_value(v, entry["type"]) for v in values]
            typed = [value for value, error in casts if value is not None]
            failures = sum(1 for _, error in casts if error)
            ordered = entry["type"] in ORDERED_TYPES and typed
            stats[entry["target_name"]] = {
                "empty_rate": empty / total if total else None,
                "cast_failures": failures,
                "null_rate": (empty + failures) / total if total else None,
                "min": min(typed) if ordered else None,
                "max": max(typed) if ordered else None,
                "distinct_values": len({v for v in values if v is not None and v.strip()}),
                "max_length": max((len(v) for v in values if v is not None), default=None),
            }
    return stats


def build_report(contract: dict, result: LoadResult) -> dict:
    """Rapport d'ingestion : volumes, statut du lot, transformations."""
    return {"kind": "BRONZE_INGESTION_REPORT", **result.as_dict(),
            "source_path": contract["source"]["path"], "transformations": transformations(contract)}


def build_data_dictionary(contract: dict, result: LoadResult, engine: Engine) -> dict:
    """Dictionnaire de données : une entrée par colonne, plus les colonnes dérivées (silver)."""
    stats = column_statistics(contract, result, engine) if result.status == "SUCCESS" else {}
    columns = [
        {"source_name": entry["source_name"], "target_name": entry["target_name"],
         "bronze_type": "TEXT", "type_column": type_column(entry["target_name"]), "type": entry["type"],
         "unit": entry.get("unit"),
         "silver_type": entry["silver_type"], "personal_data": entry.get("personal_data", False),
         "pseudonymized": entry.get("pseudonymize", False),
         "description": entry.get("description"), **stats.get(entry["target_name"], {})}
        for entry in contract["columns"]
    ]
    return {"kind": "BRONZE_DATA_DICTIONARY", "contract": result.contract, "target_table": result.target_table,
            "batch_id": result.batch_id, "source_hash": result.source_hash, "columns": columns,
            "derived_columns": contract.get("derived_columns", []), "transformations": transformations(contract)}


def write_json(document: dict, path: Path) -> Path:
    if not document.get("transformations"):
        raise InvalidReport(f"{path.name}: missing 'transformations' section")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def write_reports(contract: dict, result: LoadResult, engine: Engine, reports_dir: Path = REPORTS_DIR) -> tuple[Path, Path]:
    """Écrit <reports_dir>/<AAAAMMJJ>/<table>_{report,data_dictionary}_<HHMMSS>.json ; retourne les deux chemins."""
    now = datetime.now()
    folder, stem = Path(reports_dir) / f"{now:%Y%m%d}", f"{contract['target']['table']}_%s_{now:%H%M%S}.json"
    return (write_json(build_report(contract, result), folder / (stem % "report")),
            write_json(build_data_dictionary(contract, result, engine), folder / (stem % "data_dictionary")))
