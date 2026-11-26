"""Contrat d'ingestion bronze (v2) : brouillon à partir du profil JSON, écriture et lecture de `config/bronze/*.json`.

Le profil (voir profile.py) fournit tout ce qui se déduit du fichier : format, encodage, séparateurs, colonnes,
types inférés, types SQL déclarés. Les décisions qui ne s'en déduisent pas (clé naturelle, unités inconnues,
descriptions, données personnelles, opérations silver) sont passées dans `decisions`. Le contrat écrit puis
versionné fait foi : c'est lui, pas le profil, qui pilote la validation et le chargement.
"""

import copy
import json
import re
from pathlib import Path

from indusense.settings import PROJECT_ROOT

CONFIG_DIR = PROJECT_ROOT / "config" / "bronze"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema" / "bronze.v2.schema.json"
CONTRACT_VERSION = "2.0.0"

# type inféré (carte d'identité) -> type du contrat
CONTRACT_TYPES = {
    "IDENTITY": "IDENTIFIER",
    "CATEGORY": "CATEGORY",
    "TEXT": "TEXT",
    "EMPTY": "TEXT",
    "INTEGER": "INTEGER",
    "FLOAT": "NUMBER",
    "BOOLEAN": "BINARY",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "TIMESTAMPTZ",
}

# type du contrat -> silver_type proposé quand le fichier ne déclare pas de type SQL (CSV)
DEFAULT_SILVER_TYPES = {
    "IDENTIFIER": "TEXT",
    "CATEGORY": "TEXT",
    "TEXT": "TEXT",
    "NUMBER": "DOUBLE PRECISION",
    "INTEGER": "INTEGER",
    "BINARY": "BOOLEAN",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
}

# suffixe du nom de colonne -> unité (NUMBER uniquement) ; sinon l'unité est une décision explicite
UNIT_SUFFIXES = {"_c": "CELSIUS", "_bar": "BAR", "_v": "VOLT", "_rpm": "RPM", "_hours": "HOUR"}

# encodage du profil -> encodage du contrat : un fichier ASCII est lu en UTF-8, qui le contient
CONTRACT_ENCODINGS = {"ascii": "utf-8"}


class ContractError(Exception):
    """Contrat illisible (JSON invalide) ; le message localise l'erreur."""


def normalize_sql_type(sql_type: str) -> str:
    """Type SQL déclaré, écrit comme en PostgreSQL : INT -> INTEGER, DECIMAL(6, 2) -> NUMERIC(6,2)."""
    text = re.sub(r"\s*,\s*", ",", sql_type.upper())
    text = re.sub(r"^DECIMAL\b", "NUMERIC", text)
    return {"INT": "INTEGER", "INT4": "INTEGER", "INT8": "BIGINT", "BOOL": "BOOLEAN"}.get(text, text)


def target_name(name: str) -> str:
    """Nom de colonne bronze : minuscules, caractères hors [a-z0-9_] remplacés par _."""
    return re.sub(r"[^a-z0-9_]+", "_", name.strip().lower()).strip("_") or "column"


def draft_column(column: dict) -> dict:
    """Entrée `columns[]` proposée pour une colonne du profil."""
    col_type = CONTRACT_TYPES[column["inferred_type"]]
    entry = {
        "source_name": column["name"],
        "target_name": target_name(column["name"]),
        "type": col_type,
        "silver_type": normalize_sql_type(column["sql_type"]) if column["sql_type"] else DEFAULT_SILVER_TYPES[col_type],
    }
    if col_type == "NUMBER":
        unit = next((u for suffix, u in UNIT_SUFFIXES.items() if column["name"].lower().endswith(suffix)), None)
        if unit:
            entry["unit"] = unit
    if column.get("missing") is not None:
        entry["nullable"] = column["missing"] > 0  # observé dans les données : à confirmer par le métier
    return entry


def project_path(path: str | Path) -> str:
    """Chemin relatif à la racine du projet (forme portable du contrat)."""
    path = Path(path).resolve()
    return path.relative_to(PROJECT_ROOT).as_posix() if path.is_relative_to(PROJECT_ROOT) else str(path)


def draft_contract(profile: dict, table: str, decisions: dict | None = None) -> dict:
    """Contrat bronze v2 d'une table du profil, complété des décisions :

    - target_table : nom de la table cible (défaut : nom de la table du profil),
    - natural_key : clé métier, en target_name,
    - max_cast_error_ratio : seuil d'erreurs de cast (défaut 0.0),
    - columns : {source_name: {clé: valeur}} fusionné dans l'entrée de la colonne (unit, description...),
    - derived_columns : opérations à exécuter en silver.

    Seules les colonnes réellement fournies par les données (provided) sont retenues."""
    decisions = copy.deepcopy(decisions or {})
    tables = {t["name"]: t for t in profile["tables"]}
    if table not in tables:
        raise KeyError(f"table '{table}' absente du profil de {profile['name']} (tables : {', '.join(tables)})")

    source = {"path": project_path(profile["source"]), "format": profile["format"],
              "encoding": CONTRACT_ENCODINGS.get(profile["encoding"], profile["encoding"])}
    if profile["format"] == "CSV":
        source |= {"field_delimiter": profile["field_delimiter"], "row_delimiter": profile["row_delimiter"],
                   "has_header": True}
    else:
        source["table"] = table

    overrides = decisions.get("columns", {})
    columns = []
    for column in tables[table]["columns"]:
        if column["provided"]:
            columns.append(draft_column(column) | overrides.get(column["name"], {}))
    unknown = set(overrides) - {c["source_name"] for c in columns}
    if unknown:
        raise KeyError(f"décisions sur des colonnes inconnues de {table} : {', '.join(sorted(unknown))}")

    contract = {
        "version": CONTRACT_VERSION,
        "kind": "BRONZE",
        "timezone": "UTC",
        "source": source,
        "target": {"schema": "bronze", "table": decisions.get("target_table", target_name(table)),
                   "natural_key": decisions.get("natural_key", [])},
        "max_cast_error_ratio": decisions.get("max_cast_error_ratio", 0.0),
        "columns": columns,
    }
    if decisions.get("derived_columns"):
        contract["derived_columns"] = decisions["derived_columns"]
    return contract


def write_contract(contract: dict, config_dir: Path = CONFIG_DIR) -> Path:
    """Écrit le contrat dans <config_dir>/<table cible>.json et retourne son chemin."""
    path = Path(config_dir) / f"{contract['target']['table']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_contract(path: Path) -> dict:
    """Lit un contrat ; une erreur de syntaxe est rapportée avec sa ligne et sa colonne."""
    path = Path(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ContractError(f"{path.name}: line {error.lineno}, column {error.colno}: {error.msg}") from error


def contract_paths(config_dir: Path = CONFIG_DIR) -> list[Path]:
    """Contrats du dossier, triés par nom."""
    return sorted(Path(config_dir).glob("*.json"))


def load_contracts(config_dir: Path = CONFIG_DIR) -> dict[str, dict]:
    """Tous les contrats du dossier : {nom du fichier: contrat}."""
    return {path.name: load_contract(path) for path in contract_paths(config_dir)}


def source_path(contract: dict) -> Path:
    """Chemin absolu du fichier source, résolu depuis la racine du projet."""
    path = Path(contract["source"]["path"])
    return path if path.is_absolute() else PROJECT_ROOT / path
