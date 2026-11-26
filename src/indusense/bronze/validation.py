"""Validation des contrats bronze, avant toute lecture ou écriture de données.

Toutes les règles sont évaluées et tous les problèmes sont retournés, chacun localisé (contrat + pointeur) :
valider ne lève jamais d'exception, même sur une source illisible.

| #  | Règle |
| 1  | Conformité au JSON Schema |
| 2  | Unicité des target_name (colonnes + dérivées) |
| 3  | operate_on_columns référence une colonne déclarée |
| 4  | Pas de collision avec les colonnes techniques (_batch_id...) |
| 5  | natural_key référence des colonnes déclarées |
| 6  | Compatibilité type <-> silver_type |
| 7  | source_name existe dans le fichier |
| 8  | source_name est réellement fourni par les données |
| 9  | La table SQL contient des lignes |
| 10 | Deux contrats n'écrivent pas la même table |
"""

import difflib
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

from indusense.bronze.contract import SCHEMA_PATH, ContractError, contract_paths, load_contract, source_path
from indusense.bronze.probes import SourceInspectionError, probe_source
from indusense.bronze.schema import TECHNICAL_COLUMNS

# type du contrat -> silver_type compatibles (expressions régulières, majuscules)
_CHAR = r"(TEXT|VARCHAR(\(\d+\))?|CHARACTER VARYING(\(\d+\))?|CHAR\(\d+\))"
_INT = r"(SMALLINT|INTEGER|BIGINT)"
COMPATIBLE_SILVER_TYPES = {
    "IDENTIFIER": rf"{_CHAR}|{_INT}|UUID",
    "CATEGORY": _CHAR,
    "TEXT": _CHAR,
    "NUMBER": r"DOUBLE PRECISION|REAL|NUMERIC(\(\d+(,\d+)?\))?",
    "INTEGER": _INT,
    "BINARY": r"BOOLEAN",
    "DATE": r"DATE",
    "TIME": r"TIME",
    "TIMESTAMPTZ": r"TIMESTAMPTZ|TIMESTAMP WITH TIME ZONE",
}


@dataclass(frozen=True)
class ValidationIssue:
    config: str   # nom du contrat (telemetry.json)
    pointer: str  # emplacement : columns[0].target_name, source, ...
    rule: int     # numéro de la règle (0 : contrat illisible)
    message: str

    def __str__(self) -> str:
        return f"{self.config}: {self.pointer}: {self.message}"


@lru_cache
def schema_validator() -> Draft202012Validator:
    return Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def json_pointer(path) -> str:
    """Chemin jsonschema (deque) -> columns[0].target_name."""
    text = ""
    for part in path:
        text += f"[{part}]" if isinstance(part, int) else f".{part}"
    return text.lstrip(".") or "(root)"


def check_schema(name: str, contract: dict) -> list[ValidationIssue]:
    """Règle 1 : conformité au JSON Schema."""
    errors = sorted(schema_validator().iter_errors(contract), key=lambda e: list(map(str, e.absolute_path)))
    return [ValidationIssue(name, json_pointer(e.absolute_path), 1, e.message) for e in errors]


def check_names(name: str, contract: dict) -> list[ValidationIssue]:
    """Règles 2 à 6 : cohérence interne du contrat."""
    issues = []
    columns, derived = contract["columns"], contract.get("derived_columns", [])
    # rule 2 : unicité des target_name, colonnes puis dérivées
    seen = {}
    for kind, entries in (("columns", columns), ("derived_columns", derived)):
        for i, entry in enumerate(entries):
            where = f"{kind}[{i}].target_name"
            if entry["target_name"] in seen:
                issues.append(ValidationIssue(name, where, 2, f"duplicate target_name '{entry['target_name']}' "
                                                               f"(already used at {seen[entry['target_name']]})"))
            else:
                seen[entry["target_name"]] = where
    declared = {c["target_name"] for c in columns}
    # rule 3 : les opérations silver portent sur des colonnes déclarées
    for i, entry in enumerate(derived):
        for j, ref in enumerate(entry["operate_on_columns"]):
            if ref not in declared:
                issues.append(ValidationIssue(name, f"derived_columns[{i}].operate_on_columns[{j}]", 3,
                                              f"'{ref}' is not a declared target_name in columns"))
    # rule 4 : les colonnes techniques sont réservées
    for i, entry in enumerate(columns):
        if entry["target_name"] in TECHNICAL_COLUMNS:
            issues.append(ValidationIssue(name, f"columns[{i}].target_name", 4,
                                          f"'{entry['target_name']}' is reserved for a technical column"))
    # rule 5 : clé naturelle
    for i, ref in enumerate(contract["target"]["natural_key"]):
        if ref not in declared:
            issues.append(ValidationIssue(name, f"target.natural_key[{i}]", 5, f"'{ref}' is not a declared target_name"))
    # rule 6 : type <-> silver_type
    for i, entry in enumerate(columns):
        pattern = COMPATIBLE_SILVER_TYPES[entry["type"]]
        silver = re.sub(r"\s*,\s*", ",", entry["silver_type"].strip().upper())
        if not re.fullmatch(pattern, silver):
            issues.append(ValidationIssue(name, f"columns[{i}].silver_type", 6,
                                          f"'{entry['silver_type']}' is not compatible with type '{entry['type']}'"))
    return issues


def check_source(name: str, contract: dict) -> list[ValidationIssue]:
    """Règles 7 à 9 : confrontation au fichier réel."""
    try:
        schema = probe_source(contract, source_path(contract))
    except SourceInspectionError as error:
        return [ValidationIssue(name, "source", 7, str(error))]
    issues = []
    if schema.duplicates:
        issues.append(ValidationIssue(name, "source", 7, f"duplicate column names in {schema.label}: "
                                                         f"{', '.join(schema.duplicates)}"))
    for i, entry in enumerate(contract["columns"]):
        column = entry["source_name"]
        where = f"columns[{i}].source_name"
        if column not in schema.declared:  # rule 7
            hint = difflib.get_close_matches(column, schema.declared, n=1)
            suffix = f". Did you mean '{hint[0]}'?" if hint else ""
            issues.append(ValidationIssue(name, where, 7, f"column '{column}' does not exist in {schema.label}{suffix}"))
        elif column not in schema.provided:  # rule 8
            issues.append(ValidationIssue(name, where, 8, f"column '{column}' exists in the DDL of {schema.label} "
                                                          f"but is never provided by the INSERT"))
    if schema.rows == 0:  # rule 9
        issues.append(ValidationIssue(name, "source.table", 9, f"{schema.label} has no INSERT: no data to ingest"))
    return issues


def validate_contract(name: str, contract: dict) -> list[ValidationIssue]:
    """Règles 1 à 9 pour un contrat ; les règles 2 à 9 supposent un contrat conforme au schéma."""
    issues = check_schema(name, contract)
    if issues:
        return issues
    return check_names(name, contract) + check_source(name, contract)


def validate_contracts(contracts: dict[str, dict]) -> list[ValidationIssue]:
    """Toutes les règles sur un ensemble de contrats {nom: contrat}, dont la règle 10 (tables cibles uniques)."""
    issues, targets = [], {}
    for name, contract in contracts.items():
        contract_issues = validate_contract(name, contract)
        issues += contract_issues
        if any(issue.rule == 1 for issue in contract_issues):
            continue
        target = f"{contract['target']['schema']}.{contract['target']['table']}"
        if target in targets:
            issues.append(ValidationIssue(name, "target.table", 10, f"{target} is already produced by {targets[target]}"))
        else:
            targets[target] = name
    return issues


def validate_directory(config_dir: Path) -> list[ValidationIssue]:
    """Valide tous les contrats d'un dossier ; un contrat illisible est un problème, pas une exception."""
    contracts, issues = {}, []
    for path in contract_paths(config_dir):
        try:
            contracts[path.name] = load_contract(path)
        except ContractError as error:
            issues.append(ValidationIssue(path.name, "(file)", 0, str(error)))
    return issues + validate_contracts(contracts)
