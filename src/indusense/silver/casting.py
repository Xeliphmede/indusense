"""Conversion au mieux d'une valeur bronze (texte) vers le type déclaré par le contrat (`type`).

Le bronze stocke tout en texte ; cette conversion est la première étape de la couche silver. Elle sert aussi,
dès l'ingestion, à repérer les valeurs que silver ne pourra pas typer (voir bronze.ipynb, contrôles).

`cast_value` retourne (valeur, erreur) :

| Situation                        | valeur      | erreur                       |
| Valeur présente et convertible   | typée       | None                         |
| Valeur absente ou vide           | None        | None (une absence n'est pas une erreur) |
| Valeur non convertible           | None        | motif, ex. "not a number: 'n/a'"        |

Les horodatages sans fuseau sont **étiquetés** UTC (horloge déclarée par le contrat), jamais convertis (ADR-003).
"""

import math
import re
from datetime import UTC, date, datetime, time

INTEGER_PATTERN = re.compile(r"[+-]?\d+")
TRUE_VALUES = {"1", "true", "t", "yes", "y"}
FALSE_VALUES = {"0", "false", "f", "no", "n"}


def to_number(text: str) -> float:
    value = float(text.replace(",", "."))  # virgule décimale acceptée, comme pour la carte d'identité
    if not math.isfinite(value):
        raise ValueError
    return value


def to_integer(text: str) -> int:
    if not INTEGER_PATTERN.fullmatch(text):
        raise ValueError
    return int(text)


def to_boolean(text: str) -> bool:
    lowered = text.lower()
    if lowered in TRUE_VALUES:
        return True
    if lowered in FALSE_VALUES:
        return False
    raise ValueError


def to_timestamptz(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value if value.tzinfo else value.replace(tzinfo=UTC)  # étiquetage, pas conversion


CASTS = {
    "NUMBER": (to_number, "not a number"),
    "INTEGER": (to_integer, "not an integer"),
    "BINARY": (to_boolean, "not a boolean"),
    "DATE": (date.fromisoformat, "not a date (YYYY-MM-DD)"),
    "TIME": (time.fromisoformat, "not a time (HH:MM[:SS])"),
    "TIMESTAMPTZ": (to_timestamptz, "not a timestamp (YYYY-MM-DD HH:MM[:SS][+HH])"),
}


def cast_value(raw: str | None, col_type: str):
    """(valeur typée | None, motif d'erreur | None) ; les types texte reprennent la valeur brute telle quelle."""
    if raw is None or not raw.strip():
        return None, None
    if col_type not in CASTS:  # IDENTIFIER, CATEGORY, TEXT
        return raw, None
    cast, reason = CASTS[col_type]
    try:
        return cast(raw.strip()), None
    except ValueError:
        return None, f"{reason}: {raw!r}"
