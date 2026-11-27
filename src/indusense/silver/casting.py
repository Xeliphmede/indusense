"""Conversion au mieux d'une valeur bronze (texte) vers le type déclaré par le contrat (`type`).

Le bronze stocke tout en texte ; cette conversion est la première étape de la couche silver. Elle sert aussi,
dès l'ingestion, à repérer les valeurs que silver ne pourra pas typer (voir bronze.ipynb, contrôles).

`cast_value` retourne (valeur, erreur) :

| Situation                        | valeur      | erreur                       |
| Valeur présente et convertible   | typée       | None                         |
| Valeur absente ou vide           | None        | None (une absence n'est pas une erreur) |
| Valeur non convertible           | None        | motif, ex. "not a number: 'n/a'"        |

Les horodatages sans fuseau sont **étiquetés** UTC (horloge déclarée par le contrat), jamais convertis (ADR-003).

`cast_column` est le cast de la couche silver pour une colonne du contrat : `cast_value` vers le `type`, puis
les contraintes du `silver_type` (longueur VARCHAR, précision NUMERIC et flottante, bornes des entiers, UUID) et
de `nullable`. Toute valeur que PostgreSQL refuserait ou arrondirait est ainsi une erreur détectée avant écriture.
"""

import math
import re
import uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation

import numpy as np

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


LENGTH_PATTERN = re.compile(r"(?:VARCHAR|CHARACTER VARYING|CHAR)\((\d+)\)")
NUMERIC_PATTERN = re.compile(r"NUMERIC(?:\((\d+)(?:,(\d+))?\))?")
INTEGER_RANGES = {"SMALLINT": 2**15, "INTEGER": 2**31, "BIGINT": 2**63}  # bornes : [-n, n - 1]


FLOAT_TYPES = {"DOUBLE PRECISION", "REAL"}


def exact_decimal(text: str) -> Decimal:
    """Valeur exacte d'un nombre écrit en texte (virgule décimale acceptée)."""
    try:
        value = Decimal(text.strip().replace(",", "."))
    except InvalidOperation:
        raise ValueError from None
    if not value.is_finite():
        raise ValueError
    return value


def float_decimal(value: float, silver_type: str = "DOUBLE PRECISION") -> Decimal:
    """Ce qu'un flottant PostgreSQL restitue réellement : plus courte écriture décimale du double (REAL : du float
    32 bits)."""
    return Decimal(str(np.float32(value)) if silver_type.upper() == "REAL" else repr(float(value)))


def to_numeric(text: str, precision: int | None, scale: int) -> Decimal:
    """Décimal exact ; refuse plus de `scale` décimales ou plus de `precision` chiffres (PostgreSQL arrondirait)."""
    value = exact_decimal(text)
    if precision is not None:
        exponent = value.normalize().as_tuple().exponent
        if -exponent > scale or abs(value) >= Decimal(10) ** (precision - scale):
            raise ValueError
    return value


def cast_column(raw: str | None, entry: dict):
    """(valeur typée | None, motif d'erreur | None) d'une valeur bronze pour une colonne du contrat :
    conversion vers `type`, puis respect de `silver_type` et de `nullable`."""
    value, error = cast_value(raw, entry["type"])
    if error:
        return None, error
    if value is None:
        return (None, "missing value (column is not nullable)") if entry.get("nullable") is False else (None, None)

    silver_type = entry["silver_type"].upper()
    text = raw.strip()
    if match := LENGTH_PATTERN.fullmatch(silver_type):
        if len(value) > int(match[1]):
            return None, f"longer than {silver_type}: {raw!r}"
    elif silver_type in INTEGER_RANGES:
        try:
            value = to_integer(text) if isinstance(value, str) else value  # IDENTIFIER stocké en entier
        except ValueError:
            return None, f"not an integer: {raw!r}"
        bound = INTEGER_RANGES[silver_type]
        if not -bound <= value < bound:
            return None, f"out of {silver_type} range: {raw!r}"
    elif match := NUMERIC_PATTERN.fullmatch(silver_type):
        precision, scale = (int(match[1]), int(match[2] or 0)) if match[1] else (None, 0)
        try:
            value = to_numeric(text, precision, scale)
        except ValueError:
            return None, f"not a {silver_type}: {raw!r}"
    elif silver_type in FLOAT_TYPES:
        if float_decimal(value, silver_type) != exact_decimal(text):
            return None, f"loses precision in {silver_type}: {raw!r}"
    elif silver_type == "UUID":
        try:
            value = uuid.UUID(text)
        except ValueError:
            return None, f"not a UUID: {raw!r}"
    return value, None


TYPE_TREATMENTS = {
    "NUMBER": "conversion en nombre : virgule décimale acceptée, NaN et infini refusés",
    "INTEGER": "conversion en entier : chiffres et signe uniquement (4.5 refusé)",
    "BINARY": "conversion en booléen : 1/true/t/yes/y -> vrai, 0/false/f/no/n -> faux (casse ignorée), autre refusé",
    "DATE": "conversion en date ISO YYYY-MM-DD",
    "TIME": "conversion en heure ISO HH:MM[:SS[.ffffff]]",
    "TIMESTAMPTZ": "conversion en horodatage ISO ; sans fuseau : étiqueté UTC, jamais converti (ADR-003) ; "
                   "avec fuseau : fuseau conservé",
}


def column_treatments(entry: dict) -> list[str]:
    """Traitements que `cast_column` applique à une colonne du contrat, dans l'ordre, en clair (rapport silver).
    Tenue à jour avec `cast_column` : toute règle ajoutée là doit être décrite ici."""
    col_type, silver_type = entry["type"], entry["silver_type"].upper()
    steps = ["valeur absente (NULL) ou vide / composée d'espaces -> NULL"]
    if col_type in CASTS:
        steps.append("espaces de début et de fin ignorés")
        steps.append(TYPE_TREATMENTS[col_type])
    elif silver_type in INTEGER_RANGES or silver_type == "UUID":
        steps.append("espaces de début et de fin ignorés")
    else:
        steps.append(f"type {col_type} : valeur texte conservée telle quelle, caractère pour caractère")
    if entry.get("pseudonymize"):
        steps.append("valeur déjà pseudonymisée en bronze (HMAC-SHA256 salé) : le pseudonyme est recopié")
    if match := LENGTH_PATTERN.fullmatch(silver_type):
        steps.append(f"longueur <= {match[1]} caractères vérifiée ({silver_type}) : jamais tronquée")
    elif silver_type in INTEGER_RANGES:
        if col_type not in CASTS:
            steps.append(f"identifiant {col_type} converti en entier : chiffres et signe uniquement")
        bound = INTEGER_RANGES[silver_type]
        steps.append(f"bornes {silver_type} vérifiées [{-bound}, {bound - 1}]")
    elif match := NUMERIC_PATTERN.fullmatch(silver_type):
        if match[1]:
            precision, scale = int(match[1]), int(match[2] or 0)
            steps.append(f"décimal exact {silver_type} : au plus {scale} décimale(s) et {precision - scale} chiffre(s) "
                         f"avant la virgule, jamais arrondi")
        else:
            steps.append("décimal exact NUMERIC, sans limite de précision")
    elif silver_type in FLOAT_TYPES:
        steps.append(f"le flottant {silver_type} doit restituer exactement le nombre écrit (sinon : précision "
                     f"insuffisante, refusé)")
    elif silver_type == "UUID":
        steps.append("conversion en UUID")
    if entry.get("nullable") is False:
        steps.append("valeur obligatoire (nullable: false) : une absence est une erreur")
    return steps
