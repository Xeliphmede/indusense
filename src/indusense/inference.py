"""Inférence du type des colonnes."""

from typing import NamedTuple

import pandas as pd


IDENTITY_PATTERN = r"[A-Za-z]+-?\d{2,}|\d+"  # MACH-01, OP1002 ; ou chiffres restés en texte (zéro initial : 0012)
BOOLEAN_VALUES = {"0", "1", "0.0", "1.0", "true", "false"}
CATEGORY_MIN, CATEGORY_RATIO = 20, 0.01  # CATEGORY : au plus max(20, 1 % des lignes) valeurs distinctes
PARSE_THRESHOLD = 0.95  # part minimale de valeurs lisibles pour une colonne numérique, heure ou date
TZ_PATTERN = r"\d:\d{2}(?::\d{2}(?:\.\d+)?)?\s*(?:Z|[+-]\d{2}(?::?\d{2})?)$"  # heure suivie d'un fuseau
TIME_FORMATS = ["%H:%M", "%H:%M:%S", "%H:%M:%S.%f"]
DATE_FORMATS = ["ISO8601", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]  # jour avant mois prioritaire
TEMPORAL_FAMILIES = [("TIME", TIME_FORMATS)] + [  # testées dans l'ordre ; DATE devient DATETIME si heure
    ("DATE", [d] if d == "ISO8601" else [d, *(f"{d} {t}" for t in TIME_FORMATS)])  # ISO8601 lit déjà l'heure
    for d in DATE_FORMATS
]
NUMERIC_TYPES = {"INTEGER", "FLOAT"}
TEMPORAL_TYPES = {"DATE", "DATETIME", "TIME"}
STATS_TYPES = NUMERIC_TYPES | TEMPORAL_TYPES


class Temporal(NamedTuple):
    type: str          # TIME | DATE | DATETIME
    values: pd.Series  # valeurs converties, NaT si illisibles
    notes: list[str]   # remarques affichées dans la carte


def invalid_note(count: int) -> list[str]:
    """Remarque sur les valeurs illisibles, exclues des statistiques (liste vide s'il n'y en a pas)."""
    return [f"{count} invalid value{'s' if count > 1 else ''}"] if count else []


def to_datetime(text: pd.Series, formats: list[str]) -> tuple[pd.Series, float]:
    """Lit chaque valeur avec le premier format qui la reconnaît (NaT si aucun) ; retourne aussi la part lisible."""
    parsed = pd.to_datetime(text, format=formats[0], errors="coerce", utc=True)
    for fmt in formats[1:]:
        missing = parsed.isna()
        if not missing.any():
            break
        parsed = parsed.fillna(pd.to_datetime(text[missing], format=fmt, errors="coerce", utc=True))
    return parsed, parsed.notna().mean()


def parse_temporal(series: pd.Series) -> Temporal | None:
    """Lit la colonne avec la famille de formats qui reconnaît le plus de valeurs ;
    None si moins de PARSE_THRESHOLD des valeurs sont lisibles."""
    text = series.dropna().astype(str)
    best_ratio, best = 0.0, None
    for col_type, formats in TEMPORAL_FAMILIES:
        parsed, ratio = to_datetime(text, formats)
        if ratio > best_ratio:
            best_ratio, best = ratio, (col_type, formats, parsed)
        if ratio == 1:
            break
    if best_ratio < PARSE_THRESHOLD:
        return None

    col_type, formats, parsed = best
    valid, valid_text = parsed.dropna(), text[parsed.notna()]
    if col_type == "DATE" and ((valid != valid.dt.normalize()).any() or valid_text.str.len().max() > 10):
        col_type = "DATETIME"
    notes = invalid_note(int(parsed.isna().sum()))
    if formats[0].startswith(("%d", "%m")):  # JJ/MM ou MM/JJ : ambigu si l'ordre inverse lit autant de valeurs
        swapped = [f.replace("%d", "%_").replace("%m", "%d").replace("%_", "%m") for f in formats]
        if to_datetime(text, swapped)[1] == best_ratio:
            notes.append(f"ambiguous day/month, read as {'DD/MM' if formats[0].startswith('%d') else 'MM/DD'}")
    if col_type == "DATETIME" and valid_text.str.contains(TZ_PATTERN).any():
        notes.append("UTC")
    return Temporal(col_type, parsed, notes)


def infer_column_type(series: pd.Series) -> str:
    """Infère le type métier d'une colonne à partir de son nom et de ses valeurs."""
    name, values = str(series.name).lower(), series.dropna()
    if values.empty:
        return "EMPTY"
    if name == "id" or name.endswith(("_id", "_code")):
        return "IDENTITY"
    if {str(v).lower() for v in values.unique()} <= BOOLEAN_VALUES:
        return "BOOLEAN"
    if pd.api.types.is_numeric_dtype(values):
        return "INTEGER" if pd.api.types.is_integer_dtype(values) else "FLOAT"

    text = values.astype(str)
    if text.str.fullmatch(IDENTITY_PATTERN).all():
        return "IDENTITY"
    if temporal := parse_temporal(values):
        return temporal.type
    return "CATEGORY" if text.nunique() <= max(CATEGORY_MIN, CATEGORY_RATIO * len(values)) else "TEXT"


def type_notes(series: pd.Series, col_type: str, invalid: int = 0) -> list[str]:
    """Remarques de lecture d'une colonne : valeurs illisibles (invalid pour une colonne numérique),
    et pour une heure / date ambiguïté jour/mois et fuseau."""
    return parse_temporal(series).notes if col_type in TEMPORAL_TYPES else invalid_note(invalid)
