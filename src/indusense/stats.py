"""Statistiques des colonnes numériques et dates."""

import pandas as pd

from indusense.inference import NUMERIC_TYPES, parse_temporal


def column_stats(series: pd.Series, col_type: str) -> dict:
    """Statistiques sérialisables en JSON : {min, max, mean, median} (nombres) pour une colonne numérique,
    {min, max} (texte AAAA-MM-JJ, HH:MM[:SS[.fff]]) pour une heure ou une date ; valeurs illisibles exclues."""
    values = series.dropna()
    if col_type in NUMERIC_TYPES:
        cast = int if col_type == "INTEGER" else float
        return {
            "min": cast(values.min()),
            "max": cast(values.max()),
            "mean": float(values.mean()),
            "median": float(values.median()),
        }
    dates = parse_temporal(values).values.dropna()
    if dates.dt.microsecond.any():
        time_format = "%H:%M:%S.%f"
    elif col_type == "DATETIME" or dates.dt.second.any():
        time_format = "%H:%M:%S"
    else:
        time_format = "%H:%M"
    date_format = {"DATE": "%Y-%m-%d", "DATETIME": f"%Y-%m-%d {time_format}", "TIME": time_format}[col_type]

    def fmt(ts: pd.Timestamp) -> str:
        text = ts.strftime(date_format)
        return text[:-3] if date_format.endswith("%f") else text  # %f : microsecondes -> millisecondes

    return {"min": fmt(dates.min()), "max": fmt(dates.max())}
