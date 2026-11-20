"""Statistiques des colonnes numériques et dates."""

import pandas as pd

from indusense.inference import NUMERIC_TYPES, parse_temporal


def column_stats(series: pd.Series, col_type: str) -> list[str]:
    """Retourne [min, max, mean, median] formatés ; mean et median valent "-" hors numériques."""
    values = series.dropna()
    if col_type in NUMERIC_TYPES:
        fmt = "{}" if col_type == "INTEGER" else "{:.3f}"
        return [fmt.format(values.min()), fmt.format(values.max()), f"{values.mean():.3f}", f"{values.median():.3f}"]
    temporal = parse_temporal(values)
    dates = temporal.values.dropna()  # valeurs illisibles exclues
    if dates.dt.microsecond.any():
        time_format = "%H:%M:%S.%f"
    elif col_type == "DATETIME" or dates.dt.second.any():
        time_format = "%H:%M:%S"
    else:
        time_format = "%H:%M"
    date_format = {"DATE": "%Y-%m-%d", "DATETIME": f"%Y-%m-%d {time_format}", "TIME": time_format}[col_type]
    suffix = " UTC" if "UTC" in temporal.notes else ""

    def fmt(ts: pd.Timestamp) -> str:
        text = ts.strftime(date_format)
        return (text[:-3] if date_format.endswith("%f") else text) + suffix  # %f : microsecondes -> millisecondes

    return [fmt(dates.min()), fmt(dates.max()), "-", "-"]
