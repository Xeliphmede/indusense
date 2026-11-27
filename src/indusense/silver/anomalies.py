"""Indicateurs d'anomalies des tables silver, pour les visualiser (silver.ipynb) : rien n'est modifié.

- valeurs aberrantes au sens de Tukey : hors de [Q1 - 1,5 IQR, Q3 + 1,5 IQR], les points isolés d'une boîte à
  moustaches ;
- valeurs collées aux bornes : plusieurs valeurs exactement égales au minimum ou au maximum signalent un capteur
  saturé ou une valeur plafonnée (une mesure continue ne retombe pas souvent sur le même nombre) ;
- doublons de clé naturelle (contrat : `natural_key`).
"""

import pandas as pd

TUKEY_K = 1.5


def tukey_bounds(series: pd.Series, k: float = TUKEY_K) -> tuple[float, float]:
    q1, q3 = series.quantile([0.25, 0.75])
    return q1 - k * (q3 - q1), q3 + k * (q3 - q1)


def anomaly_summary(frame: pd.DataFrame, columns: list[str], k: float = TUKEY_K) -> pd.DataFrame:
    """Une ligne par colonne numérique : volumes, quartiles, bornes de Tukey, aberrants, valeurs aux bornes."""
    rows = []
    for column in columns:
        values = pd.to_numeric(frame[column]).astype(float)
        present = values.dropna()
        low, high = tukey_bounds(present, k)
        outliers = present[(present < low) | (present > high)]
        rows.append({
            "colonne": column, "valeurs": len(present), "NULL": int(values.isna().sum()),
            "min": present.min(), "Q1": present.quantile(0.25), "médiane": present.median(),
            "Q3": present.quantile(0.75), "max": present.max(), "borne basse": low, "borne haute": high,
            "aberrantes": len(outliers), "aberrantes %": round(100 * len(outliers) / len(present), 2) if len(present) else 0.0,
            "au minimum": int((present == present.min()).sum()), "au maximum": int((present == present.max()).sum()),
        })
    return pd.DataFrame(rows).set_index("colonne")


def duplicate_keys(frame: pd.DataFrame, key: list[str]) -> int:
    """Nombre de lignes en trop : lignes dont la clé naturelle a déjà été vue."""
    return int(frame.duplicated(key).sum())
