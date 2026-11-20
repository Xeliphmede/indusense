"""Construction de la carte d'identité ; lecture et description d'un fichier CSV."""

from datetime import datetime
from pathlib import Path

import pandas as pd

from indusense.inference import PARSE_THRESHOLD, STATS_TYPES, infer_column_type, type_label
from indusense.reading import detect_charset, detect_separator, format_filesize
from indusense.stats import column_stats

ARTIFACTS_DIR = Path("artifacts/ingestion/identities")


def md_table(headers: list[str], rows: list[list]) -> list[str]:
    """Lignes d'un tableau Markdown."""
    return [
        f"| {' | '.join(headers)} |",
        "|" + " --- |" * len(headers),
        *(f"| {' | '.join(map(str, row))} |" for row in rows),
    ]


def describe_frame(
    df: pd.DataFrame, sql_types: dict | None = None, level: str = "##", invalid: dict | None = None
) -> list[str]:
    """Dimensions, types inférés (et types SQL déclarés) et statistiques d'un DataFrame ;
    invalid : nombre de valeurs illisibles par colonne numérique (voir parse_numbers)."""
    invalid = invalid or {}
    types = {col: infer_column_type(df[col]) for col in df.columns}
    labels = {col: type_label(df[col], t, invalid.get(col, 0)) for col, t in types.items()}
    if sql_types:  # colonnes déclarées, y compris celles absentes des données (valeur par défaut)
        columns = list(sql_types) + [col for col in types if col not in sql_types]
        column_list = md_table(
            ["Colname", "SQL Type", "Inferred Type"],
            [[col, sql_types.get(col, "-"), labels.get(col, "-")] for col in columns],
        )
    else:
        columns = list(types)
        column_list = md_table(["Colname", "Inferred Type"], labels.items())
    stats = [[col, *column_stats(df[col], t)] for col, t in types.items() if t in STATS_TYPES]
    return [
        f"- Rows Number: {len(df)}",
        f"- Cols Number: {len(columns)}",
        "", f"{level} Column list", "", *column_list,
        "", f"{level} Numeric and date columns", "",
        *md_table(["Colname", "Min value", "Max value", "Mean value", "Median value"], stats),
    ]


NULL_VALUES = {"", "null"}  # valeurs manquantes, casse ignorée (pandas en reconnaît bien plus : NA, None, n/a...)
DECIMAL_PATTERN = r"[+-]?(?:\d+(?:[.,]\d+)?|[.,]\d+)(?:[eE][+-]?\d+)?"  # 12 | 12.5 | 12,5 | -3,25 | .5 | 1e-3
LEADING_ZERO_PATTERN = r"[+-]?0\d"  # 0012 : un code, pas un nombre


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Retire les espaces autour des valeurs texte ; vide ou null (casse ignorée) devient une valeur manquante."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            text = df[col].str.strip()
            df[col] = text.mask(text.str.lower().isin(NULL_VALUES))
    return df


def parse_numbers(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Convertit en nombres (point ou virgule décimale) les colonnes texte dont au moins PARSE_THRESHOLD des
    valeurs sont des nombres ; les autres valeurs deviennent manquantes. Une colonne sans décimale reste entière
    (Int64) même avec des valeurs manquantes. Retourne aussi le nombre de valeurs illisibles par colonne convertie."""
    invalid = {}
    for col in df.columns:
        text = df[col].dropna().astype(str)
        if pd.api.types.is_numeric_dtype(df[col]) or text.empty:
            continue
        is_number = text.str.fullmatch(DECIMAL_PATTERN)
        if is_number.mean() < PARSE_THRESHOLD or text.str.match(LEADING_ZERO_PATTERN).any():
            continue
        numbers = pd.to_numeric(text[is_number].str.replace(",", ".", regex=False))
        if pd.api.types.is_integer_dtype(numbers):
            numbers = numbers.astype("Int64")  # int64 passerait en float à cause des valeurs manquantes
        df[col] = numbers.reindex(df.index)
        invalid[col] = int((~is_number).sum())
    return df, invalid


def describe_csv(path: Path, encoding: str) -> list[str]:
    """Corps de la carte d'identité d'un fichier CSV, lu entièrement en texte : les types sont inférés ici."""
    sep = detect_separator(path, encoding)
    raw = pd.read_csv(path, sep=sep, encoding=encoding, dtype=str, na_filter=False, skipinitialspace=True)
    df, invalid = parse_numbers(clean_text(raw))
    return describe_frame(df, invalid=invalid)


def write_card(path: Path, describe) -> Path:
    """Écrit la carte dans ARTIFACTS_DIR/<AAAAMMJJ>/<nom_fichier>_<HHMMSS>.md et retourne son chemin."""
    begin = datetime.now()
    charset, encoding = detect_charset(path)
    body = describe(path, encoding)
    end = datetime.now()
    lines = [
        f"# Carte d'identité : {path.name}",
        "",
        f"- Source: {path.resolve()}",
        f"- Filesize: {format_filesize(path.stat().st_size)}",
        f"- ProcessBeginAt: {begin:%H:%M:%S}",
        f"- ProcessEndAt: {end:%H:%M:%S}",
        f"- Charset: {charset}",
        *body,
    ]
    output = ARTIFACTS_DIR / f"{begin:%Y%m%d}" / f"{path.name.split('.')[0]}_{begin:%H%M%S}.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
