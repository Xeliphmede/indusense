"""Dédoublonnage silver sur la clé naturelle du contrat (ADR-004).

Le bronze garde toutes les lignes du fichier ; le silver garde **une ligne par clé naturelle** (`natural_key` du
contrat) et par batch. Quand plusieurs lignes typées partagent la même clé :

1. on garde la ligne **la plus complète** (le moins de valeurs NULL dans les colonnes du contrat) ;
2. à égalité, la **première du fichier** (plus petit `_bronze_id`, donc plus petit `_row_number`).

La ligne gardée est recopiée telle quelle : aucune valeur n'est fusionnée ni moyennée, chaque valeur silver reste
une valeur exacte du bronze. Les lignes écartées sont consignées dans `silver._duplicate` (ligne bronze écartée,
ligne bronze gardée, clé, motif) : rien ne disparaît sans trace.

La clé est comparée sur les valeurs **typées** : `2025-06-01 00:00:00` et `2025-06-01 00:00:00+00` sont le même
instant, donc la même clé. Une ligne dont la clé contient un NULL n'est jamais dédoublonnée.
"""

import json

RULE = "même clé naturelle : gardée la ligne la plus complète (le moins de NULL), puis la première du fichier"


def key_text(contract: dict, values: tuple) -> str:
    return json.dumps(dict(zip(contract["target"]["natural_key"], values)), default=str, ensure_ascii=False)


def deduplicate(contract: dict, records: list[dict]) -> tuple[list[dict], list[dict]]:
    """(enregistrements gardés, lignes écartées) ; `records` est dans l'ordre du fichier (`_bronze_id`)."""
    key = contract["target"].get("natural_key") or []
    if not key:
        return records, []
    columns = [entry["target_name"] for entry in contract["columns"]]
    missing = [sum(record[c] is None for c in columns) for record in records]
    groups: dict[tuple, list[int]] = {}
    for position, record in enumerate(records):
        values = tuple(record[c] for c in key)
        if None not in values:
            groups.setdefault(values, []).append(position)

    dropped, positions_dropped = [], set()
    for values, positions in groups.items():
        if len(positions) < 2:
            continue
        best = min(positions, key=lambda p: (missing[p], records[p]["_bronze_id"]))
        for position in positions:
            if position != best:
                positions_dropped.add(position)
                dropped.append({
                    "bronze_id": records[position]["_bronze_id"], "kept_bronze_id": records[best]["_bronze_id"],
                    "natural_key": key_text(contract, values), "missing_values": missing[position],
                    "kept_missing_values": missing[best], "reason": RULE,
                })
    kept = [record for position, record in enumerate(records) if position not in positions_dropped]
    return kept, sorted(dropped, key=lambda d: d["bronze_id"])
