"""Empreinte d'une ligne bronze : SHA-256 des valeurs des colonnes du fichier source.

Seules les colonnes du fichier entrent dans l'empreinte, dans l'ordre du contrat, avec leur valeur **telle que
stockée** en bronze (pseudonyme pour les colonnes `pseudonymize`) : l'empreinte se recalcule depuis la base seule
et ne révèle aucune donnée personnelle. Les colonnes ajoutées (`<col>_type`, `hash`, colonnes techniques) n'y
entrent pas.

Sérialisation : tableau JSON compact des valeurs, `null` pour NULL. Elle est sans ambiguïté : `''` et NULL
diffèrent, et une valeur contenant un séparateur ne peut pas se confondre avec deux valeurs.

    ["MACH-01","2025-06-01 00:00:00","46.348","198.203","227.568","1541.787","4"]  ->  sha256 hex (64 caractères)
"""

import hashlib
import json
from collections.abc import Sequence


def row_fingerprint(values: Sequence[str | None]) -> str:
    """SHA-256 (hexadécimal) des valeurs d'une ligne, dans l'ordre des colonnes du contrat."""
    payload = json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
