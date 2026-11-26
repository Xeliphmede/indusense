"""InduSense : profil (JSON) et carte d'identité (Markdown) des fichiers de données (CSV et scripts SQL),
ingestion bronze dans PostgreSQL (sous-paquet `indusense.bronze`).

`import indusense` ne charge rien de lourd (ni pandas, ni parseur SQL, ni driver PostgreSQL) : les fonctions
exportées sont importées à leur premier usage.
"""

from importlib import import_module

__all__ = ["profile_file", "render_card", "write_card"]

_EXPORTS = {"profile_file": "indusense.profile", "render_card": "indusense.card", "write_card": "indusense.card"}


def __getattr__(name: str):
    if name in _EXPORTS:
        return getattr(import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module 'indusense' has no attribute {name!r}")
