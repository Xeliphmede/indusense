"""Ligne de commande : `indusense FICHIER [FICHIER ...]` écrit la carte d'identité de chaque fichier."""

import argparse
import sys
from pathlib import Path

from indusense import describer, write_card
from indusense.card import ARTIFACTS_DIR


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="indusense",
        description=f"Écrit la carte d'identité Markdown de chaque fichier (CSV ou script SQL) dans "
        f"{ARTIFACTS_DIR}/<AAAAMMJJ>/<nom_fichier>_<HHMMSS>.md et affiche son chemin.",
    )
    parser.add_argument("files", nargs="+", type=Path, metavar="FICHIER", help="fichier CSV ou script SQL")
    args = parser.parse_args(argv)

    status = 0
    for path in args.files:
        if not path.is_file():
            print(f"indusense: {path} : fichier introuvable", file=sys.stderr)
            status = 1
            continue
        try:
            print(write_card(path, describer(path)))
        except Exception as error:  # un fichier illisible ne bloque pas les suivants
            print(f"indusense: {path} : {error}", file=sys.stderr)
            status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
