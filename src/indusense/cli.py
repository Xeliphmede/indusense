"""Ligne de commande.

- `indusense FICHIER [FICHIER ...]` : écrit la carte d'identité (et le profil JSON) de chaque fichier ;
- `indusense bronze validate [CONTRAT ...]` : valide les contrats bronze (par défaut `config/bronze/*.json`)
  contre leur schéma et les fichiers réels, sans rien lire ni écrire d'autre ; code de sortie 1 s'il y a
  un problème.
"""

import argparse
import sys
from pathlib import Path


def cards(argv: list[str]) -> int:
    from indusense.card import ARTIFACTS_DIR, write_card

    parser = argparse.ArgumentParser(
        prog="indusense",
        description=f"Écrit la carte d'identité Markdown (et le profil JSON) de chaque fichier (CSV ou script SQL) dans "
        f"{ARTIFACTS_DIR}/<AAAAMMJJ>/<nom_fichier>_<HHMMSS>.md et affiche son chemin. "
        f"Voir aussi : indusense bronze validate.",
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
            print(write_card(path))
        except Exception as error:  # un fichier illisible ne bloque pas les suivants
            print(f"indusense: {path} : {error}", file=sys.stderr)
            status = 1
    return status


def bronze(argv: list[str]) -> int:
    from indusense.bronze.contract import CONFIG_DIR, ContractError, load_contract
    from indusense.bronze.validation import ValidationIssue, validate_contracts, validate_directory

    parser = argparse.ArgumentParser(prog="indusense bronze", description="Ingestion bronze.")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser(
        "validate", help="valide les contrats bronze avant toute lecture ou écriture de données",
        description=f"Valide les contrats (par défaut {CONFIG_DIR}/*.json) : JSON Schema, cohérence interne et "
                    f"confrontation aux fichiers réels. Affiche tous les problèmes ; code de sortie 1 s'il y en a.")
    validate.add_argument("contracts", nargs="*", type=Path, metavar="CONTRAT", help="contrat JSON (défaut : tous)")
    args = parser.parse_args(argv)

    if args.contracts:
        contracts, issues = {}, []
        for path in args.contracts:
            try:
                contracts[path.name] = load_contract(path)
            except (ContractError, OSError) as error:
                issues.append(ValidationIssue(path.name, "(file)", 0, str(error)))
        issues += validate_contracts(contracts)
        count = len(args.contracts)
    else:
        issues = validate_directory(CONFIG_DIR)
        count = len(list(CONFIG_DIR.glob("*.json")))

    for issue in issues:
        print(f"indusense: {issue}", file=sys.stderr)
    if issues:
        print(f"{len(issues)} problème(s) dans {count} contrat(s)", file=sys.stderr)
        return 1
    print(f"{count} contrat(s) valide(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["bronze"]:
        return bronze(argv[1:])
    return cards(argv)


if __name__ == "__main__":
    sys.exit(main())
