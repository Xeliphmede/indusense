"""Ligne de commande.

- `indusense FICHIER [FICHIER ...]` : écrit la carte d'identité (et le profil JSON) de chaque fichier ;
- `indusense bronze validate [CONTRAT ...]` : valide les contrats bronze (par défaut `config/bronze/*.json`)
  contre leur schéma et les fichiers réels, sans rien lire ni écrire d'autre ; code de sortie 1 s'il y a
  un problème ;
- `indusense pipeline` : ingestion bronze des fichiers puis typage silver de ce batch ; code de sortie 1 si une
  étape échoue (les erreurs de cast sont listées) ;
- `indusense silver cast BATCH_ID` : (re)type en silver un batch bronze existant, par exemple après correction
  d'un contrat ;
- `indusense silver dictionary [--batch-id BATCH_ID]` : régénère le dictionnaire de données silver
  (docs/silver_dictionary.md) à partir des contrats et du dernier batch typé.
"""

import argparse
import sys
import uuid
from pathlib import Path


def cards(argv: list[str]) -> int:
    from indusense.card import ARTIFACTS_DIR, write_card

    parser = argparse.ArgumentParser(
        prog="indusense",
        description=f"Écrit la carte d'identité Markdown (et le profil JSON) de chaque fichier (CSV ou script SQL) dans "
        f"{ARTIFACTS_DIR}/<AAAAMMJJ>/<nom_fichier>_<HHMMSS>.md et affiche son chemin. "
        f"Voir aussi : indusense bronze validate, indusense pipeline, indusense silver cast.",
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


def print_results(title: str, results) -> None:
    print(title)
    for r in results:
        print(f"  {r.target_table:28} {r.status:12} {r.rows_inserted:>8} ligne(s)" + (f"  {r.error}" if r.error else ""))


def print_cast_errors(errors) -> None:
    for error in errors:
        print(f"indusense: {error}", file=sys.stderr)
    if errors:
        print(f"{len(errors)} erreur(s) de cast : rien n'a été écrit en silver", file=sys.stderr)


def pipeline(argv: list[str]) -> int:
    argparse.ArgumentParser(prog="indusense pipeline",
                            description="Ingestion bronze des fichiers (contrats config/bronze/*.json), puis typage "
                                        "silver de ce batch. Le schéma doit être à jour (alembic upgrade head).",
                            ).parse_args(argv)
    from indusense.pipeline import SchemaOutOfDate, run_pipeline

    try:
        result = run_pipeline()
    except SchemaOutOfDate as error:
        print(f"indusense: {error}", file=sys.stderr)
        return 1
    print(f"batch_id : {result.batch_id}")
    print_results("bronze", result.bronze)
    if result.silver:
        print_results("silver", result.silver)
        print(f"rapport des traitements : {result.silver[0].report}")
    print_cast_errors(result.cast_errors)
    return 0 if result.succeeded else 1


def silver(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="indusense silver", description="Couche silver.")
    commands = parser.add_subparsers(dest="command", required=True)
    cast = commands.add_parser("cast", help="type en silver un batch bronze existant",
                               description="Type le batch bronze BATCH_ID dans les tables silver (remplace les lignes "
                                           "silver déjà issues de ce batch). Code de sortie 1 en cas d'erreur.")
    cast.add_argument("batch_id", type=uuid.UUID, metavar="BATCH_ID", help="batch_id de l'ingestion bronze")
    dictionary = commands.add_parser("dictionary", help="régénère le dictionnaire de données silver",
                                     description="Écrit docs/silver_dictionary.md : description, type, unité et "
                                                 "statistiques de chaque colonne silver, à partir des contrats et "
                                                 "d'un batch typé (par défaut le dernier).")
    dictionary.add_argument("--batch-id", type=uuid.UUID, metavar="BATCH_ID", help="batch décrit (défaut : dernier)")
    args = parser.parse_args(argv)

    from sqlalchemy import create_engine

    from indusense.bronze.contract import load_contracts
    from indusense.settings import get_settings
    from indusense.silver.loader import BatchNotReady, cast_batch

    if args.command == "dictionary":
        from indusense.silver.dictionary import write_dictionary

        print(write_dictionary(load_contracts(), create_engine(get_settings().url()), batch_id=args.batch_id))
        return 0

    try:
        results = cast_batch(load_contracts(), create_engine(get_settings().url()), args.batch_id)
    except BatchNotReady as error:
        print(f"indusense: {error}", file=sys.stderr)
        return 1
    print_results("silver", results)
    print(f"rapport des traitements : {results[0].report}")
    print_cast_errors([e for r in results for e in r.errors])
    return 0 if all(r.status == "SUCCESS" for r in results) else 1


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["bronze"]:
        return bronze(argv[1:])
    if argv[:1] == ["pipeline"]:
        return pipeline(argv[1:])
    if argv[:1] == ["silver"]:
        return silver(argv[1:])
    return cards(argv)


if __name__ == "__main__":
    sys.exit(main())
