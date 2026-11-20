# InduSense

Projet de formation IA : ingestion de données industrielles (télémétrie machines, incidents, maintenance).

**Sprint 1** : générer une *carte d'identité* Markdown pour chaque fichier de données : taille, charset,
dimensions, types de colonnes inférés et statistiques des colonnes numériques et dates.

## Prérequis

- [uv](https://docs.astral.sh/uv/)
- Python 3.15 (`uv python install 3.15`)
- VS Code avec les extensions Python (`ms-python.python`) et Jupyter (`ms-toolsai.jupyter`)

## Installation

```shell
git clone git@github.com:Xeliphmede/indusense.git
cd indusense
uv sync
```

## Utilisation

Ouvrir `indusense.ipynb` dans VS Code, choisir le noyau de l'environnement `indusense`, puis tout exécuter.

Les cartes sont écrites dans `artifacts/ingestion/identities/<AAAAMMJJ>/<fichier>_<HHMMSS>.md`
(dossier ignoré par git). Le notebook est versionné avec ses sorties : les cartes sont visibles sur GitHub
sans l'exécuter.

## Structure

```text
datas/
  telemetry.csv            télémétrie horaire des machines
  releves_incidents.csv    relevés d'incidents opérateurs
  machine.sql.txt          référentiel machines et maintenance (script PostgreSQL)
  TODO-SPRINT1_1.md        consignes du sprint
indusense.ipynb            génération des cartes d'identité
artifacts/                 cartes générées (non versionnées)
```

## Dépendances

| Paquet | Rôle |
| --- | --- |
| `pandas` | lecture et analyse des données |
| `sqlglot` | analyse du script SQL sans base de données (choix justifié dans le notebook) |
| `ipykernel` (dev) | exécution du notebook |

## Contribution

- Ne jamais travailler sur `main` : une branche par incrément (`feat/...`, `fix/...`, `docs/...`).
- Messages de commit au format [Conventional Commits](https://www.conventionalcommits.org/).
- Fusionner dans `main`, puis pousser uniquement `main`.
- Réexécuter le notebook avant de le committer pour que ses sorties restent à jour.
