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

Les cartes sont écrites dans `artifacts/ingestion/identities/<AAAAMMJJ>/<fichier>_<HHMMSS>.md`
(dossier ignoré par git), relatif au dossier courant, accompagnées du **profil JSON** du fichier
(`<fichier>_<HHMMSS>.json`) : les mêmes informations, lisibles par un programme, qui servent de base au contrat
d'ingestion bronze.

### Ligne de commande

Depuis la racine du projet, pour un ou plusieurs fichiers CSV (`.csv`, `.tsv`) ou scripts SQL (`.sql`, `.sql.txt`) :

```shell
uv run indusense datas/telemetry.csv
uv run indusense datas/releves_incidents.csv datas/machine.sql.txt
```

La commande affiche le chemin de chaque carte écrite. Les erreurs (fichier introuvable, format non pris en
charge...) sont affichées sur la sortie d'erreur sans bloquer les autres fichiers ; le code de sortie vaut alors 1.

### Notebook

Ouvrir `indusense.ipynb` dans VS Code, choisir le noyau de l'environnement `indusense`, puis tout exécuter :
il explique la démarche et génère les cartes des trois fichiers de `datas/`. Il est versionné avec ses sorties :
les cartes sont visibles sur GitHub sans l'exécuter.

### Skill Claude Code

Le skill `carte-identite` (`.claude/skills/carte-identite/`) appelle la ligne de commande puis résume la carte.
Dans Claude Code, lancé à la racine du projet : `/carte-identite datas/telemetry.csv`, ou simplement
« fais la carte d'identité de datas/telemetry.csv ».

## Structure

```text
datas/
  telemetry.csv            télémétrie horaire des machines
  releves_incidents.csv    relevés d'incidents opérateurs
  machine.sql.txt          référentiel machines et maintenance (script PostgreSQL)
  TODO-SPRINT1_1.md        consignes du sprint
src/indusense/
  reading.py               taille, charset, séparateurs ; lecture et nettoyage d'un CSV en texte
  inference.py             inférence du type des colonnes
  stats.py                 statistiques des colonnes numériques et dates
  sql.py                   lecture d'un script SQL (types déclarés, INSERT)
  profile.py               profil JSON d'un fichier
  card.py                  rendu Markdown du profil, écriture de la carte et du profil
  cli.py                   ligne de commande indusense
tests/                     tests pytest (uv run pytest)
indusense.ipynb            explications et génération des cartes
.claude/skills/            skill Claude Code carte-identite
artifacts/                 cartes générées (non versionnées)
```

## Dépendances

| Paquet | Rôle |
| --- | --- |
| `pandas` | lecture et analyse des données |
| `sqlglot` | analyse du script SQL sans base de données (choix justifié dans le notebook) |
| `ipykernel` (dev) | exécution du notebook |
| `pytest` (dev) | tests |

## Contribution

- Ne jamais travailler sur `main` : une branche par incrément (`feat/...`, `fix/...`, `docs/...`).
- Messages de commit au format [Conventional Commits](https://www.conventionalcommits.org/).
- Fusionner dans `main`, puis pousser uniquement `main`.
- Réexécuter le notebook avant de le committer pour que ses sorties restent à jour.
