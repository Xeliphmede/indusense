# InduSense

Projet de formation IA : ingestion de données industrielles (télémétrie machines, incidents, maintenance).

**Sprint 1** : générer une *carte d'identité* Markdown pour chaque fichier de données : taille, charset,
dimensions, types de colonnes inférés et statistiques des colonnes numériques et dates.

**Ingestion bronze** : charger les fichiers tels quels dans le schéma `bronze` de PostgreSQL, à partir d'un
contrat JSON généré depuis le profil de la carte d'identité (voir `bronze.ipynb`).

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

### Ingestion bronze

1. Créer `.env` à partir du modèle (`cp .env.example .env`, puis remplacer chaque `change-me`) et démarrer
   PostgreSQL : `sudo docker compose up -d db admin` (pgAdmin sur
   http://localhost:8081).
2. Ouvrir `bronze.ipynb` et tout exécuter. Le notebook enchaîne :
   - le profil JSON des sources ;
   - les décisions métier (clé naturelle, données personnelles, opérations silver) ;
   - les contrats `config/bronze/*.json` ;
   - la validation (10 règles) ;
   - la migration Alembic ;
   - le chargement et le rejeu ;
   - les contrôles SQL ;
   - les rapports `artifacts/ingestion/bronze/` ;
   - les tests.

Le bronze ne convertit rien : chaque colonne source devient une colonne `TEXT` qui reçoit la valeur d'origine
(un champ vide reste `''`, `NULL` reste `NULL`), suivie de `<col>_type`, le type déclaré par le contrat. La colonne
`hash` porte l'empreinte de la ligne : SHA-256 du tableau JSON des valeurs stockées des seules colonnes du fichier
(`src/indusense/bronze/fingerprint.py`). Le contrat déclare le type visé (`type`, `silver_type`), appliqué
par la couche silver (`src/indusense/silver/casting.py`).

Seule exception : les données personnelles marquées `pseudonymize` dans le contrat (`operator_name`,
`operator_badge`) ne sont jamais écrites en clair. Le bronze reçoit `HMAC-SHA256(ANONYMISATION_SALT, valeur)` :
un même opérateur garde le même pseudonyme, sans que son nom soit stocké. Le sel (`ANONYMISATION_SALT` dans
`.env`) est un secret : sans lui le chargement échoue, et le changer change tous les pseudonymes.

Chaque exécution d'ingestion a un `batch_id` (UUID) et une heure (`_ingested_at`), communs aux quatre tables. Elle
insère toutes les lignes de ses fichiers (historique en ajout seul, clé primaire `_id` auto-générée) :
`WHERE _batch_id = ...` retrouve ses lignes dans chaque table, et le registre `bronze._ingestion_batch` dit quels
fichiers elle a utilisés (chemin et SHA-256). Une exécution est atomique : au moindre échec, aucune table ne
garde son batch.

Si un contrat change le DDL, la cellule de migration échoue sur `command.check` : générer la migration
(`uv run alembic revision --autogenerate -m "..."`), la relire, la committer, puis relancer.

La validation des contrats existe aussi en ligne de commande (code de sortie 1 s'il y a un problème) :

```shell
uv run indusense bronze validate
```

### Tests

```shell
uv run pytest            # les tests d'intégration créent puis suppriment une base indusense_test
uv run pytest -m "not integration"
```

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
  settings.py              configuration lue dans .env (URL PostgreSQL)
  bronze/
    contract.py            contrat bronze : brouillon depuis le profil, lecture, écriture
    probes.py              description de la source réelle (en-tête CSV, dump SQL)
    validation.py          les 10 règles de validation des contrats
    schema.py              métadonnée SQLAlchemy du schéma bronze, configuration Alembic
    readers.py             lecture en flux des lignes brutes
    loader.py              chargement idempotent, registre des lots
    pseudonymization.py    pseudonymisation HMAC-SHA256 salée des données personnelles
    fingerprint.py         empreinte SHA-256 d'une ligne (colonne hash)
    reports.py             rapport d'ingestion et dictionnaire de données
  silver/
    casting.py             conversion au mieux des valeurs texte vers leur type (couche silver)
config/
  bronze/*.json            contrats d'ingestion (un par table cible)
  schema/                  JSON Schema du contrat v2
migrations/                migrations Alembic du schéma bronze
alembic.ini                configuration Alembic (sans identifiants)
  cli.py                   ligne de commande indusense
tests/                     tests pytest (uv run pytest)
indusense.ipynb            explications et génération des cartes
bronze.ipynb               ingestion bronze de bout en bout
.claude/skills/            skill Claude Code carte-identite
artifacts/                 cartes générées (non versionnées)
```

## Dépendances

| Paquet | Rôle |
| --- | --- |
| `pandas` | lecture et analyse des données |
| `sqlglot` | analyse du script SQL sans base de données (choix justifié dans le notebook) |
| `ipykernel` (dev) | exécution du notebook |
| `sqlalchemy`, `psycopg[binary]` | accès PostgreSQL |
| `alembic` | migrations du schéma bronze |
| `pydantic-settings` | configuration lue dans `.env` |
| `jsonschema` | validation des contrats |
| `pytest` (dev) | tests |

## Contribution

- Ne jamais travailler sur `main` : une branche par incrément (`feat/...`, `fix/...`, `docs/...`).
- Messages de commit au format [Conventional Commits](https://www.conventionalcommits.org/).
- Fusionner dans `main`, puis pousser uniquement `main`.
- Réexécuter le notebook avant de le committer pour que ses sorties restent à jour.
