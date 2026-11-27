# InduSense

Projet de formation IA : ingestion de données industrielles (télémétrie machines, incidents, maintenance).

**Sprint 1** : générer une *carte d'identité* Markdown pour chaque fichier de données : taille, charset,
dimensions, types de colonnes inférés et statistiques des colonnes numériques et dates.

**Ingestion bronze** : charger les fichiers tels quels dans le schéma `bronze` de PostgreSQL, à partir d'un
contrat JSON généré depuis le profil de la carte d'identité (voir `bronze.ipynb`).

**Couche silver** : typer chaque colonne du bronze selon le contrat dans le schéma `silver`, chaque ligne reliée à
son batch et à sa ligne bronze (voir `silver.ipynb`).

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

### Pipeline et couche silver

Le pipeline enchaîne l'ingestion bronze et le typage silver de ce même batch :

```shell
uv run alembic upgrade head      # schémas bronze et silver à jour (le pipeline ne migre jamais lui-même)
uv run indusense pipeline        # bronze puis silver ; code de sortie 1 si une étape échoue
```

`silver.ipynb` fait de même, puis contrôle le résultat (volumes, valeurs absentes, traçabilité, types).

Le schéma `silver` contient une table par contrat. Chaque colonne y est convertie vers le type du contrat
(`type`, puis `silver_type` : longueur `VARCHAR`, précision `NUMERIC`, `NOT NULL` si `nullable: false`) ; un vide
du bronze devient `NULL`, un horodatage sans fuseau est étiqueté UTC (ADR-003). Chaque ligne garde `_batch_id`
(l'exécution d'ingestion bronze) et `_bronze_id`, clé primaire et clé étrangère vers `bronze.<table>._id` :
une jointure retrouve le fichier, le numéro de ligne et la valeur d'origine. Le registre `silver._cast_run`
consigne chaque exécution silver.

Toutes les lignes sont typées avant la moindre écriture. **Une seule erreur de cast** (valeur non convertible ou
absente dans une colonne non nullable) : chaque erreur est loguée (logger `indusense.silver`, affichée par le
notebook et la ligne de commande), rien n'est écrit en silver, le registre dit `FAILED` / `ROLLED_BACK`. Le batch
bronze est conservé : corriger le contrat, puis le retyper sans recharger les fichiers (les lignes silver déjà
issues de ce batch sont remplacées) :

```shell
uv run indusense silver cast <batch_id>
```

**Doublons (ADR-004).** Le silver garde une ligne par clé naturelle du contrat (`natural_key`) et par batch,
garantie par une contrainte d'unicité. Entre lignes de même clé, il garde la plus complète (le moins de `NULL`),
puis la première du fichier, recopiée telle quelle (rien n'est fusionné ni moyenné). Chaque ligne écartée est
consignée dans `silver._duplicate` avec la ligne gardée à sa place. Seule la télémétrie est concernée :
1 346 lignes en trop sur 135 626, des mesures réémises aussitôt avec quelques centièmes d'écart
(`docs/adr/004-dedoublonnage-silver.md`).

**Aucune perte par rapport au bronze.** Le cast refuse ce qu'un type ne peut pas restituer (texte trop long pour
un `VARCHAR`, décimales en trop pour un `NUMERIC`, nombre qu'un `DOUBLE PRECISION` n'écrit pas exactement). Puis,
avant le commit, chaque table silver est relue telle que PostgreSQL l'a stockée et rapprochée du bronze par un
schéma [pandera](https://pandera.readthedocs.io/) généré à partir du contrat
(`src/indusense/silver/fidelity.py`) : chaque ligne bronze a sa ligne silver (ou est consignée comme doublon d'une
ligne silver de même clé), chaque valeur silver restitue
exactement la valeur bronze (texte caractère pour caractère, nombre comparé à son décimal exact, `NULL` si et
seulement si vide). Une seule différence et la transaction est annulée, comme pour une erreur de cast.

**Rapport des traitements.** Chaque exécution silver, réussie ou non, écrit un rapport Markdown daté :
`artifacts/ingestion/silver/<AAAAMMJJ>/silver_<HHMMSS>_<run_id>.md` (chemin affiché par la commande). Il donne le
début et la fin (UTC), le batch bronze, la taille des lots d'écriture (`batch_size`), le commit du code, la
version et l'empreinte de chaque contrat, puis, pour chaque colonne, ses volumes (valeurs typées, `NULL`,
erreurs) et la liste ordonnée des traitements appliqués, et enfin les erreurs.

**Répartition et anomalies.** `silver.ipynb` (section 7) trace, sur le batch typé, l'histogramme et la boîte à
moustaches de chaque mesure, une boîte par machine, la production rapportée à la capacité déclarée, et une
synthèse : valeurs aberrantes (bornes de Tukey), valeurs collées aux bornes (capteur saturé), doublons de clé
naturelle (`src/indusense/silver/anomalies.py`). Rien n'y est corrigé : c'est l'état des lieux avant les règles
de nettoyage.

Les colonnes dérivées déclarées par les contrats (`date_time`, `comment_normalized`) ne sont pas encore produites.

### Tests

```shell
uv run pytest            # les tests d'intégration créent puis suppriment indusense_test et indusense_test_silver
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
    casting.py             conversion des valeurs texte vers le type du contrat (type, silver_type, nullable)
    schema.py              métadonnée SQLAlchemy du schéma silver (tables typées, registre _cast_run)
    loader.py              typage d'un batch bronze en silver, tout ou rien
    dedup.py               dédoublonnage sur la clé naturelle (ADR-004)
    fidelity.py            relecture du silver et rapprochement avec le bronze (pandera) : aucune perte
    report.py              rapport Markdown daté d'une exécution : traitements de chaque colonne, batch_size
    anomalies.py           indicateurs d'anomalies (Tukey, valeurs aux bornes, doublons de clé)
  pipeline.py              pipeline bronze puis silver, sous un même batch_id
config/
  bronze/*.json            contrats d'ingestion (un par table cible)
  schema/                  JSON Schema du contrat v2
migrations/                migrations Alembic des schémas bronze et silver
docs/adr/                  décisions d'architecture (004 : dédoublonnage silver)
alembic.ini                configuration Alembic (sans identifiants)
  cli.py                   ligne de commande indusense
tests/                     tests pytest (uv run pytest)
indusense.ipynb            explications et génération des cartes
bronze.ipynb               ingestion bronze de bout en bout
silver.ipynb               pipeline bronze puis silver, contrôles du typage
.claude/skills/            skill Claude Code carte-identite
artifacts/                 cartes générées (non versionnées)
```

## Dépendances

| Paquet | Rôle |
| --- | --- |
| `pandas` | lecture et analyse des données |
| `sqlglot` | analyse du script SQL sans base de données (choix justifié dans le notebook) |
| `ipykernel` (dev) | exécution du notebook |
| `matplotlib` (dev) | histogrammes et boîtes à moustaches de `silver.ipynb` |
| `sqlalchemy`, `psycopg[binary]` | accès PostgreSQL |
| `alembic` | migrations du schéma bronze |
| `pydantic-settings` | configuration lue dans `.env` |
| `jsonschema` | validation des contrats |
| `pandera` | validation du silver relu : aucune ligne ni valeur perdue par rapport au bronze |
| `pytest` (dev) | tests |

## Contribution

- Ne jamais travailler sur `main` : une branche par incrément (`feat/...`, `fix/...`, `docs/...`).
- Messages de commit au format [Conventional Commits](https://www.conventionalcommits.org/).
- Fusionner dans `main`, puis pousser uniquement `main`.
- Réexécuter le notebook avant de le committer pour que ses sorties restent à jour.
