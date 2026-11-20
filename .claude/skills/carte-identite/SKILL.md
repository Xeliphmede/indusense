---
name: carte-identite
description: Génère la carte d'identité Markdown d'un ou plusieurs fichiers de données InduSense (CSV, TSV ou script SQL) en appelant la ligne de commande `indusense` du projet. La carte donne la taille, le charset, les dimensions, les types de colonnes inférés et les statistiques. À utiliser quand l'utilisateur demande la carte d'identité, le profil ou la description d'un fichier de données (ex. datas/telemetry.csv).
argument-hint: <fichier> [<fichier> ...]
allowed-tools: Bash(uv run indusense:*), Read
---

# Carte d'identité d'un fichier de données

Fichiers demandés : $ARGUMENTS

1. **Choisir les fichiers.** Si aucun n'est donné, demander lequel traiter en proposant ceux de `datas/`.
   Formats acceptés : `.csv`, `.tsv` et scripts SQL (`.sql`, `.sql.txt`).

2. **Lancer la commande** depuis la racine du projet (le dossier de `pyproject.toml`), avec tous les fichiers
   en une seule fois :

   ```shell
   uv run indusense <fichier> [<fichier> ...]
   ```

   - Sortie standard : le chemin de chaque carte écrite, `artifacts/ingestion/identities/<AAAAMMJJ>/<nom>_<HHMMSS>.md`.
   - Sortie d'erreur : une ligne `indusense: <fichier> : <message>` par fichier en échec (introuvable, format non
     pris en charge, fichier illisible). Les autres fichiers sont quand même traités ; le code de sortie vaut alors 1.

3. **Lire chaque carte** générée avec Read.

4. **Restituer**, pour chaque fichier :
   - le lien vers la carte,
   - un résumé court : charset, nombre de lignes et de colonnes, types inférés,
   - les remarques de lecture indiquées entre parenthèses après un type : `N invalid values`,
     `ambiguous day/month, read as DD/MM`, `UTC`.

   Ne recopier la carte entière que si l'utilisateur le demande. Rapporter chaque erreur telle que la commande
   l'affiche, sans la contourner : ne modifier ni le fichier source ni le code.
