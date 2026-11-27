# ADR-004 — Dédoublonnage silver : une ligne par clé naturelle, la plus complète

- **Statut** : accepté
- **Date** : 2026-11-27

## Contexte

Le bronze garde toutes les lignes du fichier (ADR-002) ; la clé naturelle de chaque table est déclarée dans son
contrat (`target.natural_key`) mais n'y est pas imposée : c'est un contrat pour silver.

Dans `telemetry.csv`, la clé `(machine_id, timestamp)` n'est pas unique : **1 346 lignes en trop** sur 135 626,
réparties sur **1 340 heures** (1 334 heures en double, 6 en triple), sur les quinze machines (158 à 204 lignes
chacune). Les trois autres sources n'ont aucun doublon.

Ces doublons ne sont pas des copies identiques :

| Constat | Valeur |
| --- | --- |
| Position dans le fichier | lignes consécutives (écart de 1, au plus 2) : une ligne réémise aussitôt |
| `pieces_produced` | toujours identique |
| Mesures (`temperature_c`, `pressure_bar`, `voltage_mean_v`, `rotation_mean_rpm`) | différentes dans ~97 % des heures, de 0,016 en moyenne et 0,09 au plus : du bruit de mesure |
| Mesures vides | 67 vides dans les lignes en double, parfois sur une seule des deux lignes |

Exemple (`MACH-01`, `2025-06-01 00:00:00`) :

| `_row_number` | `temperature_c` | `pressure_bar` | `voltage_mean_v` | `rotation_mean_rpm` | `pieces_produced` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 46.348 | 198.203 | 227.568 | 1541.787 | 4 |
| 2 | 46.332 | 198.206 | 227.57 | 1541.76 | 4 |

Garder les deux lignes donnerait deux mesures pour la même heure : le fenêtrage et les jointures temporelles de
gold compteraient l'heure deux fois.

## Décision

1. **Une ligne par clé naturelle et par batch** en silver, pour toutes les tables qui déclarent une
   `natural_key` (`src/indusense/silver/dedup.py`), garantie par une contrainte d'unicité
   `uq_silver_<table>_natural_key (natural_key..., _batch_id)`.
2. **La ligne gardée est la plus complète** (le moins de valeurs NULL dans les colonnes du contrat) ; à égalité,
   **la première du fichier** (plus petit `_bronze_id`). Une mesure présente dans une seule des lignes n'est
   donc pas perdue par le choix de la ligne.
3. **Rien n'est fusionné** : la ligne gardée est recopiée telle quelle, sans moyenne ni complément pris dans le
   doublon. Chaque valeur silver reste une valeur exacte du bronze, et le contrôle de fidélité (pandera) continue
   de le vérifier.
4. **La clé est comparée sur les valeurs typées** : `2025-06-01 00:00:00` et `2025-06-01 00:00:00+00` sont le même
   instant (ADR-003). Une ligne dont la clé contient un NULL n'est jamais dédoublonnée.
5. **Rien ne disparaît sans trace** : chaque ligne écartée est consignée dans `silver._duplicate` (ligne bronze
   écartée, ligne bronze gardée, clé naturelle, NULL de chacune, règle, `run_id`). Le contrôle de fidélité exige
   que toute ligne bronze absente du silver y figure, que la ligne gardée soit en silver et que leurs clés soient
   identiques.
6. Le nombre de lignes écartées est consigné par exécution (`silver._cast_run.rows_deduplicated`) et détaillé
   dans le rapport des traitements (`artifacts/ingestion/silver/`).
7. La migration `0007` applique la même règle, en SQL, aux batches déjà typés avant elle.

## Conséquences

- `silver.telemetry` a 134 280 lignes par batch au lieu de 135 626 ; `silver._duplicate` en explique la différence,
  ligne à ligne.
- L'écart de mesure entre deux doublons (0,09 au plus) est négligeable devant la dispersion des mesures : le choix
  de la ligne ne change pas l'analyse.
- Une moyenne des doublons aurait été défendable, mais elle aurait produit des valeurs absentes du bronze. Si elle
  devient nécessaire, c'est une nouvelle opération déclarée et documentée, pas un changement silencieux de cette
  règle.
- Le bronze reste intact : changer de règle ne demande que de retyper les batches
  (`uv run indusense silver cast <batch_id>`).
