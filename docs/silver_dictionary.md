# Dictionnaire de données silver

> Fichier **généré** par `uv run indusense silver dictionary` (ou `silver.ipynb`) : ne pas le modifier à la main. Les descriptions et les unités viennent des contrats `config/bronze/*.json`, générés depuis les décisions de `bronze.ipynb` ; les statistiques, du batch silver indiqué.

Batch décrit : `dc04cc10-13c2-41f5-a436-72886c642b50`, généré le 2026-11-27 à 16:04 UTC.

## Unités

| Unité (contrat) | Symbole | Signification |
| --- | --- | --- |
| `CELSIUS` | °C | degré Celsius |
| `BAR` | bar | bar (pression) |
| `VOLT` | V | volt |
| `RPM` | tr/min | tours par minute |
| `HOUR` | h | heure (durée) |
| `PIECE_PER_HOUR` | pièces/h | pièces produites par heure |
| `PIECE_PER_DAY` | pièces/jour | pièces produites par jour |

Les horodatages sont en UTC (horloge déclarée, ADR-003).

## Colonnes techniques (toutes les tables)

| Colonne | Type | Description |
| --- | --- | --- |
| `_batch_id` | UUID | Exécution d'ingestion bronze dont vient la ligne (voir `bronze._ingestion_batch`) |
| `_bronze_id` | BIGINT | Clé primaire ; ligne d'origine `bronze.<table>._id` (fichier, numéro de ligne, valeur brute) |
| `_cast_at` | TIMESTAMPTZ | Instant UTC du typage |

## `silver.machine`

- **Source** : `datas/machine.sql.txt` (table `machine`), via `bronze.machine`
- **Clé naturelle** : `machine_code` (une ligne par clé et par batch, ADR-004)
- **Lignes** (batch décrit) : 15

| Colonne | Description | Type | Unité | Obligatoire | Donnée personnelle | NULL | Min | Max | Valeurs |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `machine_code` | Code unique de la machine (MACH-01 à MACH-15), référencé par la télémétrie, les incidents et les maintenances | VARCHAR(16) |  | oui |  | 0.0 % |  |  | 15 valeurs distinctes |
| `commissioning_date` | Date de mise en service de la machine | DATE |  | oui |  | 0.0 % | 2019-07-23 | 2025-05-25 |  |
| `max_daily_capacity` | Capacité de production maximale déclarée par jour | INTEGER | pièces/jour | oui |  | 0.0 % | 750 | 1428 |  |
| `max_hourly_capacity_pieces` | Capacité de production maximale déclarée par heure | INTEGER | pièces/h | oui |  | 0.0 % | 47 | 89 |  |
| `model` | Modèle de la machine (InduPress-X1, X2, X3, Z1) | VARCHAR(32) |  | oui |  | 0.0 % |  |  | InduPress-X2 (7), InduPress-X3 (3), InduPress-Z1 (3), InduPress-X1 (2) |
| `production_line` | Ligne de production de la machine (Ligne-A à Ligne-C) | VARCHAR(16) |  | oui |  | 0.0 % |  |  | Ligne-A (7), Ligne-B (5), Ligne-C (3) |
| `location` | Atelier où se trouve la machine (Atelier-1 à Atelier-3) | VARCHAR(16) |  | oui |  | 0.0 % |  |  | Atelier-3 (7), Atelier-1 (5), Atelier-2 (3) |
| `criticality` | Criticité de la machine pour la production (LOW, MEDIUM, HIGH) | VARCHAR(8) |  | oui |  | 0.0 % |  |  | MEDIUM (7), LOW (5), HIGH (3) |

Observation (à confirmer par le métier) : `max_daily_capacity` vaut environ 16 fois `max_hourly_capacity_pieces` sur toutes les machines (une journée de 16 h ?), alors que les incidents sont déclarés sur trois équipes : à confirmer.

## `silver.maintenance`

- **Source** : `datas/machine.sql.txt` (table `maintenance`), via `bronze.maintenance`
- **Clé naturelle** : `maintenance_id` (une ligne par clé et par batch, ADR-004)
- **Lignes** (batch décrit) : 115

| Colonne | Description | Type | Unité | Obligatoire | Donnée personnelle | NULL | Min | Max | Valeurs |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `maintenance_id` | Identifiant de l'intervention de maintenance | INTEGER |  | oui |  | 0.0 % |  |  | 115 valeurs distinctes |
| `machine_code` | Machine concernée, référence machine.machine_code | VARCHAR(16) |  | oui |  | 0.0 % |  |  | 15 valeurs distinctes |
| `maintenance_at` | Début de l'intervention, horodatage avec fuseau (+00) | TIMESTAMPTZ |  | oui |  | 0.0 % | 2025-06-15 06:00 UTC | 2026-05-30 22:00 UTC |  |
| `maintenance_type` | Type d'intervention : proactive (planifiée) ou reactive (après une panne) | VARCHAR(16) |  | oui |  | 0.0 % |  |  | proactive (90), reactive (25) |
| `action_type` | Action : changement_programme ou changement_suite_panne (redondant avec maintenance_type sur les données actuelles) | VARCHAR(32) |  | oui |  | 0.0 % |  |  | changement_programme (90), changement_suite_panne (25) |
| `component` | Composant remplacé (capteur pression, filtre hydraulique...) | VARCHAR(64) |  | oui |  | 0.0 % |  |  | capteur pression (24), filtre hydraulique (18), capteur température (16), joint hydraulique (13), roulement axe principal (13), courroie moteur (12), convoyeur sortie (9), variateur vitesse (8), outillage presse (1), relais sécurité (1) |
| `description` | Libellé libre de l'intervention | TEXT |  | oui |  | 0.0 % |  |  | 33 valeurs distinctes |
| `related_incident_id` | Incident à l'origine d'une maintenance corrective (NULL sinon) | VARCHAR(16) |  | non |  | 78.3 % |  |  | 25 valeurs distinctes |
| `duration_hours` | Durée de l'intervention | NUMERIC(6,2) | h | oui |  | 0.0 % | 1.00 | 8.00 |  |

## `silver.releves_incidents`

- **Source** : `datas/releves_incidents.csv`, via `bronze.releves_incidents`
- **Clé naturelle** : `incident_id` (une ligne par clé et par batch, ADR-004)
- **Lignes** (batch décrit) : 1245

| Colonne | Description | Type | Unité | Obligatoire | Donnée personnelle | NULL | Min | Max | Valeurs |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `incident_id` | Identifiant de l'incident (INC-000001...), référencé par maintenance.related_incident_id | TEXT |  | oui |  | 0.0 % |  |  | 1245 valeurs distinctes |
| `date` | Date de déclaration (horloge UTC déclarée) | DATE |  | oui |  | 0.0 % | 2025-06-01 | 2026-06-08 |  |
| `time` | Heure de déclaration HH:MM (horloge UTC déclarée) | TIME |  | oui |  | 0.0 % | 00:06:00 | 23:55:00 |  |
| `operator_name` | Nom de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé) | VARCHAR(64) |  | oui | oui, pseudonymisée | 0.0 % |  |  | 15 pseudonymes distincts |
| `machine_id` | Machine concernée, référence machine.machine_code | TEXT |  | oui |  | 0.0 % |  |  | 15 valeurs distinctes |
| `severity` | Gravité déclarée (1 à 5) | INTEGER |  | oui |  | 0.0 % | 2 | 5 |  |
| `operator_badge` | Matricule de l'opérateur, pseudonymisé à l'ingestion (HMAC-SHA256 salé) | VARCHAR(64) |  | oui | oui, pseudonymisée | 0.0 % |  |  | 10 pseudonymes distincts |
| `comment` | Texte libre : peut contenir des données personnelles | TEXT |  | oui | oui | 0.0 % |  |  | 46 valeurs distinctes |
| `shift` | Équipe de l'opérateur au moment de la déclaration (matin, apres-midi, nuit) | TEXT |  | oui |  | 0.0 % |  |  | matin (427), apres-midi (410), nuit (408) |
| `type_surchauffe` | Vrai si l'incident relève du type « surchauffe » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1111), vrai (134) |
| `type_baisse_pression` | Vrai si l'incident relève du type « baisse de pression » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1077), vrai (168) |
| `type_vibration` | Vrai si l'incident relève du type « vibration » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1066), vrai (179) |
| `type_bruit_mecanique` | Vrai si l'incident relève du type « bruit mécanique » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1038), vrai (207) |
| `type_surconsommation` | Vrai si l'incident relève du type « surconsommation » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1096), vrai (149) |
| `type_blocage_mecanique` | Vrai si l'incident relève du type « blocage mécanique » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1237), vrai (8) |
| `type_alarme_capteur` | Vrai si l'incident relève du type « alarme capteur » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1030), vrai (215) |
| `type_arret_urgence` | Vrai si l'incident relève du type « arrêt d'urgence » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1226), vrai (19) |
| `type_defaut_qualite` | Vrai si l'incident relève du type « défaut qualité » (un incident a 1 ou 2 types) | BOOLEAN |  | oui |  | 0.0 % |  |  | faux (1064), vrai (181) |

Colonnes déclarées par le contrat, pas encore produites :

- `date_time` (TIMESTAMPTZ) : MERGE de `date`, `time`, Concaténation 'date time', horloge UTC déclarée, aucune conversion
- `comment_normalized` (TEXT) : NORM de `comment`, Minuscules, espaces et accents normalisés

## `silver.telemetry`

- **Source** : `datas/telemetry.csv`, via `bronze.telemetry`
- **Clé naturelle** : `machine_id`, `timestamp` (une ligne par clé et par batch, ADR-004)
- **Lignes** (batch décrit) : 134280, après 1346 doublon(s) écarté(s) (même clé naturelle : gardée la ligne la plus complète (le moins de NULL), puis la première du fichier)

| Colonne | Description | Type | Unité | Obligatoire | Donnée personnelle | NULL | Min | Max | Valeurs |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `machine_id` | Code machine (MACH-01...), référence machine.machine_code | TEXT |  | oui |  | 0.0 % |  |  | 15 valeurs distinctes |
| `timestamp` | Heure de la mesure, horloge UTC déclarée (ADR-003) | TIMESTAMPTZ |  | oui |  | 0.0 % | 2025-06-01 00:00 UTC | 2026-06-08 23:00 UTC |  |
| `temperature_c` | Température moyenne de la machine sur l'heure | DOUBLE PRECISION | °C | non |  | 0.7 % | 32 | 80 |  |
| `pressure_bar` | Pression hydraulique moyenne de la machine sur l'heure | DOUBLE PRECISION | bar | non |  | 0.7 % | 159.982 | 215.814 |  |
| `voltage_mean_v` | Tension d'alimentation moyenne de la machine sur l'heure | DOUBLE PRECISION | V | oui |  | 0.0 % | 220.9 | 242 |  |
| `rotation_mean_rpm` | Vitesse de rotation moyenne de la machine sur l'heure | DOUBLE PRECISION | tr/min | non |  | 0.7 % | 1100 | 1900 |  |
| `pieces_produced` | Pièces produites sur l'heure | INTEGER | pièces/h | oui |  | 0.0 % | 0 | 114 |  |

Observation (à confirmer par le métier) : 27,4 % des heures dépassent `machine.max_hourly_capacity_pieces` ; des valeurs collées aux bornes (1 900 et 1 100 tr/min, 80 °C) suggèrent des capteurs saturés (voir `silver.ipynb`, section 8).
