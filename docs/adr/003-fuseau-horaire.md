# ADR-003 — Fuseau horaire : horloge UTC déclarée, étiquetée, jamais convertie

- **Statut** : accepté
- **Date** : 2026-11-26

## Contexte

Les sources n'expriment pas toutes leurs horodatages de la même façon :

| Source | Forme | Fuseau |
| --- | --- | --- |
| `telemetry.csv` | `timestamp` = `2025-06-01 00:00:00` | absent |
| `releves_incidents.csv` | `date` = `2025-06-01` et `time` = `05:42`, séparés | absent |
| `machine.sql.txt` (maintenance) | `maintenance_at` = `2025-06-15 10:00:00+00` | `+00` |

Interpréter une heure sans fuseau dans le fuseau du serveur (ou de Paris) la décalerait d'une ou deux heures
selon la saison, sans que rien ne le signale, et fausserait l'alignement télémétrie / incidents / maintenance
sur lequel reposent les features et la cible.

## Décision

1. **L'horloge de chaque source est déclarée** dans son contrat : `timezone: UTC`, seule valeur acceptée par le
   JSON Schema. Une source dans un autre fuseau demande une nouvelle décision, pas un paramètre.
2. **Étiqueter, jamais convertir.** Une valeur sans fuseau est lue comme une heure UTC (`2025-06-01 00:00:00`
   devient `2025-06-01 00:00:00+00`) ; une valeur qui porte un fuseau le garde. Aucun décalage n'est appliqué.
3. **Le bronze stocke le texte d'origine** (ADR-002) : l'étiquetage a lieu au typage, en silver
   (`indusense.silver.casting.to_timestamptz`).
4. **`date` + `time` des incidents** sont fusionnés en silver (`date_time`, opération `MERGE` déclarée dans le
   contrat), avec la même règle d'étiquetage.
5. Les rapports d'ingestion le disent dans leur section `transformations` (« horloge UTC déclarée : valeur sans
   fuseau étiquetée, jamais convertie »).

## Conséquences

- Les horodatages des trois sources sont comparables entre eux, tant que la déclaration UTC est exacte.
- Si une source s'avère être en heure locale, il faudra corriger sa déclaration et retyper en silver depuis
  le bronze, qui a gardé le texte d'origine : rien n'est à recharger.
