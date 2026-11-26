# ADR-001 — Définition d'un arrêt machine non planifié (cible du modèle)

- **Statut** : proposé — à valider avec le métier avant la construction du gold
- **Date** : 2026-11-26

## Contexte

Le modèle doit prédire, à un horizon de *n* heures, un **arrêt machine non planifié**
(`guide_ingestion_medallion_indusense.md`, §4). Les données ne contiennent pas de colonne « arrêt » : il faut
la définir à partir de ce qui existe, et cette définition détermine la cible, donc tout ce que le modèle
apprend.

Sources disponibles :

- `maintenance` : `maintenance_type` `proactive` (planifiée) ou `reactive` (corrective), `maintenance_at`,
  `duration_hours`, `related_incident_id` ;
- `releves_incidents` : déclarations d'opérateurs, `severity` de 2 à 5, indicateurs `type_*` dont
  `type_arret_urgence` ;
- `telemetry` : `pieces_produced` horaire (0 possible).

Une déclaration d'incident n'est pas une panne confirmée (`13_RGPD_7_principes_Indusense.md`, principe 4).

## Décision proposée

1. **Un arrêt non planifié** est une intervention de maintenance `reactive` : elle commence à `maintenance_at`
   et dure `duration_hours`. C'est la seule source qui atteste un arrêt réellement traité ; une maintenance
   `proactive` est planifiée et n'en est pas un.
2. Les incidents (dont `type_arret_urgence`) et la production à 0 **ne définissent pas** la cible : ce sont des
   signaux candidats pour les features, calculés uniquement sur le passé.
3. **Cible** : `target_unplanned_stop` = 1 à l'instant *t* si un arrêt non planifié **commence** dans
   `[t + buffer, t + buffer + horizon]` ; 0 sinon. Les instants où la machine est déjà à l'arrêt sont exclus
   de l'apprentissage.
4. Horodatages en UTC (ADR-003) ; split chronologique strict, jamais aléatoire.

## Questions ouvertes

- Valeurs de `buffer` et `horizon` (le guide illustre avec 2 h et 4 h).
- Une maintenance `reactive` sans `related_incident_id` est-elle un arrêt à part entière ?
- Faut-il exclure les arrêts très courts (seuil sur `duration_hours`) ?

## Conséquences

- La cible ne dépend que de faits de maintenance, pas de déclarations non vérifiées.
- Le déséquilibre des classes est fort (25 maintenances `reactive` sur 115, pour 15
  machines sur un an) : à mesurer en gold et à prendre en compte dans l'évaluation (rappel, F1).
