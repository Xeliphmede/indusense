# ADR-002 — Doctrine bronze : la donnée telle quelle, en texte

- **Statut** : accepté
- **Date** : 2026-11-26
- **Remplace** : la variante initiale « miroir `<col>_raw` TEXT + colonne typée + `_cast_errors` »

## Contexte

Le bronze est la source de vérité historique : il doit permettre de rejouer silver et gold sans relire les
fichiers, et ne rien perdre de ce que contenaient les sources. Il faut aussi pouvoir prouver d'où vient chaque
ligne et ce qui a été fait à la donnée.

La première version stockait, pour chaque colonne, un miroir texte `<col>_raw` et une colonne typée « au
mieux », avec le motif des échecs de conversion dans `_cast_errors`. Elle doublait le nombre de colonnes et
mettait de la logique de typage dans le bronze, alors que le typage est une interprétation de la donnée,
qui relève de silver.

## Décision

1. **Tout en texte, à l'identique.** Chaque colonne du fichier devient une colonne `TEXT` qui reçoit la valeur
   d'origine, caractère pour caractère : un champ vide reste `''`, un `NULL` SQL reste `NULL`, `n/a` reste
   `n/a`. Le bronze ne convertit, ne nettoie, ne fusionne ni ne dédoublonne rien.
2. **Le type est déclaré, pas appliqué.** Après chaque colonne, `<col>_type` porte le type déclaré par le
   contrat (`IDENTIFIER`, `NUMBER`, `DATE`…), identique sur toutes les lignes. La conversion vers ce type
   (`silver_type`) est faite en silver.
3. **Empreinte de ligne.** `hash` = SHA-256 du tableau JSON des valeurs stockées des seules colonnes du fichier
   (colonnes ajoutées exclues). Elle se recalcule depuis la base et identifie les lignes de contenu identique.
4. **Traçabilité.** `_batch_id`, `_source_file`, `_source_hash` (SHA-256 du fichier), `_row_number`,
   `_ingested_at` ; registre des lots `bronze._ingestion_batch`.
5. **Idempotence portée par la base.** Clé primaire `(_source_hash, _row_number)` et `ON CONFLICT DO NOTHING` :
   rejouer un fichier n'insère rien.
6. **Tout ou rien.** Une transaction par fichier ; une ligne mal formée (nombre de champs différent de
   l'en-tête) annule le chargement : la tronquer serait une perte silencieuse.
7. **Seule exception : les données personnelles.** Les colonnes `pseudonymize` du contrat (`operator_name`,
   `operator_badge`) sont remplacées par HMAC-SHA256(`ANONYMISATION_SALT`, valeur) avant écriture : la valeur
   d'origine n'entre jamais en base (RGPD, minimisation). C'est une pseudonymisation : le sel est un secret.

## Conséquences

- Aucune perte : toute valeur du fichier est en base (sauf les données personnelles, par décision).
- Le bronze ne détecte pas lui-même les valeurs non typables ; le dictionnaire de données et le notebook les
  comptent (`cast_failures`) en appliquant la conversion silver sans rien écrire.
- Requêter le bronze demande des conversions explicites (`temperature_c::float`) : c'est le rôle de silver.
- Changer le sel change tous les pseudonymes : les lignes déjà chargées ne se recoupent plus avec les nouvelles.
