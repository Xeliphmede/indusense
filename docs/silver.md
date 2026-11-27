# Création du silver dataset

L'objectif est de créer le silver dataset qui sera un ensemble de table ayant subit le traitement du bronze dataset.
je souhait que tu me le fasse dans un notebook silver.ipynb

## Continuité

La création du silver dataset vient juste après celle du bronze dataset.
Elle doit faire partie intégrante du pipeline.
Lorsque je lance le pipeline je souhaite obtenir à terme les données dans le silver dataset
avec les batchs_id me permettant d'identifier l'ingestion et les identifiants de ligne du bronze dataset
pour savoir exactement d'où vient chaque ligne de mon silver dataset.

## Cast

Créé un nouveau schema "silver" qui contiendra les tables provenant de fichiers bruts du bronze.
L'objectif est de cast chaque colonne avec le type inféré.
S'il y'a une erreur au niveau du cast log la moi dans le notebook et annule l'ingestion.