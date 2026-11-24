# Contribution — Bronze, Silver et Gold

## Projet
**Projet Big Data IL14 — Suivi des transports publics en temps réel**

Cette documentation présente les travaux réalisés sur les couches **Bronze, Silver et Gold** du Data Lake.

## Architecture de la contribution
```text
Kafka
  |
  v
Bronze (données brutes)
  |
  v
Silver (données nettoyées et structurées)
  |
  v
Gold (données agrégées et indicateurs)
  |
  v
Consommation par les traitements des autres membres
```
Le stockage est assuré par **MinIO**. Les données transformées sont stockées au format **Parquet**.

## 1. Bronze — Raw Data
### Objectif
Conserver les données brutes provenant de l'ingestion, avant les transformations métier.

### Travaux réalisés
- Mise à jour du pipeline Bronze.
- Mise à jour du writer Bronze.
- Centralisation de la configuration et du client MinIO dans `scripts/common/` pour leur réutilisation.

### Fichiers concernés
- `scripts/bronze/bronze_pipeline.py`
- `scripts/bronze/bronze_writer.py`
- `scripts/common/config.py`
- `scripts/common/minio_client.py`

## 2. Silver — Cleaned Data
### Objectif
Préparer des données propres et structurées pour les traitements analytiques.

### Travaux réalisés
- Implémentation du pipeline Silver.
- Transformation et préparation des données provenant de Bronze.
- Stockage des données transformées au format Parquet dans MinIO.
- Ajout d'un script de validation Silver.
- Définition d'un contrat JSON pour les données `predictions_arrets`.

### Fichiers concernés
- `scripts/silver/silver_pipeline.py`
- `scripts/silver/validate_silver.py`
- `contracts/predictions_arrets_silver.schema.json`

## 3. Gold — Aggregated Data
### Objectif
Produire des données agrégées et des indicateurs prêts à être consommés par les composants analytiques en aval.

### Travaux réalisés
- Agrégation des données par ligne.
- Calcul des métriques :
  - `total_predictions`
  - `total_trips`
  - `total_stops`
  - `total_vehicles`
  - `first_prediction`
  - `last_prediction`
  - `avg_prediction_interval`
- Écriture des résultats Gold au format Parquet dans MinIO.
- Ajout du pipeline Gold.
- Ajout d'un script de validation Gold.
- Ajout d'un audit comparant les métriques Silver et Gold.

### Fichiers concernés
- `scripts/gold/gold_predictions_by_route.py`
- `scripts/gold/gold_pipeline.py`
- `scripts/gold/validate_gold.py`
- `scripts/gold/audit_gold.py`

## 4. Configuration et stockage
Les modules partagés ont été regroupés dans :
```text
scripts/common/
├── config.py
└── minio_client.py
```
Cela évite la duplication de la configuration MinIO entre les couches.

### Organisation logique du Data Lake
```text
transport-data/
├── bronze/
├── silver/
│   └── predictions_arrets/
│       └── *.parquet
└── gold/
    └── predictions_by_route/
        └── *.parquet
```
> Ces chemins décrivent l'organisation logique attendue dans le bucket MinIO. Les objets présents dépendent des exécutions des pipelines.

## 5. Validation et contrôle qualité
| Script | Rôle |
|---|---|
| `scripts/silver/validate_silver.py` | Contrôle des données Silver |
| `scripts/gold/validate_gold.py` | Contrôle du résultat Gold |
| `scripts/gold/audit_gold.py` | Comparaison des métriques Silver et Gold |
| `scripts/kafka_test/check_topic_offsets.py` | Vérification des offsets Kafka |

Lors d'une exécution de test Gold, les données observées étaient :
- 36 prédictions
- 9 trajets
- 8 arrêts
- 9 véhicules
- 1 ligne agrégée dans le résultat Gold (jeu de données de test)

L'audit Silver/Gold a confirmé la cohérence de ces totaux sur ce jeu de données. Ces valeurs correspondent au test réalisé et ne garantissent pas les résultats des exécutions futures.

## 6. Commandes d'exécution
À exécuter depuis la racine du dépôt, avec l'environnement Python et les dépendances configurés.

### Pipeline Gold
```bash
python -m scripts.gold.gold_pipeline
```

### Validation Gold
```bash
python -m scripts.gold.validate_gold
```

### Audit Silver/Gold
```bash
python -m scripts.gold.audit_gold
```

### Vérification des offsets Kafka
```bash
python -m scripts.kafka_test.check_topic_offsets
```

Les commandes de Silver et les validations Bronze doivent être vérifiées dans les scripts correspondants et selon la configuration locale.

## 7. Dépendances principales
- Python
- Kafka (source de données en amont)
- MinIO (stockage objet du Data Lake)
- Parquet (format des données transformées)

## 8. Travaux restant à confirmer ou compléter
- Vérifier le pipeline complet sur l'environnement partagé.
- Automatiser le lancement des pipelines (Airflow), si cela fait partie de l'intégration finale.
- Confirmer et finaliser l'intégration Hudi, si elle est requise et pas encore réalisée.
- Tester plusieurs lignes et des volumes plus importants.
- Vérifier que l'audit Gold gère correctement plusieurs lignes.

## 9. Commit Git
Les travaux ont été versionnés et poussés sur `main`.

```text
Commit: a11f2ff
Message: feat: implement bronze silver gold data pipeline
```

## Auteur de cette contribution
**Student B — Bronze / Silver / Gold**

Contribution réalisée dans le cadre du projet Big Data IL14.
