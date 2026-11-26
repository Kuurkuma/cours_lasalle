## Dossier d'Architecture — FastMarket Data Platform

### 1. Matrice des composants (Les boîtes du diagramme)

| Couche / Rôle | Service GCP | Nom exact / URI / ID | Région | Format & Modélisation | Stratégie (`APPEND` / `MERGE` / Autre) | Sécurité & Coût *(Jours 4–5)* |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Base OLTP (Jour 1)** | Cloud SQL (PostgreSQL) | `________________` | `________` | Tables relationelles normalisés   | Transactions OLTP (CRUD) | *À compléter J4–J5* |
| **Pipeline CDC (Jour 2)** | Datastream | `________________` | `________` | Replication CDC | `MERGE` (via clé primaire) | *À compléter J4–J5* |
| **DWH — Zone CDC brute (Jour 2)** | BigQuery (Dataset) | `fastmarket_db_public_____` | `________` | 5 tables miroir + metadata de `datastream` | `MERGE`  | *À compléter J4–J5* |
| **Data Lake — Zone Bronze (Jour 3)** | Cloud Storage (Bucket) | `gs://________________` | `________` | Fichiers NDJSON imbriqués | `APPEND` (immuable / brut) | *À compléter J4–J5* |
| **Data Lake — Zone Silver (Jour 3)** | Cloud Storage (Bucket) | `gs://________________` | `________` | Fichiers Parquet aplatis (Snappy) | Batch / Écrasement | *À compléter J4–J5* |
| **Lakehouse (Jour 3)** | BigQuery (Tables Externes) | `ext_cart_events_...` | `________` | Lecture sur GCS (JSON & Parquet) | Lecture seule  | *À compléter J4–J5* |
| **DWH — Optimiser BI (Jour 3)** | BigQuery (OBT) | `obt_order_items_opt` | `________` | Dénormalisée (OBT) + Partition + Cluster | Batch (sans données sensible) | *À compléter J4–J5* |
| **Sécurité & Gouvernance *(Jour 4)*** | *IAM / Cloud DLP / ...* | `________________` | `________` | *À compléter J4* | *À compléter J4* | *À compléter J4* |
| **Observabilité & FinOps *(Jour 5)*** | *Monitoring / Budgets / ...* | `________________` | `________` | *À compléter J5* | *À compléter J5* | *À compléter J5* |

---

### 2. Matrice des flux de données (Les flèches du diagramme)

| ID Flux | Source | Destination | Outil / Mécanisme | Fréquence | Transformation appliquée |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **F1** | App E-commerce | Cloud SQL | Connexion SQL / Backend | Temps réel | Aucune (écritures transactionnelles 3NF) |
| **F2** | Cloud SQL | BigQuery (`fastmarket_db_public_...`) | `________________` | 5 min ou Journalier | `________________` |
| **F3** | App Web / Mobile | GCS Bronze (`cart_events/`) | Flux événements JSON | Batch | `________________` |
| **F4** | GCS Bronze | BigQuery (`ext_..._bronze_json`) | Table Externe BigQuery | À la demande | Lecture directe sans copie |
| **F5** | BigQuery (Bronze) | GCS Silver (`silver/cart_events/`) | `________________` | Batch SQL | Aplatissement `RECORD`, typage strict, JSON $\rightarrow$ Parquet |
| **F6** | BigQuery (`fastmarket_db_public_...`) | BigQuery (`obt_order_items_opt`) | Requête SQL | Batch | `________________` |
| **F7** | GCS Silver + BigQuery CDC | Outil BI / Analyste | `________________` | À la demande | Jointure Lakehouse (`LEFT` / `INNER JOIN`) |