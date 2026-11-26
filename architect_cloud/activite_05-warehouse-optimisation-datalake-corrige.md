# Corrigé — Activité 05 : Entrepôt Analytique, Optimisation & Data Lake (FastMarket)

> **Rappel :** Dans toutes les requêtes ci-dessous, adaptez `NOM` (ex. `dupont`) dans `fastmarket_db_public_NOM` et `fastmarket_analytics_NOM`, ainsi que `<VOTRE-BUCKET>` pour le bucket Silver personnel.

---

## Partie 1 : De la 3NF à la One Big Table (OBT) & Optimisation

### Étape 1.1 : Création de l'OBT brute (sans partition ni cluster)

```sql
CREATE OR REPLACE TABLE `fastmarket_analytics_NOM.obt_order_items_raw` AS
SELECT
  oi.id AS order_item_id,
  o.id AS order_id,
  o.status AS order_status,
  o.created_at AS order_created_at,
  o.updated_at AS order_updated_at,
  oi.quantity,
  oi.unit_price AS item_unit_price,
  (oi.quantity * oi.unit_price) AS line_total_amount,
  p.id AS product_id,
  p.sku AS product_sku,
  p.title AS product_title,
  p.category AS product_category,
  u.id AS user_id,
  u.city AS customer_city,
  u.country AS customer_country
FROM `fastmarket_db_public_NOM.order_items` AS oi
INNER JOIN `fastmarket_db_public_NOM.orders` AS o
  ON oi.order_id = o.id
INNER JOIN `fastmarket_db_public_NOM.products` AS p
  ON oi.product_id = p.id
INNER JOIN `fastmarket_db_public_NOM.users` AS u
  ON o.user_id = u.id;
```

*(Note : Les colonnes sensibles `email`, `password_hash`, `phone_number`, `address_line1`, `shipping_address` et `ip_address` sont volontairement exclues).*

### Étape 1.2 : Création de l'OBT optimisée (Partitionnement & Clustering)

```sql
CREATE OR REPLACE TABLE `fastmarket_analytics_NOM.obt_order_items_opt`
PARTITION BY DATE(order_created_at)
CLUSTER BY order_status, product_category, customer_country
AS
SELECT *
FROM `fastmarket_analytics_NOM.obt_order_items_raw`;
```

### Étape 1.3 : Requêtes de benchmark (trous complétés)

```sql
-- 1) Sur la table brute
SELECT
  product_category,
  SUM(line_total_amount) AS revenue
FROM `fastmarket_analytics_NOM.obt_order_items_raw`
WHERE DATE(order_created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
  AND order_status = 'PAID'
GROUP BY product_category
ORDER BY revenue DESC;

-- 2) Sur la table optimisée
SELECT
  product_category,
  SUM(line_total_amount) AS revenue
FROM `fastmarket_analytics_NOM.obt_order_items_opt`
WHERE DATE(order_created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
  AND order_status = 'PAID'
GROUP BY product_category
ORDER BY revenue DESC;

-- Variante sur plage vide (si toutes les données datent d'aujourd'hui) :
-- WHERE DATE(order_created_at) BETWEEN DATE('2025-01-01') AND DATE('2025-04-01')
```

**Réponses attendues (Partie 1) :**
1. **Différence d'estimation entre `_raw` et `_opt` :**
   * Sur `_raw`, le validateur estime la lecture de **100 % des octets** des colonnes sélectionnées (*Full scan* de toutes les lignes).
   * Sur `_opt`, grâce au **Partition Pruning**, BigQuery n'estime que les partitions comprises dans la fenêtre des 90 jours (et affiche **`0 o`** sur une plage vide comme `2025-01-01` à `2025-04-01`).
   * *(À noter : le gain supplémentaire du clustering sur `order_status = 'PAID'` n'est visible qu'**après** exécution dans les octets réellement traités).*
2. **Pourquoi `order_created_at` pour la partition et non `order_status` ?**
   * BigQuery n'autorise le partitionnement que sur des colonnes temporelles (`DATE`, `TIMESTAMP`, `DATETIME`) ou des plages d'entiers (`INT64`), pas sur des chaînes de caractères (`STRING`).
   * De plus, la quasi-totalité des requêtes analytiques filtrent d'abord par période temporelle. `order_status` (4 valeurs distinctes) est en revanche idéal comme **1ʳᵉ colonne de clustering**.
3. **Pourquoi exclure `email` et `password_hash` dès la création de l'OBT ?**
   * Par respect du **principe du moindre privilège** et du **RGPD** (préparation au Jour 4 : Sécurité). Les analystes BI calculant des ventes par catégorie ou pays n'ont besoin ni des identifiants de connexion, ni des données personnelles nominatives.

---

## Partie 2 : Stratégie d'ingestion — `APPEND` face au `MERGE` de Datastream

### Étape 2.1 : Constat `MERGE` et test du pattern `APPEND`

```sql
-- 1) Constat sur la table répliquée par Datastream (mode MERGE)
SELECT id, status, created_at, updated_at
FROM `fastmarket_db_public_NOM.orders`
LIMIT 5;

-- 2) Création de la nouvelle table de dimension historique
CREATE TABLE IF NOT EXISTS `fastmarket_analytics_NOM.dim_order_status_history` (
  event_id STRING,
  order_id INT64,
  new_status STRING,
  changed_at TIMESTAMP
)
PARTITION BY DATE(changed_at);

-- Alimentation en mode APPEND (à exécuter 2 fois pour simuler une relance du job)
INSERT INTO `fastmarket_analytics_NOM.dim_order_status_history` (event_id, order_id, new_status, changed_at)
VALUES
  ('evt_1001', 1, 'PENDING', TIMESTAMP('2026-11-24 10:00:00+00')),
  ('evt_1002', 1, 'PAID',    TIMESTAMP('2026-11-24 10:05:00+00')),
  ('evt_1003', 1, 'SHIPPED', TIMESTAMP('2026-11-25 08:30:00+00'));

-- 3) Vérification après la 2ᵉ exécution
SELECT *
FROM `fastmarket_analytics_NOM.dim_order_status_history`
ORDER BY changed_at;
```

**Réponses attendues (Partie 2) :**
4. **Constat après la 2ᵉ exécution & comparaison `APPEND` vs `MERGE` :**
   * La 2ᵉ exécution du `INSERT INTO` (`APPEND`) a **dupliqué les 3 lignes** (6 lignes au total dans `dim_order_status_history`).
   * **`MERGE` (Datastream sur `orders`) est idempotent** : rejouer un événement sur le même `id` met à jour la ligne sans créer de doublon, mais **écrase l'historique**.
   * **`APPEND` (`dim_order_status_history`) n'est pas idempotent** (nécessite une déduplication en aval si rejoué), mais il permet de **conserver 100 % de l'historique** des transitions (`PENDING` $\rightarrow$ `PAID` $\rightarrow$ `SHIPPED`).
5. **Choix pour `cart_events` et `payments` :**
   * **Événements clics/paniers (`cart_events`) :** **`APPEND`** — ce sont des faits passés immuables et très volumineux ; on veut écrire vite sans relire la table cible et garder toute la chronologie.
   * **Paiements (`payments`) :** **`APPEND`** si l'on veut tracer chaque tentative (`FAILED`, puis `SUCCESS`, puis `REFUNDED`) ; **`MERGE`** si l'on veut uniquement maintenir un miroir de l'état courant par `id` de transaction (comme Datastream).

---

## Partie 3 : Data Lake sur Cloud Storage (JSON Bronze $\rightarrow$ Parquet Silver)

### Étape 3.3 : Table Externe Zone Bronze (JSON)

```sql
CREATE OR REPLACE EXTERNAL TABLE `fastmarket_analytics_NOM.ext_cart_events_bronze_json`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://fastmarket-website-data/cart_events/*/*.ndjson']
);

SELECT *
FROM `fastmarket_analytics_NOM.ext_cart_events_bronze_json`
LIMIT 20;
```

### Étape 3.4 : Transformation en Parquet aplati (Zone Silver) & Table Externe

```sql
-- 1) Transformation Bronze (JSON imbriqué) -> Silver (Parquet aplati & typé)
EXPORT DATA OPTIONS (
  uri = 'gs://<VOTRE-BUCKET>/silver/cart_events/*.parquet',
  format = 'PARQUET',
  compression = 'SNAPPY',
  overwrite = true
) AS
SELECT
  event_id,
  session_id,
  CAST(user_id AS INT64) AS user_id,
  CAST(product_id AS INT64) AS product_id,
  event_type,
  CAST(quantity AS INT64) AS quantity,
  CAST(unit_price_seen AS NUMERIC) AS unit_price_seen,
  -- Aplatissement des sous-objets JSON (RECORD -> colonnes simples)
  client.device AS device,
  client.os AS os,
  client.browser AS browser,
  client.country AS country,
  marketing.utm_source AS utm_source,
  marketing.utm_campaign AS utm_campaign,
  CAST(event_timestamp AS TIMESTAMP) AS event_timestamp
FROM `fastmarket_analytics_NOM.ext_cart_events_bronze_json`;

-- 3) Création de la Table Externe Silver (Parquet)
CREATE OR REPLACE EXTERNAL TABLE `fastmarket_analytics_NOM.ext_cart_events_silver_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<VOTRE-BUCKET>/silver/cart_events/*.parquet']
);
```

### Étape 3.5 : Requête Lakehouse (Data Lake Silver $\leftrightarrow$ Data Warehouse)

```sql
SELECT
  p.id AS product_id,
  p.title,
  p.category,
  COUNT(e.event_id) AS nb_add_to_cart,
  SUM(e.quantity) AS total_qty_added,
  COUNTIF(e.device = 'mobile') AS nb_mobile_add_to_cart
FROM `fastmarket_analytics_NOM.ext_cart_events_silver_parquet` AS e
INNER JOIN `fastmarket_db_public_NOM.products` AS p
  ON e.product_id = p.id
WHERE e.event_type = 'ADD_TO_CART'
GROUP BY p.id, p.title, p.category
ORDER BY total_qty_added DESC;
```

### Requêtes complémentaires pour la Question 7 (comparaison Bronze vs Silver)

```sql
-- Sur Bronze (JSON)
SELECT event_type, COUNT(*) AS nb
FROM `fastmarket_analytics_NOM.ext_cart_events_bronze_json`
GROUP BY 1;

-- Sur Silver (Parquet)
SELECT event_type, COUNT(*) AS nb
FROM `fastmarket_analytics_NOM.ext_cart_events_silver_parquet`
GROUP BY 1;
```

**Réponses attendues (Partie 3) :**
6. **Avantages du schéma Silver (Parquet aplati) :**
   * En Bronze (JSON), l'autodétection crée des colonnes imbriquées de type `RECORD` (`client`, `marketing`) et type les prix en `FLOAT` (approximatif pour la finance).
   * En Silver (Parquet), l'**aplatissement** rend toutes les colonnes (`device`, `country`, `utm_source`) directement accessibles aux outils BI sans syntaxe imbriquée, et le **schéma strict** (`INT64`, `NUMERIC`, `TIMESTAMP`) est stocké dans les métadonnées binaires du fichier Parquet.
7. **Octets traités & passage à l'échelle sur 100 Go :**
   * Dans l'onglet *Informations sur la tâche* (`Bytes processed`), BigQuery lit **moins d'octets** sur la table Parquet que sur la table JSON.
   * Sur **100 Go**, le gain est double :
     1. **Compression :** Parquet Snappy réduit le volume de 5 à 8 fois par rapport au texte JSON (suppression de la répétition des clés JSON sur chaque ligne + encodage par dictionnaire).
     2. **Stockage colonnaire (*Column Pruning*) :** Sur 100 Go de JSON (orienté ligne), BigQuery doit parser 100 % des fichiers. En Parquet (orienté colonne), BigQuery ne lit **que la colonne `event_type`**, soit quelques centaines de Mo au lieu de 100 Go (impact direct sur les coûts FinOps du Jour 5).
8. **`user_id = NULL` et jointure avec `users` :**
   * Les valeurs `NULL` correspondent aux **visiteurs anonymes** (non connectés) qui naviguent ou ajoutent des produits au panier avant de s'authentifier.
   * Il faut utiliser un **`LEFT JOIN`** depuis `ext_cart_events_silver_parquet` vers `fastmarket_db_public_NOM.users` (un `INNER JOIN` exclurait silencieusement tout le trafic et les paniers des visiteurs anonymes).