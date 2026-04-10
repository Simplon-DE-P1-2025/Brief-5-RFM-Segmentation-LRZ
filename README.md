# Pipeline de Segmentation RFM

Un pipeline de segmentation client en production, construit sur Apache Airflow, PostgreSQL et Flask. Le système ingère des données transactionnelles brutes, applique un scoring RFM, classe les clients en 11 segments de cycle de vie, et expose les résultats via un tableau de bord interactif.

---

## Vue d'ensemble

L'analyse RFM (Récence, Fréquence, Montant) mesure le comportement client selon trois axes :

- **Récence** — depuis combien de temps le client a effectué un achat
- **Fréquence** — à quelle fréquence il achète
- **Montant** — combien il dépense au total

Chaque client reçoit un score de 1 à 5 sur chaque axe (via des quintiles NTILE), ce qui produit 125 codes RFM possibles. Ces codes sont ensuite mappés vers 11 segments nommés, regroupés en 4 macro-catégories.

| Macro | Segments |
|---|---|
| A. Fideles | Champions, Clients fidèles, Clients récents |
| B. Prometteurs | Fidèles potentiels, Prometteurs, A surveiller |
| C. Endormis | Sur le point de partir, A risque, A ne pas perdre |
| D. Perdus | En hibernation, Perdus |

Le pipeline s'exécute sur les données historiques du [jeu de données UCI Online Retail II](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II) : 1 067 371 transactions, 5 943 clients uniques, couvrant décembre 2009 à décembre 2011.

---

## Architecture

```
online_retail_II.xlsx (1,07M lignes)
        |
        v
[ingest.py]  streaming XLSX -> PostgreSQL COPY
        |
        v
raw.online_retail
        |
        v
[02_clean.sql]  routage en cascade exclusif
        |
        +---> clean.stock_movements      (~4 382 lignes)
        +---> clean.cancellations        (~19 494 lignes)
        +---> clean.non_product_lines    (~6 074 lignes)
        +---> clean.sales                (~798 637 lignes)
                      |
                      v
             [03_rfm.sql]  scoring NTILE(5)
                      |
                      v
             analytics.customer_rfm      (5 852 clients)
                      |
                      v
             [04_segments.sql]  fn_rfm_segment()
                      |
                      v
             analytics.customer_rfm_v   (vue enrichie)
                      |
                      v
             [06_history.sql]  snapshots mensuels
                      |
                      v
             analytics.customer_rfm_history  (~97 000 lignes)
                      |
                      v
             [Tableau de bord Flask]  requêtes -> graphiques Plotly
```

**Schemas de base de données :**

| Schema | Role |
|---|---|
| `raw` | Ingestion verbatim, sans transformation |
| `clean` | Transactions séparées et validées par type |
| `analytics` | Scores RFM, segments, snapshots historiques |

---

## Technologies

| Couche | Technologie |
|---|---|
| Orchestration | Apache Airflow 3.1.0 (CeleryExecutor + Redis) |
| Base de données | PostgreSQL 16 |
| Broker de messages | Redis 7.2 |
| ETL | Python 3.11, pandas 2.2, psycopg2, SQLAlchemy 2 |
| Qualité des données | soda-core-postgres |
| Tableau de bord | Flask 3.1, Plotly 6.6, Gunicorn |
| Packaging | uv (lock file pour des builds reproductibles) |
| Conteneurs | Docker + docker-compose (8 services) |

---

## Structure du projet

```
.
├── dags/
│   ├── dag_rfm.py            # DAG principal avec TaskGroups
│   └── rfm_pipeline.py       # DAG alternatif monolithique
├── etl/
│   ├── 00_functions.sql      # fn_rfm_segment, fn_rfm_macro, vue de couverture
│   ├── 02_clean.sql          # Routage en cascade vers le schema clean
│   ├── 03_rfm.sql            # Calcul RFM et scoring NTILE
│   ├── 04_segments.sql       # Attribution des 11 libellés de segments
│   ├── 05_view_rfm_v.sql     # Vues analytiques enrichies
│   ├── 06_history.sql        # Génération des snapshots mensuels
│   ├── ingest.py             # Ingestion XLSX en streaming
│   └── config.py             # Configuration par variables d'environnement
├── dashboard/
│   ├── app.py                # Routes Flask et proxy API Airflow
│   ├── queries.py            # Couche de requêtes (pandas)
│   ├── charts.py             # Constructeurs de graphiques Plotly
│   ├── templates/            # Templates HTML Jinja2
│   ├── static/               # CSS et JavaScript
│   └── Dockerfile
├── data/
│   └── online_retail_II.xlsx # Jeu de données source (45 Mo)
├── notebooks/
│   ├── EDA.ipynb             # Analyse exploratoire des données
│   └── EDA.md                # Résumé de l'EDA
├── docs/
│   ├── airflow.md            # Guide d'installation Airflow
│   └── data_quality_audit.md # Audit qualité complet (8 sections)
├── init-db.sql               # Initialisation du schema PostgreSQL
├── docker-compose.yaml       # Orchestration multi-services
├── Dockerfile                # Conteneur ETL (basé sur uv)
├── Dockerfile.airflow        # Image Airflow
└── pyproject.toml            # Métadonnées et dépendances du projet
```

---

## Démarrage rapide

### Prérequis

- Docker et docker-compose
- Le fichier source `online_retail_II.xlsx` placé dans le dossier `data/` à la racine du dépôt

### Lancer la stack

```bash
docker-compose up -d
```

Cela démarre 8 services : PostgreSQL, Redis, Airflow (serveur API, scheduler, processeur de DAGs, worker, triggerer) et le tableau de bord Flask.

| Service | URL |
|---|---|
| Interface Airflow | http://localhost:8080 |
| Tableau de bord | http://localhost:8501 |
| Flower (monitoring Celery) | http://localhost:5555 |

Les identifiants Airflow par défaut sont définis dans `docker-compose.yaml` via les variables d'environnement.

### Déclencher le pipeline

Depuis l'interface Airflow ou en ligne de commande :

```bash
airflow dags trigger dag_rfm
```

Le pipeline exécute les groupes de tâches suivants dans l'ordre :

1. **ingest** — charge le fichier XLSX dans `raw.online_retail`
2. **clean** — route les lignes vers les tables typées du schema clean
3. **rfm** — calcule les scores et attribue les segments
4. **history** — matérialise les snapshots mensuels

### Lancer l'ingestion manuellement

```bash
docker-compose run --rm etl python -m etl.ingest
```

---

## Qualite des donnees

Un audit complet du jeu de données source est documenté dans [docs/data_quality_audit.md](docs/data_quality_audit.md).

Principaux constats :

| Probleme | Volume | Decision |
|---|---|---|
| Identifiants client manquants | 243 007 lignes (22,77 %) | Exclus du scoring RFM |
| Factures annulées (préfixe C) | 19 494 | Routées vers `clean.cancellations` |
| Mouvements de stock (sans description, sans client, prix = 0) | 4 382 | Routés vers `clean.stock_movements` |
| Lignes hors-produit (frais, port, ajustements) | 6 074 | Routées vers `clean.non_product_lines` |
| Doublons exacts | 32 907 | Conservés (décision métier) |
| Ventes valides retenues | ~798 637 (~75 %) | Utilisées pour le scoring RFM |

Les contrôles qualité sont appliqués sur `clean.sales` via soda-core-postgres avant l'étape de calcul RFM.

---

## Detail du scoring RFM

- **Date de référence :** `MAX(invoice_date) + 1 jour` sur l'ensemble du jeu de données
- **Récence :** nombre de jours entre la date de référence et le dernier achat du client (plus bas = plus récent)
- **Fréquence :** nombre de factures distinctes par client
- **Montant :** somme de `quantité * prix` par client
- **Scoring :** NTILE(5) par dimension ; le score de récence est inversé pour que 5 = le plus récent
- **Departage :** `customer_id` utilisé comme clé de tri secondaire pour garantir des résultats déterministes

Le mapping des 125 codes RFM vers les segments est implémenté dans `analytics.fn_rfm_segment(r, f, m)` et validé par la vue de diagnostic `analytics.v_rfm_codes_coverage`, qui confirme la couverture exhaustive de toutes les combinaisons.

---

## Tableau de bord

Le tableau de bord Flask se connecte directement au schema `analytics` et propose :

- Distribution des segments (treemap, camembert, barres)
- Heatmaps des scores RFM
- Mouvements des clients entre segments dans le temps (depuis les snapshots mensuels)
- Vues d'analyse de cohortes
- Décomposition sunburst par macro-segment et segment

Les réponses sont mises en cache pendant 60 secondes (flask-caching). Le tableau de bord proxifie également les requêtes vers l'API REST d'Airflow pour afficher l'état du pipeline.

---

## Variables d'environnement

Les conteneurs ETL et dashboard lisent leur configuration depuis des variables d'environnement. Variables principales :

| Variable | Valeur par defaut | Description |
|---|---|---|
| `POSTGRES_HOST` | `postgres` | Hote PostgreSQL |
| `POSTGRES_PORT` | `5432` | Port PostgreSQL |
| `POSTGRES_DB` | `rfm_db` | Nom de la base de données |
| `POSTGRES_USER` | `rfm_user` | Utilisateur de la base de données |
| `POSTGRES_PASSWORD` | `rfm_pass` | Mot de passe |
| `XLSX_PATH` | `/data/online_retail_II.xlsx` | Chemin du fichier source |

Ces valeurs peuvent être surchargées dans `docker-compose.yaml` ou via un fichier `.env` à la racine du dépôt.

---

## Developpement local

### Installer les dépendances

```bash
pip install uv
uv sync
```

### Lancer le tableau de bord en local

```bash
cd dashboard
flask run --port 8501
```

### Exécuter les contrôles qualité

```bash
soda scan -d rfm_db -c soda_config.yml etl/checks.yml
```

---

## Documentation

| Document | Description |
|---|---|
| [docs/data_quality_audit.md](docs/data_quality_audit.md) | Audit complet du jeu de données source (8 sections) |
| [docs/airflow.md](docs/airflow.md) | Guide d'installation Airflow étape par étape |
| [notebooks/EDA.md](notebooks/EDA.md) | Résumé de l'analyse exploratoire |
| [notebooks/EDA.ipynb](notebooks/EDA.ipynb) | Notebook EDA interactif |

---

## Equipe

Brief-5 — Simplon DE P1 2025 — Groupe ALRZ
