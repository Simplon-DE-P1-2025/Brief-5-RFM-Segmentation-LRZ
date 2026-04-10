# Pipeline de Segmentation RFM

Un pipeline de segmentation client en production, construit sur Apache Airflow, PostgreSQL et Flask.
Le systeme ingere des donnees transactionnelles brutes, applique un scoring RFM, classe les clients en 11 segments de cycle de vie, et expose les resultats via un tableau de bord interactif.

---

## Vue d'ensemble

L'analyse RFM (Recency, Frequency, Monetary) mesure le comportement client selon trois axes :

- Recence : depuis combien de temps le client a effectue un achat
- Frequence : a quelle frequence il achete
- Montant : combien il depense au total

Chaque client recoit un score de 1 a 5 sur chaque axe (via des quintiles NTILE), ce qui produit 125 codes RFM possibles. Ces codes sont ensuite mappes vers 11 segments nommes, regroupes en 4 macro-categories.

| Macro | Segments |
|---|---|
| A. Fideles | Champions, Clients fideles, Clients recents |
| B. Prometteurs | Fideles potentiels, Prometteurs, A surveiller |
| C. Endormis | Sur le point de partir, A risque, A ne pas perdre |
| D. Perdus | En hibernation, Perdus |

Le pipeline s'execute sur les donnees historiques du jeu de donnees UCI Online Retail II : 1 067 371 transactions, 5 943 clients uniques, couvrant decembre 2009 a decembre 2011.

---

## Architecture

```text
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
             analytics.customer_rfm      (~5 852 clients)
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
             [Dashboard Flask]  requetes -> graphiques Plotly
```

Schemas de base de donnees :

| Schema | Role |
|---|---|
| raw | Ingestion verbatim, sans transformation |
| clean | Transactions separees et validees par type |
| analytics | Scores RFM, segments, snapshots historiques |

---

## Technologies

| Couche | Technologie |
|---|---|
| Orchestration | Apache Airflow 3.1.0 (CeleryExecutor + Redis) |
| Base de donnees | PostgreSQL 16 |
| Broker de messages | Redis 7.2 |
| ETL | Python 3.11, pandas 2.2, psycopg2, SQLAlchemy 2 |
| Qualite des donnees | soda-core-postgres |
| Tableau de bord | Flask 3.1, Plotly 6.6, Gunicorn |
| Packaging | uv (lock file pour des builds reproductibles) |
| Conteneurs | Docker + docker compose |

---

## Structure du projet

```text
.
├── dags/
│   ├── dag_rfm_taskgroup.py  # DAG principal avec TaskGroups (dag_id=dag_rfm)
│   └── rfm_pipeline.py       # DAG alternatif monolithique
├── etl/
│   ├── 00_functions.sql      # fn_rfm_segment, fn_rfm_macro, vue de couverture
│   ├── 02_clean.sql          # Routage en cascade vers le schema clean
│   ├── 03_rfm.sql            # Calcul RFM et scoring NTILE
│   ├── 04_segments.sql       # Attribution des 11 libelles de segments
│   ├── 05_view_rfm_v.sql     # Vues analytiques enrichies
│   ├── 06_history.sql        # Generation des snapshots mensuels
│   ├── ingest.py             # Ingestion XLSX en streaming
│   └── config.py             # Configuration par variables d'environnement
├── dashboard/
│   ├── app.py                # Routes Flask et proxy API Airflow
│   ├── queries.py            # Couche de requetes (pandas)
│   ├── charts.py             # Constructeurs de graphiques Plotly
│   ├── templates/            # Templates HTML Jinja2
│   ├── static/               # CSS et JavaScript
│   └── Dockerfile
├── data/
│   └── online_retail_II.xlsx # Jeu de donnees source
├── notebooks/
│   ├── EDA.ipynb             # Analyse exploratoire des donnees
│   └── EDA.md                # Resume de l'EDA
├── docs/
│   ├── airflow.md            # Guide d'installation Airflow
│   └── data_quality_audit.md # Audit qualite complet
├── init-db.sql               # Initialisation du schema PostgreSQL
├── docker-compose.yaml       # Orchestration multi-services
├── Dockerfile                # Conteneur ETL
├── Dockerfile.airflow        # Image Airflow
└── pyproject.toml            # Metadonnees et dependances du projet
```

---

## Demarrage rapide

### Prerequis

- Docker et docker compose
- Le fichier source online_retail_II.xlsx place dans le dossier data a la racine du depot

### Lancer la stack

```bash
docker compose up airflow-init
docker compose up -d
```

Acces principaux :

| Service | URL |
|---|---|
| Interface Airflow | http://localhost:8080 |
| Tableau de bord | http://localhost:8501 |
| Flower (monitoring Celery, profil optionnel) | http://localhost:5555 |

Pour activer Flower :

```bash
docker compose --profile flower up -d
```

Les identifiants Airflow par defaut sont definis dans docker-compose.yaml (airflow / airflow).

### Declencher le pipeline

Depuis l'interface Airflow ou en ligne de commande :

```bash
docker compose exec airflow-apiserver airflow dags trigger dag_rfm
```

Le pipeline execute les groupes de taches suivants dans l'ordre :

1. ingest : charge le fichier XLSX dans raw.online_retail
2. transform_rfm : route vers clean et calcule le RFM
3. segmentation : attribue les segments et cree les vues
4. historique : materialise les snapshots mensuels

Le DAG alternatif monolithique est disponible sous le nom rfm_pipeline.

### Lancer l'ingestion manuellement

```bash
docker compose --profile manual run --rm etl python -m etl.ingest
```

---

## Qualite des donnees

Un audit complet du jeu de donnees source est documente dans docs/data_quality_audit.md.

Principaux constats :

| Probleme | Volume | Decision |
|---|---|---|
| Identifiants client manquants | 243 007 lignes (22,77 %) | Exclus du scoring RFM |
| Factures annulees (prefixe C) | 19 494 | Routees vers clean.cancellations |
| Mouvements de stock (sans description, sans client, prix = 0) | 4 382 | Routes vers clean.stock_movements |
| Lignes hors-produit (frais, port, ajustements) | 6 074 | Routees vers clean.non_product_lines |
| Doublons exacts | 32 907 | Conserves (decision metier) |
| Ventes valides retenues | ~798 637 (~75 %) | Utilisees pour le scoring RFM |

---


## Data Quality avec Soda

Le pipeline intègre des contrôles de qualité automatisés à l'aide de **Soda Core**. Ces tests agissent comme des "Data Quality Gates" pour garantir l'intégrité des données à chaque étape critique :

1.  **Post-Ingestion** : Vérification de la volumétrie et de la structure de `raw.online_retail`.
2.  **Post-Nettoyage** : Validation que `clean.sales` ne contient plus de valeurs nulles sur les identifiants critiques et que les prix sont positifs.
3.  **Post-Scoring** : Contrôle de la distribution des scores NTILE dans `analytics.customer_rfm` (vérification que les scores sont bien compris entre 1 et 5).

Si un test critique échoue, le pipeline Airflow est stoppé pour éviter la propagation de données erronées vers le dashboard. Les fichiers de configuration se trouvent dans le dossier `soda/`.

---


## Detail du scoring RFM

- Date de reference : MAX(invoice_date) + 1 jour
- Recence : nombre de jours entre date de reference et dernier achat
- Frequence : nombre de factures distinctes par client
- Montant : somme de quantite * prix par client
- Scoring : NTILE(5) par dimension ; le score recence est inverse (5 = plus recent)
- Departage : customer_id utilise comme tri secondaire pour des resultats deterministes

Le mapping des 125 codes vers les segments est implemente dans analytics.fn_rfm_segment(r, f, m), avec verification de couverture via analytics.v_rfm_codes_coverage.

---

## Tableau de bord

Le dashboard Flask se connecte au schema analytics et propose :

- Distribution des segments (treemap, pie, barres)
- Heatmaps des scores RFM
- Mouvements des clients entre segments dans le temps
- Vues d'analyse de cohortes
- Decomposition par macro-segment et segment

Les reponses sont mises en cache pendant 60 secondes via flask-caching.
Le dashboard expose aussi un proxy vers l'API REST Airflow pour afficher l'etat et declencher le pipeline.

---

## Variables d'environnement

Variables principales utilisees par ETL, Airflow et dashboard :

| Variable | Exemple de valeur | Description |
|---|---|---|
| RFM_DB_DSN | postgresql://rfm_user:rfm_pass@postgres/rfm_db | Connexion PostgreSQL principale |
| DATA_PATH | /opt/airflow/data/online_retail_II.xlsx | Chemin du fichier source dans Airflow |
| RFM_DB_CONN | postgresql+psycopg2://rfm_user:rfm_pass@postgres/rfm_db | Connexion SQLAlchemy (dashboard) |
| AIRFLOW_BASE_URL | http://airflow-apiserver:8080 | URL interne API Airflow |
| AIRFLOW_USER | airflow | Utilisateur API Airflow |
| AIRFLOW_PASSWORD | airflow | Mot de passe API Airflow |

Ces valeurs peuvent etre surchargees dans docker-compose.yaml ou via un fichier .env.

---

## Developpement local

### Installer les dependances

Soda est utilise comme couche de data quality gates dans le DAG rfm_pipeline_soda.py.
Apres l ingestion, il execute des controles sur raw.online_retail, puis sur clean.sales et analytics.customer_rfm afin de bloquer la suite du pipeline en cas d anomalie.
Les regles sont centralisees dans soda/configuration.yaml et soda/checks/*.yaml, avec des verifications sur les volumes, les valeurs manquantes, les doublons et les bornes metier attendues.
Cette approche permet de securiser les etapes de nettoyage, de scoring RFM et de segmentation avant la publication des donnees dans le dashboard.
```bash
pip install uv
uv sync
```

### Lancer le dashboard en local

```bash
uv run gunicorn -b 0.0.0.0:8501 -w 2 dashboard.app:app
```

---

## Documentation

- docs/data_quality_audit.md : audit complet du jeu de donnees source
- docs/airflow.md : guide d'installation Airflow
- notebooks/EDA.md : resume de l'analyse exploratoire
- notebooks/EDA.ipynb : notebook EDA interactif

---

## Equipe

Brief-5 - Simplon DE P1 2025 - Groupe ALRZ

---

## Licence

Projet distribue sous licence Apache 2.0. Voir LICENSE.