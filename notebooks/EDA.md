# Analyse Exploratoire des Données (EDA) - Pipeline RFM

Ce document détaille la phase d'analyse exploratoire réalisée sur le dataset "Online Retail II". Cette étape est un prérequis essentiel à la construction de la pipeline de données orchestrée et dockerisée définie dans le brief du projet.

## Objectifs de l'analyse
L'analyse contenue dans le notebook `EDA.ipynb` répond aux objectifs pédagogiques suivants :
* Identifier les contraintes de nettoyage nécessaires avant l'ingestion dans PostgreSQL.
* Définir les règles de gestion pour le calcul des indicateurs Recency, Frequency et Monetary (RFM).
* Préparer la structure des données pour les scripts ETL et l'orchestration via Airflow.

## Synthèse de la Qualité des Données (Data Quality)
L'exploration a permis d'isoler plusieurs points critiques qui seront traités lors de l'étape de transformation :

### 1. Intégrité des Identifiants
* Un volume important de `Customer ID` manquants a été détecté. Ces lignes seront exclues lors de la transformation vers le schema silver car l'analyse RFM nécessite un identifiant client unique par transaction.

### 2. Traitement des Annulations
* Les transactions dont le numéro de facture (`Invoice`) commence par la lettre "C" ont été identifiées comme des annulations.
* Ces entrées génèrent des quantités négatives qui doivent être traitées spécifiquement pour ne pas fausser les calculs de fréquence et de montant total.

### 3. Anomalies de Valeurs
* Des prix unitaires à 0.00 et des quantités négatives (hors annulations) ont été relevés.
* Ces lignes correspondent souvent à des ajustements de stock ou des frais administratifs et seront filtrées pour garantir la fiabilité de l'indicateur Monetary.

## Architecture Technique et Flux de Données
Les conclusions de cette EDA orientent la mise en place des composants suivants dans le `docker-compose.yml` :
* **PostgreSQL** : Stockage des données brutes (étape 1) et des données transformées (étape 2).
* **Scripts ETL** : Automatisation du nettoyage et des calculs basés sur les observations du notebook.
* **Airflow** : Orchestration des tâches d'ingestion et de transformation.

## Conclusion de l'étape
Cette phase d'EDA confirme la faisabilité du projet et définit précisément le périmètre du nettoyage des données indispensable au fonctionnement correct de la pipeline finale.