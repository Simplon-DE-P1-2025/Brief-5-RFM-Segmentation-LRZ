"""
etl/ingest.py — Sub-issues #10 + #11
--------------------------------------
Rôle : Charger le dataset brut Online Retail II (.xlsx)
       dans la table PostgreSQL `raw_orders`.

Flux :
  data/online_retail_II.xlsx
        ↓  (pandas read_excel)
  DataFrame brut (toutes colonnes, aucun filtrage)
        ↓  (pandas to_sql)
  PostgreSQL → rfm_db → table raw_orders
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

# Chemin vers le fichier source (relatif au répertoire de travail)
DATA_PATH = os.getenv("DATA_PATH", "/opt/airflow/data/online_retail_II.xlsx")

# Chaîne de connexion PostgreSQL — injectée via variable d'environnement
# Format : postgresql+psycopg2://user:pass@host/dbname
DB_CONN = os.getenv("RFM_DB_CONN", "postgresql+psycopg2://rfm_user:rfm_pass@postgres/rfm_db")

# Nom de la table cible
TABLE_NAME = "raw_orders"


# ─────────────────────────────────────────────────────────────
# Fonctions
# ─────────────────────────────────────────────────────────────

def load_excel(path: str) -> pd.DataFrame:
    """
    Lit le fichier Excel Online Retail II.
    Le dataset contient deux feuilles (Year 2009-2010 et Year 2010-2011).
    On concatène les deux pour avoir l'historique complet.
    """
    print(f"[ingest] Lecture du fichier : {path}")

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Dataset introuvable : {path}\n"
            "Placez le fichier 'online_retail_II.xlsx' dans le dossier data/."
        )

    # Lecture des deux feuilles du classeur
    df_2009 = pd.read_excel(path, sheet_name="Year 2009-2010", engine="openpyxl")
    df_2010 = pd.read_excel(path, sheet_name="Year 2010-2011", engine="openpyxl")

    # Concaténation + réinitialisation de l'index
    df = pd.concat([df_2009, df_2010], ignore_index=True)

    print(f"[ingest] {len(df):,} lignes chargées depuis le fichier Excel.")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les noms de colonnes pour PostgreSQL :
    - minuscules
    - espaces → underscores
    Exemple : 'Customer ID' → 'customer_id'
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def insert_to_postgres(df: pd.DataFrame, conn_str: str, table: str) -> None:
    """
    Insère le DataFrame dans PostgreSQL.
    - if_exists='replace' : recrée la table à chaque ingestion (idempotent)
    - chunksize=5000      : évite les timeouts sur les gros volumes
    """    print(f"[ingest] Connexion à PostgreSQL...")
    engine = create_engine(conn_str)

    # Vérification de la connexion avant l'insertion
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(f"[ingest] Connexion OK.")

    print(f"[ingest] Insertion dans la table '{table}'...")
    df.to_sql(
        name=table,
        con=engine,
        if_exists="replace",   # écrase si déjà existante → ingestion idempotente
        index=False,           # pas d'index pandas en colonne SQL
        chunksize=5000,        # insertion par lots pour les gros datasets
        method="multi",        # INSERT multi-valeurs, plus rapide
    )
    print(f"[ingest] {len(df):,} lignes insérées dans '{table}'. Ingestion terminée.")


# ─────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────

def run():
    """Orchestration des étapes d'ingestion."""
    try:
        df = load_excel(DATA_PATH)
        df = normalize_columns(df)
        insert_to_postgres(df, DB_CONN, TABLE_NAME)
    except FileNotFoundError as e:
        print(f"[ingest] ERREUR : {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ingest] ERREUR inattendue : {e}")
        raise


if __name__ == "__main__":
    run()
