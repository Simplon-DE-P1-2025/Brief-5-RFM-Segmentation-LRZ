"""
dashboard/queries.py — Couche données du dashboard RFM
=======================================================

Fonctions pures qui lisent `analytics.customer_rfm`, `customer_rfm_v` et
`clean.sales` et retournent un dict ou un DataFrame prêt à être passé à
un builder Plotly ou à un template Jinja2.

Sprint 4 (Phase 3) : ajout de ~10 fonctions pour le dashboard v2 inspiré
du dashboard JustDataPlease (cf. docs/dashboard_v2_audit.md).
Les 5 fonctions historiques (Sprint Phase 2) restent intactes en haut de
fichier pour ne pas casser dashboard.html / presentation.html.

Les fonctions sans paramètre dynamique sont mémoïsées (TTL 60 s)
via flask-caching pour éviter de retaper Postgres à chaque hit.

L'engine SQLAlchemy est passé en argument plutôt qu'importé d'app.py
afin d'éviter un import circulaire.

Robustesse : si la table n'existe pas (Phase 1 pas encore exécutée)
ou si Postgres est down, chaque fonction retourne un placeholder
plutôt que de faire planter la requête HTTP.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from flask_caching import Cache
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError

# Cache exporté ; initialisé dans app.py via cache.init_app(app).
cache = Cache()

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _safe_read_sql(sql: str, engine: Engine, params: dict | None = None) -> pd.DataFrame:
    """Wrap pd.read_sql avec gestion des erreurs DB → DataFrame vide.

    Note : on passe une Connection (et non l'Engine) à pd.read_sql car
    pandas 2.x + SQLAlchemy 1.4 ne détecte pas toujours correctement
    l'Engine et tombe dans le path DBAPI legacy (qui appelle .cursor()
    sur l'objet — ce que l'Engine n'a pas).
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql, conn, params=params or {})
    except (OperationalError, ProgrammingError, DatabaseError) as exc:
        log.warning("DB query failed (returning empty): %s", exc)
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────
# 1. KPI globaux (4 valeurs pour la barre supérieure du dashboard)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_kpis(engine: Engine) -> dict[str, Any]:
    sql = """
        SELECT COUNT(*)                              AS total_customers,
               ROUND(AVG(recency)::numeric, 1)       AS avg_recency,
               ROUND(AVG(monetary)::numeric, 0)      AS avg_monetary,
               COUNT(DISTINCT rfm_segment)           AS n_segments
        FROM analytics.customer_rfm;
    """
    df = _safe_read_sql(sql, engine)
    if df.empty:
        return {"total_customers": 0, "avg_recency": 0, "avg_monetary": 0, "n_segments": 0}
    row = df.iloc[0]
    return {
        "total_customers": int(row["total_customers"]),
        "avg_recency":     float(row["avg_recency"]),
        "avg_monetary":    float(row["avg_monetary"]),
        "n_segments":      int(row["n_segments"]),
    }


# ─────────────────────────────────────────────────────────────────────
# 2. Distribution des segments (treemap + table de référence)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_segment_distribution(engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT rfm_segment                       AS segment,
               COUNT(*)                          AS n,
               ROUND(AVG(monetary)::numeric, 0)  AS avg_monetary,
               ROUND(AVG(recency)::numeric, 1)   AS avg_recency,
               ROUND(AVG(frequency)::numeric, 1) AS avg_frequency
        FROM analytics.customer_rfm
        GROUP BY rfm_segment
        ORDER BY n DESC;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# 3. Heatmap R × F (5×5)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_rf_heatmap(engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT r_score, f_score, COUNT(*) AS count
        FROM analytics.customer_rfm
        GROUP BY r_score, f_score
        ORDER BY r_score, f_score;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# 4. Monetary par segment (boxplot)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_monetary_by_segment(engine: Engine) -> pd.DataFrame:
    sql = """
        SELECT rfm_segment AS segment, monetary
        FROM analytics.customer_rfm
        ORDER BY rfm_segment;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# 5. Table clients filtrée (HTMX, pas de cache car varie avec les filtres)
# ─────────────────────────────────────────────────────────────────────

def get_customers(engine: Engine, segments: list[str] | None = None) -> pd.DataFrame:
    """
    Top 500 clients (par monetary décroissant), optionnellement filtrés
    par une liste de segments. Pas de mémoïsation : la combinaison de
    segments est trop variable pour bénéficier d'un cache simple.
    """
    if segments:
        sql = """
            SELECT customer_id, recency, frequency, monetary,
                   rfm_segment AS segment
            FROM analytics.customer_rfm
            WHERE rfm_segment = ANY(%(segments)s)
            ORDER BY monetary DESC
            LIMIT 500;
        """
        return _safe_read_sql(sql, engine, params={"segments": segments})

    sql = """
        SELECT customer_id, recency, frequency, monetary,
               rfm_segment AS segment
        FROM analytics.customer_rfm
        ORDER BY monetary DESC
        LIMIT 500;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# 6. Liste statique des segments (pour peupler le filtre HTMX)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_all_segments(engine: Engine) -> list[str]:
    sql = """
        SELECT DISTINCT rfm_segment
        FROM analytics.customer_rfm
        ORDER BY rfm_segment;
    """
    df = _safe_read_sql(sql, engine)
    return df["rfm_segment"].tolist() if not df.empty else []


# ═════════════════════════════════════════════════════════════════════
# SPRINT 4 — Fonctions du dashboard v2 (Phase 3)
#
# Inspiré du dashboard de référence JustDataPlease (cf. docs/dashboard_v2_audit.md).
# Toutes les fonctions consomment la vue analytics.customer_rfm_v créée
# au Sprint 0 (préfixes A.→K. pour le tri alphabétique des segments).
# ═════════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────────
# v2.1 — KPI bar enrichie (réutilisée sur Overview + R/F/M deep-dive)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_kpi_bar(engine: Engine) -> dict[str, Any]:
    """Bloc de KPI globaux : volumes + taux + moyennes RFM.

    Renvoie un dict avec :
      - total_users / total_transactions / total_net_revenue
      - adslt (avg recency) / atpu (avg frequency) / arpu (avg monetary)
      - pct_new / pct_returning / pct_churned
    """
    sql = """
        SELECT
            (SELECT COUNT(*) FROM analytics.customer_rfm)         AS total_users,
            (SELECT COUNT(DISTINCT invoice_id) FROM clean.sales)  AS total_transactions,
            (SELECT SUM(line_amount)::numeric FROM clean.sales)   AS total_net_revenue,
            AVG(recency)::int                                     AS adslt,
            ROUND(AVG(frequency)::numeric, 1)                     AS atpu,
            ROUND(AVG(monetary)::numeric, 0)                      AS arpu,
            ROUND(100.0 * SUM((frequency = 1 AND recency <= 90)::int) / COUNT(*), 0)  AS pct_new,
            ROUND(100.0 * SUM((frequency >  1 AND recency <= 180)::int) / COUNT(*), 0) AS pct_returning,
            ROUND(100.0 * SUM((recency > 180)::int) / COUNT(*), 0) AS pct_churned
        FROM analytics.customer_rfm;
    """
    df = _safe_read_sql(sql, engine)
    if df.empty:
        return {
            "total_users": 0, "total_transactions": 0, "total_net_revenue": 0,
            "adslt": 0, "atpu": 0.0, "arpu": 0,
            "pct_new": 0, "pct_returning": 0, "pct_churned": 0,
        }
    row = df.iloc[0]
    return {
        "total_users":        int(row["total_users"]),
        "total_transactions": int(row["total_transactions"]),
        "total_net_revenue":  float(row["total_net_revenue"]),
        "adslt":              int(row["adslt"]),
        "atpu":               float(row["atpu"]),
        "arpu":               float(row["arpu"]),
        "pct_new":            int(row["pct_new"]),
        "pct_returning":      int(row["pct_returning"]),
        "pct_churned":        int(row["pct_churned"]),
    }


# ─────────────────────────────────────────────────────────────────────
# v2.2 — Bubble chart (page Overview 1.3)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_bubble_segments(engine: Engine) -> pd.DataFrame:
    """1.3 — 1 ligne par segment : ADSLT, total revenue, total users."""
    sql = """
        SELECT
            segment_label,
            AVG(recency)::int  AS adslt,
            SUM(monetary)      AS total_revenue,
            COUNT(*)           AS total_users
        FROM analytics.customer_rfm_v
        GROUP BY segment_label
        ORDER BY segment_label;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# v2.3 — Scatter plot user-level (page Overview 1.4)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_scatter_sample(engine: Engine, limit: int = 1500) -> pd.DataFrame:
    """1.4 — Échantillon aléatoire d'utilisateurs (~1 500 points par défaut).

    Note : on n'utilise pas TABLESAMPLE car la cible est une vue
    (`analytics.customer_rfm_v`) et Postgres rejette TABLESAMPLE sur
    les vues normales (clause réservée aux tables et vues matérialisées).
    `ORDER BY random() LIMIT N` reste très rapide sur 5 852 lignes.

    On exclut monetary <= 0 pour pouvoir mettre l'axe Y en échelle log.
    """
    sql = """
        SELECT customer_id, recency, monetary, segment_label
        FROM analytics.customer_rfm_v
        WHERE monetary > 0
        ORDER BY random()
        LIMIT %(lim)s;
    """
    return _safe_read_sql(sql, engine, params={"lim": limit})


# ─────────────────────────────────────────────────────────────────────
# v2.4 — Table KPI par segment (page Overview 1.5)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_table_kpi_per_segment(engine: Engine) -> pd.DataFrame:
    """1.5 — Une ligne par segment : counts + parts + moyennes RFM.

    Triée par préfixe alphabétique (A.→K.) — du meilleur au pire segment.
    """
    sql = """
        SELECT
            segment_label,
            COUNT(*)                                                    AS total_users,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)          AS pct_users,
            ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER (), 1)  AS pct_revenue,
            ROUND(100.0 * SUM(frequency) / SUM(SUM(frequency)) OVER (), 1) AS pct_tx,
            AVG(recency)::int                                           AS avg_recency,
            ROUND(AVG(frequency)::numeric, 1)                           AS avg_frequency,
            ROUND(AVG(monetary)::numeric, 0)                            AS avg_monetary
        FROM analytics.customer_rfm_v
        GROUP BY segment_label
        ORDER BY segment_label;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# v2.5 — Bins R/F/M (pages 4/5/6 — combo bar+line par bin)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_recency_bins(engine: Engine) -> pd.DataFrame:
    """4.4 — 5 bins recency : 0-7 / 8-30 / 31-90 / 91-180 / >180 jours."""
    sql = """
        WITH bins AS (
            SELECT
                customer_id, recency,
                CASE
                    WHEN recency BETWEEN 0 AND 7    THEN 'A.0-7 days'
                    WHEN recency BETWEEN 8 AND 30   THEN 'B.8-30 days'
                    WHEN recency BETWEEN 31 AND 90  THEN 'C.31-90 days'
                    WHEN recency BETWEEN 91 AND 180 THEN 'D.91-180 days'
                    ELSE                                 'E.>180 days'
                END AS bin_label
            FROM analytics.customer_rfm
        )
        SELECT
            bin_label,
            COUNT(*)                                              AS users,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)    AS pct_users,
            AVG(recency)::int                                     AS avg_metric
        FROM bins
        GROUP BY bin_label
        ORDER BY bin_label;
    """
    return _safe_read_sql(sql, engine)


@cache.memoize(timeout=60)
def get_frequency_bins(engine: Engine) -> pd.DataFrame:
    """5.4 — 5 bins frequency : >10 / 6-10 / 3-5 / 2 / 1.

    Le combo chart affiche le % de **transactions** (pas de monetary).
    On joint clean.sales pour avoir COUNT(DISTINCT invoice_id) par client.
    """
    sql = """
        WITH bins AS (
            SELECT
                customer_id, frequency,
                CASE
                    WHEN frequency >= 11 THEN 'A.>10 #'
                    WHEN frequency >=  6 THEN 'B.6-10 #'
                    WHEN frequency >=  3 THEN 'C.3-5 #'
                    WHEN frequency  =  2 THEN 'D.2 #'
                    ELSE                       'E.1 #'
                END AS bin_label
            FROM analytics.customer_rfm
        ),
        joined AS (
            SELECT b.bin_label, b.customer_id, COUNT(DISTINCT s.invoice_id) AS tx
            FROM bins b
            LEFT JOIN clean.sales s USING (customer_id)
            GROUP BY b.bin_label, b.customer_id
        )
        SELECT
            bin_label,
            COUNT(DISTINCT customer_id)                                                  AS users,
            ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER (), 1) AS pct_users,
            ROUND(100.0 * SUM(tx) / SUM(SUM(tx)) OVER (), 1)                            AS avg_metric
        FROM joined
        GROUP BY bin_label
        ORDER BY bin_label;
    """
    return _safe_read_sql(sql, engine)


@cache.memoize(timeout=60)
def get_monetary_bins(engine: Engine) -> pd.DataFrame:
    """6.4 — 5 bins monetary fixes en £.

    Seuils retenus après lecture de la distribution réelle :
      - 0-300 £    : très petits comptes
      - 301-1k £   : petits à moyens clients
      - 1k-3k £    : bons clients
      - 3k-10k £   : gros clients
      - >10k £     : comptes majeurs / queue haute
    """
    sql = """
        WITH
        bins AS (
            SELECT
                customer_id,
                monetary,
                CASE
                    WHEN monetary > 10000 THEN 'A.>10k £'
                    WHEN monetary > 3000  THEN 'B.3k-10k £'
                    WHEN monetary > 1000  THEN 'C.1k-3k £'
                    WHEN monetary > 300   THEN 'D.301-1k £'
                    ELSE                       'E.0-300 £'
                END AS bin_label
            FROM analytics.customer_rfm
        )
        SELECT
            bin_label,
            COUNT(*)                                              AS users,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)    AS pct_users,
            ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER (), 1) AS avg_metric
        FROM bins
        GROUP BY bin_label
        ORDER BY bin_label;
    """
    return _safe_read_sql(sql, engine)


# ─────────────────────────────────────────────────────────────────────
# v2.6 — Distributions brutes pour les box plots (pages 4/5/6)
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_recency_distribution(engine: Engine) -> pd.DataFrame:
    """4.3 — 1 ligne par client : recency. Pour le box plot."""
    return _safe_read_sql(
        "SELECT recency FROM analytics.customer_rfm WHERE recency IS NOT NULL;",
        engine,
    )


@cache.memoize(timeout=60)
def get_frequency_distribution(engine: Engine) -> pd.DataFrame:
    """5.3 — 1 ligne par client : frequency."""
    return _safe_read_sql(
        "SELECT frequency FROM analytics.customer_rfm WHERE frequency IS NOT NULL;",
        engine,
    )


@cache.memoize(timeout=60)
def get_monetary_distribution(engine: Engine) -> pd.DataFrame:
    """6.3 — 1 ligne par client : monetary."""
    return _safe_read_sql(
        "SELECT monetary FROM analytics.customer_rfm WHERE monetary IS NOT NULL;",
        engine,
    )


# ─────────────────────────────────────────────────────────────────────
# v2.7 — Charts BONUS uniques à notre dataset
# ─────────────────────────────────────────────────────────────────────

@cache.memoize(timeout=60)
def get_top_products_by_segment(engine: Engine, top_n: int = 5) -> pd.DataFrame:
    """B1 — Top N produits par segment (jointure clean.sales × customer_rfm_v).

    Renvoie segment_label / stock_code / description / revenue.
    Idéal pour une table "top 5 par segment" sur la page Overview.
    """
    sql = """
        WITH segment_sales AS (
            SELECT
                c.segment_label,
                s.stock_code,
                s.description,
                SUM(s.line_amount) AS revenue
            FROM clean.sales s
            JOIN analytics.customer_rfm_v c USING (customer_id)
            GROUP BY 1, 2, 3
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY segment_label ORDER BY revenue DESC) AS rk
            FROM segment_sales
        )
        SELECT segment_label, stock_code, description, revenue
        FROM ranked
        WHERE rk <= %(top_n)s
        ORDER BY segment_label, revenue DESC;
    """
    return _safe_read_sql(sql, engine, params={"top_n": top_n})


@cache.memoize(timeout=60)
def get_monthly_revenue(engine: Engine) -> pd.DataFrame:
    """B6 — CA mensuel total sur toute la période (24 mois sur Online Retail II)."""
    sql = """
        SELECT
            date_trunc('month', invoice_date)::date AS month,
            SUM(line_amount)                        AS revenue,
            COUNT(DISTINCT invoice_id)              AS n_invoices,
            COUNT(DISTINCT customer_id)             AS n_customers
        FROM clean.sales
        GROUP BY 1
        ORDER BY 1;
    """
    return _safe_read_sql(sql, engine)


# ═════════════════════════════════════════════════════════════════════
# SPRINT 5 — Pages Movements + Cohorts (snapshot mensuel)
#
# Toutes les fonctions consomment analytics.customer_rfm_history,
# matérialisée par la 4e task Airflow `compute_rfm_history`.
# ═════════════════════════════════════════════════════════════════════


@cache.memoize(timeout=60)
def get_macro_movements(engine: Engine) -> pd.DataFrame:
    """Page 2 — composition macro de la base clients par snapshot.

    Format long (1 ligne par snapshot × macro) prêt pour Plotly stacked.
    La vue est cumulative: chaque snapshot inclut tous les clients connus
    à date, pas seulement les clients actifs du mois.
    """
    sql = """
        WITH totals AS (
            SELECT snapshot_date, COUNT(*)::numeric AS n_total
            FROM analytics.customer_rfm_history
            GROUP BY snapshot_date
        )
        SELECT
            h.snapshot_date,
            h.macro_segment,
            COUNT(*)                                                AS n_users,
            ROUND(100.0 * COUNT(*) / t.n_total, 2)                  AS pct_users
        FROM analytics.customer_rfm_history h
        JOIN totals t USING (snapshot_date)
        GROUP BY h.snapshot_date, h.macro_segment, t.n_total
        ORDER BY h.snapshot_date, h.macro_segment;
    """
    return _safe_read_sql(sql, engine)


@cache.memoize(timeout=60)
def get_acquisitions_trend(engine: Engine) -> pd.DataFrame:
    """Page 3 — Line chart "User Acquisitions Trend Line".

    1 ligne par mois d'acquisition (= mois de la 1ère commande).
    """
    sql = """
        SELECT
            acquisition_month,
            COUNT(*) AS user_acquisitions
        FROM analytics.customer_first_purchase
        GROUP BY acquisition_month
        ORDER BY acquisition_month;
    """
    return _safe_read_sql(sql, engine)


@cache.memoize(timeout=60)
def get_cohort_pivot(engine: Engine) -> pd.DataFrame:
    """Page 3 — Pivot table "KPIs by User Acquisition Month".

    Pour chaque mois d'acquisition : nombre de clients + % par macro
    segment courant. Permet de répondre à "quelle cohorte d'acquisition
    a fini par devenir LOYAL / SLEEP / LOST ?"
    """
    sql = """
        WITH joined AS (
            SELECT
                f.acquisition_month,
                v.macro_segment
            FROM analytics.customer_first_purchase f
            JOIN analytics.customer_rfm_v v USING (customer_id)
        )
        SELECT
            acquisition_month,
            COUNT(*)                                                       AS user_acquisitions,
            ROUND(100.0 * SUM((macro_segment='A.LOYAL')::int)     / COUNT(*), 1) AS pct_loyal,
            ROUND(100.0 * SUM((macro_segment='B.PROMISING')::int) / COUNT(*), 1) AS pct_promising,
            ROUND(100.0 * SUM((macro_segment='C.SLEEP')::int)     / COUNT(*), 1) AS pct_sleep,
            ROUND(100.0 * SUM((macro_segment='D.LOST')::int)      / COUNT(*), 1) AS pct_lost
        FROM joined
        GROUP BY acquisition_month
        ORDER BY acquisition_month;
    """
    return _safe_read_sql(sql, engine)


@cache.memoize(timeout=60)
def get_history_volumes(engine: Engine) -> dict[str, Any]:
    """Sanity / KPI bar des pages Movements & Cohorts."""
    sql = """
        SELECT
            COUNT(*)                       AS total_rows,
            COUNT(DISTINCT snapshot_date)  AS n_snapshots,
            MIN(snapshot_date)             AS first_snap,
            MAX(snapshot_date)             AS last_snap
        FROM analytics.customer_rfm_history;
    """
    df = _safe_read_sql(sql, engine)
    if df.empty:
        return {"total_rows": 0, "n_snapshots": 0, "first_snap": None, "last_snap": None}
    row = df.iloc[0]
    return {
        "total_rows":  int(row["total_rows"]),
        "n_snapshots": int(row["n_snapshots"]),
        "first_snap":  row["first_snap"],
        "last_snap":   row["last_snap"],
    }
