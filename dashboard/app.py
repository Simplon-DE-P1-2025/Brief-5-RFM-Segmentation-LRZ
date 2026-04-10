"""
dashboard/app.py — Application Flask du dashboard RFM v2
=========================================================

Routes (Phase 3) :

  /             → overview.html         (page 1 du dashboard JustDataPlease)
  /recency      → analysis.html         (deep-dive Recency)
  /frequency    → analysis.html         (deep-dive Frequency)
  /monetary     → analysis.html         (deep-dive Monetary)
  /movements    → movements.html        (page 2 — % macro × mois) [Sprint 5]
  /cohorts      → cohorts.html          (page 3 — pivot acquisition) [Sprint 5]
  /about        → about.html            (présentation du dataset)
  /glossary     → glossary.html         (table des définitions métriques)
  /presentation → presentation.html     (mode reveal.js plein écran) [Sprint 6 refonte]
  /legacy       → dashboard.html        (ancienne vue exploratoire Phase 2)
  /api/segments → fragment HTMX         (table filtrée par segment)
  /api/airflow/health        → ping Airflow  [Sprint 6]
  /api/airflow/trigger       → POST trigger DAG rfm_pipeline [Sprint 6]
  /api/airflow/runs/latest   → état du dernier run [Sprint 6]
  /health       → JSON                  (healthcheck Docker)

L'engine SQLAlchemy est instancié une seule fois au démarrage.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine

from dashboard import charts, queries
from dashboard.queries import cache


# ─────────────────────────────────────────────────────────────────────
# Sprint 6 — Proxy vers l'API REST Airflow 3
#
# Le navigateur ne peut pas appeler http://localhost:8080 directement
# (CORS). Flask forward les requêtes côté serveur via urllib.
#
# Côté Flask container, le hostname Airflow est `airflow-apiserver`
# (clé YAML du service dans docker-compose, donc nom DNS interne).
# Airflow 3 utilise /api/v2/... + JWT Bearer (FabAuthManager).
# ─────────────────────────────────────────────────────────────────────

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER     = os.environ.get("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "airflow")
AIRFLOW_DAG_ID   = "rfm_pipeline"

# Cache du JWT : (token, expires_at_epoch). TTL court (~10 min) pour
# rester simple — on regénère sur 401 de toute façon.
_jwt_cache: dict[str, Any] = {"token": None, "exp": 0.0}
_JWT_TTL_SECONDS = 600


def _get_jwt_token(force_refresh: bool = False) -> str:
    """Récupère un JWT Bearer auprès de l'API auth d'Airflow 3.

    Cache le token pendant ~10 min ; force_refresh=True pour invalider
    (utilisé sur 401)."""
    now = time.time()
    if not force_refresh and _jwt_cache["token"] and _jwt_cache["exp"] > now:
        return _jwt_cache["token"]

    url = f"{AIRFLOW_BASE_URL}/auth/token"
    body = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
    req = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode())
    token = payload.get("access_token") or payload.get("jwt_token") or payload.get("token")
    if not token:
        raise URLError(f"login response missing token field: keys={list(payload.keys())}")
    _jwt_cache["token"] = token
    _jwt_cache["exp"] = now + _JWT_TTL_SECONDS
    return token


def _airflow_request(path: str, method: str = "GET", body: dict | None = None) -> dict:
    """Client urllib avec JWT Bearer pour appeler l'API REST Airflow 3.

    Path doit commencer par `/api/v2/...`. Regénère le JWT sur 401.
    """
    def _do(token: str) -> dict:
        url = f"{AIRFLOW_BASE_URL}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = json.dumps(body).encode() if body else None
        req = Request(url, data=data, headers=headers, method=method)
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())

    try:
        return _do(_get_jwt_token())
    except HTTPError as exc:
        if exc.code == 401:
            return _do(_get_jwt_token(force_refresh=True))
        raise


# ─────────────────────────────────────────────────────────────────────
# Configuration des 3 pages d'analyse R/F/M
#
# Chaque dimension partage le même template `analysis.html` ; cette
# spec décrit les particularités (titres, couleurs, bins, etc.) que
# la route injecte dans le contexte au moment du rendu.
# ─────────────────────────────────────────────────────────────────────

DIMENSION_SPECS: dict[str, dict[str, Any]] = {
    "recency": {
        "active_page":         "recency",
        "page_title":          "Recency Analysis",
        "page_subtitle":       "Distribution de la récence (jours depuis la dernière transaction)",
        "dimension_label_lc":  "récence",
        "bin_class":           "r",
        "bins":                ["A.0-7 days", "B.8-30 days", "C.31-90 days", "D.91-180 days", "E.>180 days"],
        "box_title":           "Days Since Last Transaction per User",
        "combo_title":         "% Users per bucket and AVG Days Since Last Tx",
        "metric_label":        "AVG Days",
        "metric_axis_title":   "AVG Days Since Last Tx",
        "column":              "recency",
        "color":               charts.DIMENSION_COLORS["recency"],
        "log_y":               False,
    },
    "frequency": {
        "active_page":         "frequency",
        "page_title":          "Frequency Analysis",
        "page_subtitle":       "Distribution du nombre de transactions par client",
        "dimension_label_lc":  "fréquence",
        "bin_class":           "f",
        "bins":                ["A.>10 #", "B.6-10 #", "C.3-5 #", "D.2 #", "E.1 #"],
        "box_title":           "Total Transactions per User",
        "combo_title":         "% Users per bucket contributing to % of Total Transactions",
        "metric_label":        "% Tx",
        "metric_axis_title":   "% Total Transactions",
        "column":              "frequency",
        "color":               charts.DIMENSION_COLORS["frequency"],
        "log_y":               True,   # frequency est très skewed
    },
    "monetary": {
        "active_page":         "monetary",
        "page_title":          "Monetary Analysis",
        "page_subtitle":       "Distribution du chiffre d'affaires par client (£) — bandes fixes métier en livres sterling",
        "dimension_label_lc":  "monétaire",
        "bin_class":           "m",
        "bins":                ["A.>10k £", "B.3k-10k £", "C.1k-3k £", "D.301-1k £", "E.0-300 £"],
        "box_title":           "Total Net Revenue per User (£)",
        "combo_title":         "% Users per bucket contributing to % of Total Net Revenue",
        "metric_label":        "% Revenue",
        "metric_axis_title":   "% Total Net Revenue",
        "column":              "monetary",
        "color":               charts.DIMENSION_COLORS["monetary"],
        "log_y":               True,   # monetary est très skewed (max ~600k £)
    },
}


def _build_dimension_context(engine, dimension: str) -> dict[str, Any]:
    """Construit le contexte du template `analysis.html` pour une dimension donnée."""
    spec = DIMENSION_SPECS[dimension]

    # Distribution brute (pour le box plot) + bins agrégés (pour le combo chart)
    if dimension == "recency":
        dist_df = queries.get_recency_distribution(engine)
        bins_df = queries.get_recency_bins(engine)
    elif dimension == "frequency":
        dist_df = queries.get_frequency_distribution(engine)
        bins_df = queries.get_frequency_bins(engine)
    else:  # monetary
        dist_df = queries.get_monetary_distribution(engine)
        bins_df = queries.get_monetary_bins(engine)

    # 3 KPI spécifiques calculés côté Python depuis la distribution
    dimension_kpis = _compute_dimension_kpis(dimension, dist_df)

    box_fig = charts.build_distribution_box(
        dist_df,
        column=spec["column"],
        title=spec["box_title"],
        color=spec["color"],
        log_y=spec["log_y"],
    )
    combo_fig = charts.build_combo_bin_chart(
        bins_df,
        metric_label=spec["metric_label"],
        metric_axis_title=spec["metric_axis_title"],
        color=spec["color"],
    )

    return {
        **spec,
        "kpi_bar":         queries.get_kpi_bar(engine),
        "dimension_kpis":  dimension_kpis,
        "box_fig":         box_fig,
        "combo_fig":       combo_fig,
    }


def _compute_dimension_kpis(dimension: str, dist_df) -> list[dict[str, str]]:
    """Calcule 3 KPI spécifiques par dimension à partir de la distribution brute."""
    if dist_df.empty:
        return [{"label": "Donnée indisponible", "value": "—"}]

    if dimension == "recency":
        col = dist_df["recency"]
        return [
            {"label": "ADSLT (moyenne)",          "value": f"{int(col.mean())} jours"},
            {"label": "% Recent users (≤ 7 j)",   "value": f"{(col <= 7).mean() * 100:.0f}%"},
            {"label": "% Churned users (> 180 j)", "value": f"{(col > 180).mean() * 100:.0f}%"},
        ]
    if dimension == "frequency":
        col = dist_df["frequency"]
        return [
            {"label": "ATPU (moyenne)",        "value": f"{col.mean():.1f}"},
            {"label": "% One Timers (= 1)",    "value": f"{(col == 1).mean() * 100:.0f}%"},
            {"label": "% Frequent (≥ 11)",     "value": f"{(col >= 11).mean() * 100:.0f}%"},
        ]
    # monetary
    col = dist_df["monetary"]
    return [
        {"label": "ARPU (moyenne)",         "value": f"£{col.mean():,.0f}".replace(",", " ")},
        {"label": "Median monetary",        "value": f"£{col.median():,.0f}".replace(",", " ")},
        {"label": "% High Value (> £3k)",   "value": f"{(col > 3000).mean() * 100:.0f}%"},
    ]


# ─────────────────────────────────────────────────────────────────────
# Application factory
# ─────────────────────────────────────────────────────────────────────

def create_app() -> Flask:
    app = Flask(__name__)

    # ─── Config ──────────────────────────────────────────────────────
    app.config["CACHE_TYPE"] = "SimpleCache"      # in-memory, suffisant en démo
    app.config["CACHE_DEFAULT_TIMEOUT"] = 60      # secondes
    cache.init_app(app)

    db_url = os.environ.get(
        "RFM_DB_CONN",
        "postgresql+psycopg2://rfm_user:rfm_pass@localhost:5432/rfm_db",
    )
    engine = create_engine(db_url, pool_pre_ping=True)
    app.config["RFM_ENGINE"] = engine

    # ─── Routes principales (Sprint 4) ───────────────────────────────

    @app.route("/")
    def overview():
        kpi_bar = queries.get_kpi_bar(engine)
        bubble_df = queries.get_bubble_segments(engine)
        scatter_df = queries.get_scatter_sample(engine)
        segment_kpi_df = queries.get_table_kpi_per_segment(engine)
        rf_df = queries.get_rf_heatmap(engine)
        monthly_df = queries.get_monthly_revenue(engine)
        top_products_df = queries.get_top_products_by_segment(engine, top_n=5)

        return render_template(
            "overview.html",
            active_page="overview",
            kpi_bar=kpi_bar,
            segment_kpi_table=segment_kpi_df.to_dict(orient="records"),
            top_products=charts.group_top_products_by_segment(top_products_df),
            segment_colors=charts.SEGMENT_COLORS,
            bubble_fig=charts.build_bubble_segments(bubble_df),
            scatter_fig=charts.build_scatter_sample(scatter_df),
            heatmap_fig=charts.build_rf_heatmap(rf_df),
            monthly_revenue_fig=charts.build_monthly_revenue(monthly_df),
        )

    @app.route("/recency")
    def recency():
        return render_template("analysis.html", **_build_dimension_context(engine, "recency"))

    @app.route("/frequency")
    def frequency():
        return render_template("analysis.html", **_build_dimension_context(engine, "frequency"))

    @app.route("/monetary")
    def monetary():
        return render_template("analysis.html", **_build_dimension_context(engine, "monetary"))

    @app.route("/movements")
    def movements():
        movements_df = queries.get_macro_movements(engine)
        return render_template(
            "movements.html",
            active_page="movements",
            kpi_bar=queries.get_kpi_bar(engine),
            history=queries.get_history_volumes(engine),
            movements_pct_fig=charts.build_macro_movements_pct(movements_df),
            movements_abs_fig=charts.build_macro_movements_abs(movements_df),
        )

    @app.route("/cohorts")
    def cohorts():
        return render_template(
            "cohorts.html",
            active_page="cohorts",
            kpi_bar=queries.get_kpi_bar(engine),
            cohort_pivot=queries.get_cohort_pivot(engine).to_dict(orient="records"),
            acquisitions_trend_fig=charts.build_acquisitions_trend(
                queries.get_acquisitions_trend(engine)
            ),
        )

    @app.route("/sankey")
    def sankey():
        snapshot_dates = queries.get_snapshot_dates(engine)
        history = queries.get_history_volumes(engine)

        if len(snapshot_dates) < 2:
            return render_template(
                "sankey.html",
                active_page="sankey",
                kpi_bar=queries.get_kpi_bar(engine),
                history=history,
                snapshot_dates=[],
                default_from=None,
                default_to=None,
                sankey_fig=charts._empty_figure("Pas assez de snapshots (min 2)"),
            )

        date_from = snapshot_dates[-2]
        date_to = snapshot_dates[-1]
        transitions_df = queries.get_segment_transitions(
            engine, date_from, date_to, level="macro",
        )

        return render_template(
            "sankey.html",
            active_page="sankey",
            kpi_bar=queries.get_kpi_bar(engine),
            history=history,
            snapshot_dates=[d.isoformat() for d in snapshot_dates],
            default_from=date_from.isoformat(),
            default_to=date_to.isoformat(),
            sankey_fig=charts.build_sankey_transitions(transitions_df, level="macro"),
        )

    @app.route("/api/sankey")
    def api_sankey():
        from datetime import date as _date

        date_from_str = request.args.get("from")
        date_to_str = request.args.get("to")
        level = request.args.get("level", "macro")

        if level not in ("macro", "detailed"):
            return jsonify({"error": "level must be 'macro' or 'detailed'"}), 400

        try:
            date_from = _date.fromisoformat(date_from_str)
            date_to = _date.fromisoformat(date_to_str)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid date format (expected YYYY-MM-DD)"}), 400

        transitions_df = queries.get_segment_transitions(
            engine, date_from, date_to, level=level,
        )
        fig_json = charts.build_sankey_transitions(transitions_df, level=level)
        return app.response_class(response=fig_json, mimetype="application/json")

    @app.route("/about")
    def about():
        return render_template("about.html")

    @app.route("/glossary")
    def glossary():
        return render_template("glossary.html")

    # ─── Routes legacy / présentation (conservées) ───────────────────

    @app.route("/legacy")
    def legacy_dashboard():
        """Ancienne vue Phase 2 conservée pour comparaison/archive."""
        kpis = queries.get_kpis(engine)
        seg_dist = queries.get_segment_distribution(engine)
        rf_df = queries.get_rf_heatmap(engine)
        box_df = queries.get_monetary_by_segment(engine)
        customers = queries.get_customers(engine)
        all_segments = queries.get_all_segments(engine)

        return render_template(
            "dashboard.html",
            kpis=kpis,
            segment_distribution=seg_dist.to_dict(orient="records"),
            customers=customers.to_dict(orient="records"),
            all_segments=all_segments,
            selected_segments=[],
            treemap_fig=charts.build_treemap(seg_dist),
            heatmap_fig=charts.build_rf_heatmap(rf_df),
            boxplot_fig=charts.build_monetary_boxplot(box_df),
        )

    @app.route("/presentation")
    def presentation():
        """Mode reveal.js plein écran (Sprint 6 — refonte démo orale).

        Charge tout le matériel visuel + interactif d'un seul shot :
        KPI bar, table segment, 4 charts Plotly, et le contexte pour
        les slides "démo live Airflow" qui consomment /api/airflow/*.
        """
        kpi_bar = queries.get_kpi_bar(engine)
        seg_kpi_df = queries.get_table_kpi_per_segment(engine)
        bubble_df = queries.get_bubble_segments(engine)
        movements_df = queries.get_macro_movements(engine)
        history = queries.get_history_volumes(engine)

        snapshot_dates = queries.get_snapshot_dates(engine)
        if len(snapshot_dates) >= 2:
            sankey_df = queries.get_segment_transitions(
                engine, snapshot_dates[-2], snapshot_dates[-1], level="macro",
            )
            sankey_fig = charts.build_sankey_transitions(sankey_df, level="macro", dark=True)
        else:
            sankey_fig = charts._empty_figure("Pas assez de snapshots")

        return render_template(
            "presentation.html",
            kpi_bar=kpi_bar,
            history=history,
            segment_kpi_table=seg_kpi_df.to_dict(orient="records"),
            segment_colors=charts.SEGMENT_COLORS,
            macro_colors=charts.MACRO_COLORS,
            airflow_url=os.environ.get("AIRFLOW_PUBLIC_URL", "http://localhost:8080"),
            sankey_fig=sankey_fig,
            snapshot_dates=[d.isoformat() for d in snapshot_dates],
            default_from=snapshot_dates[-2].isoformat() if len(snapshot_dates) >= 2 else None,
            default_to=snapshot_dates[-1].isoformat() if len(snapshot_dates) >= 2 else None,
            bubble_fig=charts.build_bubble_segments(bubble_df, dark=True),
            movements_pct_fig=charts.build_macro_movements_pct(movements_df, dark=True),
        )

    @app.route("/presentation-v3")
    def presentation_v3():
        """Le Voyage du Datum — présentation narrative en 9 chapitres.

        On suit en first-person le client #17850 (un revendeur britannique
        At Risk, vérifiable en base) à travers tout le pipeline. Démo
        Airflow live conservée au chapitre 6 via /api/airflow/*.
        """
        kpi_bar = queries.get_kpi_bar(engine)
        rf_df = queries.get_rf_heatmap(engine)
        movements_df = queries.get_macro_movements(engine)
        history = queries.get_history_volumes(engine)

        return render_template(
            "presentation_v3.html",
            kpi_bar=kpi_bar,
            history=history,
            segment_colors=charts.SEGMENT_COLORS,
            macro_colors=charts.MACRO_COLORS,
            airflow_url=os.environ.get("AIRFLOW_PUBLIC_URL", "http://localhost:8080"),
            heatmap_fig=charts.build_rf_heatmap(rf_df),
            movements_pct_fig=charts.build_macro_movements_pct(movements_df),
        )

    @app.route("/api/segments")
    def api_segments():
        # HTMX envoie soit ?segments=A&segments=B (multi-select natif),
        # soit ?segments=A,B (séparateur explicite). On gère les deux.
        raw = request.args.getlist("segments")
        if len(raw) == 1 and "," in raw[0]:
            raw = [s.strip() for s in raw[0].split(",") if s.strip()]
        segments = [s for s in raw if s]

        customers = queries.get_customers(engine, segments=segments or None)
        return render_template(
            "_table_segments.html",
            customers=customers.to_dict(orient="records"),
            selected_segments=segments,
        )

    @app.route("/health")
    def health():
        return jsonify({"status": "ok"})

    # ─── API proxy Airflow (Sprint 6) ────────────────────────────────

    @app.route("/api/airflow/health")
    def airflow_health():
        """Ping l'API REST Airflow et renvoie OK / KO."""
        try:
            data = _airflow_request("/api/v2/monitor/health")
            metadb = data.get("metadatabase", {}).get("status", "unknown")
            scheduler = data.get("scheduler", {}).get("status", "unknown")
            ok = metadb == "healthy" and scheduler == "healthy"
            return jsonify({"ok": ok, "metadatabase": metadb, "scheduler": scheduler})
        except (URLError, HTTPError) as exc:
            return jsonify({"ok": False, "error": str(exc)}), 503

    @app.route("/api/airflow/trigger", methods=["POST"])
    def airflow_trigger():
        """Déclenche un nouveau run du DAG rfm_pipeline."""
        try:
            # Airflow 3 a rendu logical_date obligatoire dans le body
            # POST /api/v2/dags/{id}/dagRuns (sinon 422 Unprocessable Entity).
            logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            data = _airflow_request(
                f"/api/v2/dags/{AIRFLOW_DAG_ID}/dagRuns",
                method="POST",
                body={"conf": {}, "logical_date": logical_date},
            )
            return jsonify({
                "ok": True,
                "dag_run_id": data.get("dag_run_id"),
                "state": data.get("state"),
                "logical_date": data.get("logical_date"),
            })
        except HTTPError as exc:
            return jsonify({"ok": False, "error": f"HTTP {exc.code}: {exc.reason}"}), 502
        except URLError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 503

    @app.route("/api/airflow/runs/latest")
    def airflow_runs_latest():
        """Renvoie le dernier dag run + l'état de chacune de ses tasks."""
        try:
            runs = _airflow_request(
                f"/api/v2/dags/{AIRFLOW_DAG_ID}/dagRuns?limit=1&order_by=-logical_date"
            )
            dag_runs = runs.get("dag_runs", [])
            if not dag_runs:
                return jsonify({"ok": True, "run": None, "tasks": []})

            run = dag_runs[0]
            run_id = run["dag_run_id"]
            tasks_resp = _airflow_request(
                f"/api/v2/dags/{AIRFLOW_DAG_ID}/dagRuns/{run_id}/taskInstances"
            )
            tasks = [
                {
                    "task_id":   t["task_id"],
                    "state":     t["state"],
                    "start_date": t.get("start_date"),
                    "end_date":   t.get("end_date"),
                    "duration":   t.get("duration"),
                }
                for t in sorted(tasks_resp.get("task_instances", []),
                                key=lambda x: x.get("start_date") or "")
            ]
            return jsonify({
                "ok": True,
                "run": {
                    "dag_run_id":   run["dag_run_id"],
                    "state":        run["state"],
                    "logical_date": run.get("logical_date"),
                    "start_date":   run.get("start_date"),
                    "end_date":     run.get("end_date"),
                },
                "tasks": tasks,
            })
        except HTTPError as exc:
            return jsonify({"ok": False, "error": f"HTTP {exc.code}: {exc.reason}"}), 502
        except URLError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 503

    return app


# Exposé au top-level pour gunicorn (`gunicorn dashboard.app:app`)
app = create_app()


if __name__ == "__main__":
    # Mode dev local : `uv run python -m dashboard.app`
    app.run(host="0.0.0.0", port=8501, debug=True)
