"""
dashboard/charts.py — Builders Plotly pour le dashboard RFM
============================================================

Builders qui prennent un DataFrame et retournent une string JSON
sérialisée par `plotly.io.to_json`. Le template Jinja2 l'injecte
ensuite via `{{ ... | safe }}` puis `Plotly.newPlot(...)` côté client.

Pourquoi `pio.to_json` plutôt que `fig.to_dict() | tojson` ?
  - pio.to_json gère correctement les numpy arrays et les types
    pandas (datetime, Decimal) que tojson ne sait pas sérialiser.
  - Le résultat est du JSON pur — safe à injecter tel quel dans
    un <script> (pas de séquence </script> possible).

Sprint 4 : ajout des builders du dashboard v2 inspirés de JustDataPlease
(cf. docs/dashboard_v2_audit.md). Les 3 builders historiques (treemap,
heatmap RF, monetary boxplot) restent en haut du fichier, intacts.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots


# Palette utilisée pour le treemap : continue, lisible en clair et sombre.
COLORSCALE_TREEMAP = "Viridis"
COLORSCALE_HEATMAP = "Blues"

# Layout commun (transparent pour s'adapter aux thèmes Bootstrap clair/sombre).
COMMON_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=40, b=40),
    font=dict(size=13),
)


# ─────────────────────────────────────────────────────────────────────
# Palette stable pour les 11 segments (préfixés A.→K.)
#
# Inspirée de la palette Looker Studio du dashboard JustDataPlease :
# - LOYAL (CHAMPIONS, LOYAL, POTENTIAL_LOYALIST)  → teals/verts
# - PROMISING (RECENT, PROMISING, NEED_ATTENTION) → bleus
# - SLEEP (ABOUT_TO_SLEEP, AT_RISK, CANNOT_LOSE)  → orangés / saumons
# - LOST (HIBERNATING, LOST)                       → rouges
# ─────────────────────────────────────────────────────────────────────

SEGMENT_COLORS = {
    "A.CHAMPIONS":          "#2c7a4b",  # vert foncé
    "B.LOYAL":              "#3aa364",  # vert moyen
    "C.POTENTIAL_LOYALIST": "#7ac083",  # vert clair
    "D.RECENT_CUSTOMERS":   "#5089c6",  # bleu ciel
    "E.PROMISING":          "#3060a0",  # bleu foncé
    "F.NEED_ATTENTION":     "#6b4c9a",  # violet
    "G.ABOUT_TO_SLEEP":     "#f0a868",  # orange clair
    "H.AT_RISK":            "#e07b39",  # orange moyen
    "I.CANNOT_LOSE":        "#c95b27",  # orange foncé
    "J.HIBERNATING":        "#b94734",  # rouge brique
    "K.LOST":               "#8c2e1f",  # rouge foncé
}

# Couleur par dimension (pour les pages R/F/M deep-dive et leurs combo charts)
DIMENSION_COLORS = {
    "recency":   "#6b4c9a",  # violet
    "frequency": "#3060a0",  # bleu
    "monetary":  "#2c7a4b",  # teal/vert
}


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convertit un hex `#RRGGBB` en `rgba(r, g, b, alpha)`.

    Plotly refuse les hex 8 chars (`#RRGGBBAA`) ; il faut passer par rgba()
    pour appliquer une opacité sur les fillcolor des box / fill area.
    """
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


def _empty_figure(message: str) -> str:
    """Figure de repli quand le DataFrame est vide (DB vide, erreur SQL...)."""
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False,
                       font=dict(size=16, color="#888"))
    fig.update_layout(**COMMON_LAYOUT,
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# 1. Treemap des segments (taille = effectif, couleur = monetary moyen)
# ─────────────────────────────────────────────────────────────────────

def build_treemap(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty_figure("Aucune donnée — exécutez le pipeline ETL")

    fig = px.treemap(
        df,
        path=[px.Constant("Tous segments"), "segment"],
        values="n",
        color="avg_monetary",
        color_continuous_scale=COLORSCALE_TREEMAP,
        hover_data={"avg_monetary": ":,.0f", "avg_recency": ":.1f",
                    "avg_frequency": ":.1f"},
    )
    fig.update_traces(
        textinfo="label+value+percent root",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Clients : %{value}<br>"
            "Monetary moyen : %{color:,.0f} £<br>"
            "<extra></extra>"
        ),
    )
    fig.update_layout(**COMMON_LAYOUT,
                      coloraxis_colorbar=dict(title="£ moy."))
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# 2. Heatmap R × F (5×5)
# ─────────────────────────────────────────────────────────────────────

def build_rf_heatmap(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty_figure("Aucune donnée — exécutez le pipeline ETL")

    # On pivote pour avoir une vraie matrice 5×5 (sinon px.density_heatmap
    # interpole sur les bins, ce qui floute la grille des scores).
    pivot = df.pivot(index="f_score", columns="r_score", values="count").fillna(0)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[f"R{r}" for r in pivot.columns],
            y=[f"F{f}" for f in pivot.index],
            colorscale=COLORSCALE_HEATMAP,
            text=pivot.values.astype(int),
            texttemplate="%{text}",
            textfont=dict(size=14),
            hovertemplate="R=%{x}, F=%{y}<br>Clients : %{z}<extra></extra>",
            colorbar=dict(title="Clients"),
        )
    )
    fig.update_layout(**COMMON_LAYOUT,
                      xaxis_title="Recency score (5 = très récent)",
                      yaxis_title="Frequency score (5 = très fréquent)")
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# 3. Boxplot Monetary par segment
# ─────────────────────────────────────────────────────────────────────

def build_monetary_boxplot(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty_figure("Aucune donnée — exécutez le pipeline ETL")

    fig = px.box(
        df,
        x="segment",
        y="monetary",
        points="outliers",
        color="segment",
    )
    fig.update_layout(**COMMON_LAYOUT,
                      showlegend=False,
                      xaxis_title="Segment",
                      yaxis_title="Monetary (£) — échelle log",
                      yaxis_type="log",
                      xaxis_tickangle=-30)
    return pio.to_json(fig)


# ═════════════════════════════════════════════════════════════════════
# SPRINT 4 — Builders du dashboard v2
# ═════════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────────
# v2.1 — Bubble chart segment-level (page Overview 1.3)
# ─────────────────────────────────────────────────────────────────────

def build_bubble_segments(df: pd.DataFrame) -> str:
    """Bubble chart : X = ADSLT, Y = Total Revenue (log), size = users.

    1 bulle par segment, couleur stable via SEGMENT_COLORS, label texte
    sur chaque bulle pour l'identification rapide.
    """
    if df.empty:
        return _empty_figure("Aucune donnée — lancez le pipeline RFM")

    fig = px.scatter(
        df,
        x="adslt",
        y="total_revenue",
        size="total_users",
        color="segment_label",
        text="segment_label",
        size_max=60,
        log_y=True,
        color_discrete_map=SEGMENT_COLORS,
        labels={
            "adslt": "Récence moyenne (jours)",
            "total_revenue": "CA total du segment (£)",
            "total_users": "Clients",
            "segment_label": "Segment",
        },
        hover_data={"total_users": ":,", "total_revenue": ":,.0f"},
    )
    fig.update_traces(textposition="top center", textfont=dict(size=11))
    fig.update_layout(
        **COMMON_LAYOUT,
        showlegend=False,
        yaxis_tickformat=",.0f",
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v2.2 — Scatter user-level (page Overview 1.4)
# ─────────────────────────────────────────────────────────────────────

def build_scatter_sample(df: pd.DataFrame) -> str:
    """Scatter plot : 1 point par client (échantillon TABLESAMPLE 20 %)."""
    if df.empty:
        return _empty_figure("Aucune donnée — lancez le pipeline RFM")

    fig = px.scatter(
        df,
        x="recency",
        y="monetary",
        color="segment_label",
        log_x=True,
        log_y=True,
        color_discrete_map=SEGMENT_COLORS,
        opacity=0.6,
        labels={
            "recency": "Days since last transaction (log)",
            "monetary": "Monetary £ (log)",
            "segment_label": "Segment",
        },
        hover_data={"customer_id": True, "recency": True, "monetary": ":,.0f"},
    )
    fig.update_traces(marker=dict(size=6))
    fig.update_layout(
        **COMMON_LAYOUT,
        legend=dict(orientation="h", yanchor="bottom", y=-0.30, x=0, font=dict(size=10)),
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v2.3 — Combo chart factorisé (pages 4/5/6 — bar par bin + line %users)
# ─────────────────────────────────────────────────────────────────────

def build_combo_bin_chart(
    df: pd.DataFrame,
    metric_label: str,
    metric_axis_title: str,
    color: str,
) -> str:
    """Combo chart : barre par bin (avg metric) + ligne (% users).

    Réutilisable pour les 3 pages R/F/M en passant simplement le label de
    la métrique principale et la couleur de la dimension.

    DataFrame attendu : bin_label / users / pct_users / avg_metric
    (cf. queries.get_recency_bins / get_frequency_bins / get_monetary_bins)
    """
    if df.empty:
        return _empty_figure("Aucune donnée — lancez le pipeline RFM")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=df["bin_label"],
            y=df["avg_metric"],
            name=metric_label,
            marker_color=color,
            text=df["avg_metric"],
            texttemplate="%{text:,.1f}",
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>" + metric_label + " : %{y:,.1f}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=df["bin_label"],
            y=df["pct_users"],
            name="% utilisateurs",
            mode="lines+markers+text",
            line=dict(color="#222", width=2),
            marker=dict(size=10, color="#222"),
            text=df["pct_users"].map(lambda v: f"{v}%"),
            textposition="top center",
            textfont=dict(size=11, color="#222"),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}% des utilisateurs<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_layout(
        **COMMON_LAYOUT,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0),
        xaxis_title="",
        bargap=0.35,
    )
    fig.update_yaxes(title_text=metric_axis_title, secondary_y=False, showgrid=True)
    fig.update_yaxes(
        title_text="% utilisateurs",
        secondary_y=True,
        showgrid=False,
        ticksuffix="%",
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v2.4 — Box plot factorisé (pages 4/5/6)
# ─────────────────────────────────────────────────────────────────────

def build_distribution_box(
    df: pd.DataFrame,
    column: str,
    title: str,
    color: str,
    log_y: bool = False,
) -> str:
    """Box plot vertical d'une distribution univariée (R, F ou M)."""
    if df.empty or column not in df.columns:
        return _empty_figure("Aucune donnée")

    fig = go.Figure()
    fig.add_trace(
        go.Box(
            y=df[column],
            name=title,
            boxpoints="outliers",
            marker_color=color,
            line=dict(color=color),
            fillcolor=_hex_to_rgba(color, 0.25),
        )
    )
    fig.update_layout(
        **COMMON_LAYOUT,
        showlegend=False,
        yaxis_title=title,
        yaxis_type="log" if log_y else "linear",
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v2.5 — Line CA mensuel (bonus B6)
# ─────────────────────────────────────────────────────────────────────

def build_monthly_revenue(df: pd.DataFrame) -> str:
    """Line chart du CA mensuel sur toute la période (24 mois)."""
    if df.empty:
        return _empty_figure("Aucune donnée")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["month"],
            y=df["revenue"],
            mode="lines+markers",
            name="CA mensuel",
            line=dict(color="#2c7a4b", width=2),
            marker=dict(size=7, color="#2c7a4b"),
            fill="tozeroy",
            fillcolor="rgba(44,122,75,0.15)",
            hovertemplate="<b>%{x|%B %Y}</b><br>CA : %{y:,.0f} £<extra></extra>",
        )
    )
    fig.update_layout(
        **COMMON_LAYOUT,
        xaxis_title="Mois",
        yaxis_title="CA mensuel (£)",
        showlegend=False,
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v2.6 — Helpers Jinja (pas Plotly mais centralisés ici pour la cohérence)
# ─────────────────────────────────────────────────────────────────────

def group_top_products_by_segment(df: pd.DataFrame) -> dict:
    """Pivote la sortie SQL `get_top_products_by_segment` en dict
    {segment_label: [list of dict]} pour faciliter l'itération Jinja.
    """
    if df.empty:
        return {}
    return {
        seg: g[["stock_code", "description", "revenue"]].to_dict(orient="records")
        for seg, g in df.groupby("segment_label", sort=True)
    }


# ═════════════════════════════════════════════════════════════════════
# SPRINT 5 — Builders pour les pages Movements + Cohorts
# ═════════════════════════════════════════════════════════════════════


# Palette des 4 macro segments (utilisée par Movements + Cohorts pivot)
MACRO_COLORS = {
    "A.LOYAL":     "#2c7a4b",  # vert / teal
    "B.PROMISING": "#3060a0",  # bleu
    "C.SLEEP":     "#6b4c9a",  # violet
    "D.LOST":      "#c95b27",  # orange
}


# ─────────────────────────────────────────────────────────────────────
# v3.1 — Stacked column "% Users per Macro Segment by Month"
# ─────────────────────────────────────────────────────────────────────

def build_macro_movements_pct(df: pd.DataFrame) -> str:
    """100% stacked column de la composition macro par snapshot."""
    if df.empty:
        return _empty_figure("Aucune donnée — relancez le DAG Airflow")

    fig = go.Figure()
    for macro in ["A.LOYAL", "B.PROMISING", "C.SLEEP", "D.LOST"]:
        sub = df[df["macro_segment"] == macro]
        fig.add_trace(
            go.Bar(
                x=sub["snapshot_date"],
                y=sub["pct_users"],
                name=macro,
                marker_color=MACRO_COLORS[macro],
                hovertemplate="<b>%{x|%b %Y}</b><br>"
                              + macro
                              + " : %{y:.1f}%<extra></extra>",
            )
        )
    fig.update_layout(
        **COMMON_LAYOUT,
        barmode="stack",
        xaxis_title="Mois",
        yaxis_title="% utilisateurs",
        yaxis_range=[0, 100],
        yaxis_ticksuffix="%",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0),
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v3.2 — Stacked column "Users per Macro Segment by Month" (absolu)
# ─────────────────────────────────────────────────────────────────────

def build_macro_movements_abs(df: pd.DataFrame) -> str:
    """Stacked column en valeurs absolues de la base clients par macro."""
    if df.empty:
        return _empty_figure("Aucune donnée — relancez le DAG Airflow")

    fig = go.Figure()
    for macro in ["A.LOYAL", "B.PROMISING", "C.SLEEP", "D.LOST"]:
        sub = df[df["macro_segment"] == macro]
        fig.add_trace(
            go.Bar(
                x=sub["snapshot_date"],
                y=sub["n_users"],
                name=macro,
                marker_color=MACRO_COLORS[macro],
                hovertemplate="<b>%{x|%b %Y}</b><br>"
                              + macro
                              + " : %{y:,} clients<extra></extra>",
            )
        )
    fig.update_layout(
        **COMMON_LAYOUT,
        barmode="stack",
        xaxis_title="Mois",
        yaxis_title="Nombre de clients",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0),
    )
    return pio.to_json(fig)


# ─────────────────────────────────────────────────────────────────────
# v3.3 — Line chart "User Acquisitions Trend Line"
# ─────────────────────────────────────────────────────────────────────

def build_acquisitions_trend(df: pd.DataFrame) -> str:
    """Réplique chart 3.1 du dashboard JDP : line chart des acquisitions
    mensuelles."""
    if df.empty:
        return _empty_figure("Aucune donnée — relancez le pipeline RFM")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["acquisition_month"],
            y=df["user_acquisitions"],
            mode="lines+markers+text",
            text=df["user_acquisitions"],
            textposition="top center",
            textfont=dict(size=10),
            line=dict(color="#222", width=2),
            marker=dict(size=8, color="#222"),
            hovertemplate="<b>%{x|%B %Y}</b><br>"
                          "%{y:,} nouveaux clients<extra></extra>",
        )
    )
    fig.update_layout(
        **COMMON_LAYOUT,
        xaxis_title="Mois d'acquisition",
        yaxis_title="Nouveaux clients",
        showlegend=False,
    )
    return pio.to_json(fig)
