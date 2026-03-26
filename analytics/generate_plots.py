"""
analytics/generate_plots.py
============================
Standalone script that queries the database directly and saves every analytics
chart as a static PNG image under analytics_output/.

Uses direct psycopg2 queries (bypassing the slow correlated-subquery path in
the flow layer's get_area_summary) while reusing the same SQL metric
expressions and spatial joins from SpitogatosDAO.

Usage (with venv activated):
    python analytics/generate_plots.py

Or from inside Docker:
    docker exec realestateai-api python analytics/generate_plots.py
"""

import sys
import os

# Make the repo root importable regardless of invocation directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib  # noqa: E402  (must be before pyplot)
matplotlib.use("Agg")  # headless – no display required

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from decimal import Decimal
from pathlib import Path

from database.connection import get_db_connection

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent.parent / "analytics_output"

METRICS = [
    "upload_time",
    "floor_number",
    "price",
    "sqm",
    "price_per_sqm",
    "new_development",
]

METRIC_LABELS = {
    "upload_time": "Listing Age (days)",
    "floor_number": "Floor Number",
    "price": "Price (€)",
    "sqm": "Area (sqm)",
    "price_per_sqm": "Price per sqm (€/sqm)",
    "new_development": "New Development (0/1)",
}

# SQL expression for each metric (table alias = s)
METRIC_SQL = {
    "upload_time": "EXTRACT(EPOCH FROM (NOW() - s.website_uploaded)) / 86400.0",
    "floor_number": "s.floor_number::double precision",
    "price": "s.price::double precision",
    "sqm": "s.sqm::double precision",
    "price_per_sqm": "s.price::double precision / NULLIF(s.sqm, 0)",
    "new_development": "s.new_development::double precision",
}

# Relationship pairs: (x_metric, y_metric, output_filename_stem)
REL_PAIRS = [
    ("sqm", "price", "price_vs_sqm"),
    ("floor_number", "price_per_sqm", "price_per_sqm_vs_floor"),
    ("upload_time", "price", "price_vs_age"),
]

MAX_AREAS_PER_CHART = 20   # cap legend size for readability
N_BUCKETS = 20             # histogram buckets
TREND_GRANULARITY = "month"
RELATIONSHIP_SAMPLE = 3000
DPI = 150
STYLE = "seaborn-v0_8-whitegrid"
SQM_MIN = 30
SQM_MAX = 200


def _sqm_range_predicate(alias: str = "s") -> str:
    # Shared filter for every query that sources listing rows.
    return f"{alias}.sqm > {SQM_MIN} AND {alias}.sqm < {SQM_MAX}"


# ---------------------------------------------------------------------------
# Data fetching helpers (direct SQL, no correlated subqueries)
# ---------------------------------------------------------------------------

def _to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def fetch_summary_df(db, metric: str) -> pd.DataFrame:
    """
    Fast per-area summary statistics using window-function aggregates only
    (no correlated subqueries).  Returns a DataFrame with one row per area.
    """
    expr = METRIC_SQL[metric]
    sqm_filter = _sqm_range_predicate("s")
    query = f"""
    WITH vals AS (
        SELECT
            'neighborhood'::text AS area_type,
            n.name_en            AS area_name,
            ({expr})             AS v
        FROM spitogatos_data s
        JOIN geography.athens_neighborhood n
          ON ST_Contains(n.geom, s.location::geometry)
        WHERE ({expr}) IS NOT NULL
          AND {sqm_filter}

        UNION ALL

        SELECT
            'municipality'::text AS area_type,
            m.name_en            AS area_name,
            ({expr})             AS v
        FROM spitogatos_data s
        JOIN geography.attica_municipality m
          ON ST_Contains(m.geom, s.location::geometry)
        WHERE ({expr}) IS NOT NULL
          AND {sqm_filter}
    )
    SELECT
        area_type,
        area_name,
        COUNT(*)                                                             AS n,
        MIN(v)                                                               AS min,
        MAX(v)                                                               AS max,
        AVG(v)                                                               AS mean,
        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY v)                     AS p10,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)                     AS p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY v)                     AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)                     AS p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY v)                     AS p90,
        STDDEV_SAMP(v)                                                       AS stddev,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)
          - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)                 AS iqr,
        CASE WHEN AVG(v) <> 0
             THEN STDDEV_SAMP(v) / ABS(AVG(v))
             ELSE NULL END                                                   AS cv
    FROM vals
    WHERE area_name IS NOT NULL
    GROUP BY area_type, area_name
    ORDER BY area_type, n DESC
    """
    rows = db.execute_query(query)
    if not rows:
        return pd.DataFrame()
    data = [dict(r) for r in rows]
    df = pd.DataFrame(data)
    for col in df.columns:
        if col not in ("area_type", "area_name"):
            df[col] = df[col].apply(_to_float)
    df["n"] = df["n"].astype(int)
    return df


def fetch_distribution(db, metric: str, n_buckets: int = N_BUCKETS):
    """
    Return histogram bucket counts per area.
    Result: dict with keys 'bucket_edges', 'bucket_labels', 'series'.
    series = list of {area_type, area_name, buckets: [{bucket_index, count}]}
    """
    expr = METRIC_SQL[metric]
    sqm_filter = _sqm_range_predicate("s")

    if metric == "new_development":
        # discrete 0/1 – skip bucketing
        query = f"""
        WITH vals AS (
            SELECT 'neighborhood'::text AS area_type, n.name_en AS area_name,
                   ({expr})::int AS bucket_index
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON ST_Contains(n.geom, s.location::geometry)
            WHERE ({expr}) IS NOT NULL
              AND {sqm_filter}
            UNION ALL
            SELECT 'municipality'::text, m.name_en, ({expr})::int
            FROM spitogatos_data s
            JOIN geography.attica_municipality m
              ON ST_Contains(m.geom, s.location::geometry)
            WHERE ({expr}) IS NOT NULL
              AND {sqm_filter}
        )
        SELECT area_type, area_name, bucket_index, COUNT(*) AS cnt
        FROM vals
        GROUP BY area_type, area_name, bucket_index
        ORDER BY area_type, area_name, bucket_index
        """
        rows = db.execute_query(query)
        bucket_edges = [0.0, 0.5, 1.0]
        bucket_labels = ["No", "Yes"]
    else:
        query = f"""
        WITH global_range AS (
            SELECT MIN(v) AS lo, MAX(v) AS hi
            FROM (
                SELECT ({expr}) AS v
                FROM spitogatos_data s
                WHERE ({expr}) IS NOT NULL
                  AND {sqm_filter}
            ) t
        ),
        vals AS (
            SELECT 'neighborhood'::text AS area_type, n.name_en AS area_name,
                   width_bucket(({expr}), g.lo, g.hi + 1, {n_buckets}) AS bucket_index
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON ST_Contains(n.geom, s.location::geometry)
            CROSS JOIN global_range g
            WHERE ({expr}) IS NOT NULL
              AND {sqm_filter}
            UNION ALL
            SELECT 'municipality'::text, m.name_en,
                   width_bucket(({expr}), g.lo, g.hi + 1, {n_buckets})
            FROM spitogatos_data s
            JOIN geography.attica_municipality m
              ON ST_Contains(m.geom, s.location::geometry)
            CROSS JOIN global_range g
            WHERE ({expr}) IS NOT NULL
              AND {sqm_filter}
        ),
        edges AS (
            SELECT
                lo + (hi - lo + 1) / {n_buckets}.0 * i AS edge
            FROM global_range,
                 generate_series(0, {n_buckets}) AS gs(i)
        )
        SELECT area_type, area_name, bucket_index, COUNT(*) AS cnt
        FROM vals
        WHERE bucket_index BETWEEN 1 AND {n_buckets}
        GROUP BY area_type, area_name, bucket_index
        ORDER BY area_type, area_name, bucket_index
        """
        # Fetch edges separately
        edge_query = f"""
        SELECT
            MIN(v) + (MAX(v) - MIN(v) + 1) / {n_buckets}.0 * gs.i AS edge
        FROM (
            SELECT ({expr}) AS v
            FROM spitogatos_data s
            WHERE ({expr}) IS NOT NULL
              AND {sqm_filter}
        ) t,
        generate_series(0, {n_buckets}) AS gs(i)
        GROUP BY gs.i
        ORDER BY gs.i
        """
        edge_rows = db.execute_query(edge_query)
        bucket_edges = [_to_float(r["edge"]) for r in edge_rows]
        bucket_labels = [
            f"{bucket_edges[i]:.0f}–{bucket_edges[i+1]:.0f}"
            for i in range(len(bucket_edges) - 1)
        ]
        rows = db.execute_query(query)

    # Group rows by area
    area_map: dict = {}
    for r in rows:
        key = (str(r["area_type"]), str(r["area_name"]))
        area_map.setdefault(key, []).append({"bucket_index": int(r["bucket_index"]), "count": int(r["cnt"])})

    series = [
        {"area_type": k[0], "area_name": k[1], "buckets": v}
        for k, v in area_map.items()
    ]
    return {"bucket_edges": bucket_edges, "bucket_labels": bucket_labels, "series": series}


def fetch_trend(db, metric: str, granularity: str = TREND_GRANULARITY):
    """
    Monthly median trend per area.
    Returns list of {area_type, area_name, period, median, mean, p25, p75, n}.
    """
    expr = METRIC_SQL[metric]
    sqm_filter = _sqm_range_predicate("s")
    query = f"""
    WITH vals AS (
        SELECT 'neighborhood'::text AS area_type, n.name_en AS area_name,
               date_trunc('{granularity}', s.website_uploaded) AS period,
               ({expr}) AS v
        FROM spitogatos_data s
        JOIN geography.athens_neighborhood n
          ON ST_Contains(n.geom, s.location::geometry)
        WHERE ({expr}) IS NOT NULL
          AND s.website_uploaded IS NOT NULL
          AND {sqm_filter}
        UNION ALL
        SELECT 'municipality'::text, m.name_en,
               date_trunc('{granularity}', s.website_uploaded),
               ({expr})
        FROM spitogatos_data s
        JOIN geography.attica_municipality m
          ON ST_Contains(m.geom, s.location::geometry)
        WHERE ({expr}) IS NOT NULL
          AND s.website_uploaded IS NOT NULL
          AND {sqm_filter}
    )
    SELECT
        area_type, area_name,
        TO_CHAR(period, 'YYYY-MM') AS period,
        COUNT(*)                                                             AS n,
        AVG(v)                                                               AS mean,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY v)                     AS median,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)                     AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)                     AS p75
    FROM vals
    WHERE area_name IS NOT NULL AND period IS NOT NULL
    GROUP BY area_type, area_name, period
    ORDER BY area_type, area_name, period
    """
    rows = db.execute_query(query)
    return [
        {
            "area_type": r["area_type"],
            "area_name": r["area_name"],
            "period": r["period"],
            "n": int(r["n"]),
            "mean": _to_float(r["mean"]),
            "median": _to_float(r["median"]),
            "p25": _to_float(r["p25"]),
            "p75": _to_float(r["p75"]),
        }
        for r in rows
        if r["area_type"] and r["area_name"] and r["period"]
    ]


def fetch_relationship(db, x_metric: str, y_metric: str, limit: int = RELATIONSHIP_SAMPLE):
    """
    Sample (x, y) pairs with neighborhood label for scatter plots.
    """
    x_expr = METRIC_SQL[x_metric]
    y_expr = METRIC_SQL[y_metric]
    sqm_filter = _sqm_range_predicate("s")
    query = f"""
    SELECT
        n.name_en AS area_name,
        'neighborhood'::text AS area_type,
        ({x_expr}) AS x,
        ({y_expr}) AS y
    FROM spitogatos_data s
    JOIN geography.athens_neighborhood n
      ON ST_Contains(n.geom, s.location::geometry)
    WHERE ({x_expr}) IS NOT NULL
      AND ({y_expr}) IS NOT NULL
      AND {sqm_filter}
    ORDER BY RANDOM()
    LIMIT {limit}
    """
    rows = db.execute_query(query)
    return [
        {"area_name": r["area_name"], "area_type": r["area_type"],
         "x": _to_float(r["x"]), "y": _to_float(r["y"])}
        for r in rows
        if r["x"] is not None and r["y"] is not None
    ]


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

def _get_color_cycle(n: int):
    try:
        cmap = matplotlib.colormaps.get_cmap("tab20").resampled(max(n, 1))
    except AttributeError:
        cmap = cm.get_cmap("tab20", max(n, 1))  # matplotlib < 3.7 fallback
    return [mcolors.to_hex(cmap(i)) for i in range(n)]


def _savefig(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"    saved -> {path.relative_to(OUTPUT_DIR.parent)}")


def _top_area_names(summary_df: pd.DataFrame, limit: int = MAX_AREAS_PER_CHART):
    if summary_df.empty:
        return []
    return summary_df.nlargest(limit, "n")["area_name"].tolist()


# ---------------------------------------------------------------------------
# Distribution plots
# ---------------------------------------------------------------------------

def plot_distribution_overlay(metric: str, dist_data: dict, summary_df: pd.DataFrame, out_dir: Path):
    label = METRIC_LABELS[metric]
    series = dist_data["series"]
    if not series:
        print(f"    [skip] no distribution data")
        return

    top_names = set(_top_area_names(summary_df))
    series = [s for s in series if s["area_name"] in top_names]
    if not series:
        return

    bucket_labels = dist_data["bucket_labels"]
    colors = _get_color_cycle(len(series))

    fig, ax = plt.subplots(figsize=(13, 6))
    for s, color in zip(series, colors):
        buckets = sorted(s["buckets"], key=lambda b: b["bucket_index"])
        if not buckets:
            continue
        counts = np.array([b["count"] for b in buckets], dtype=float)
        total = counts.sum()
        density = counts / total if total > 0 else counts
        x_idx = [b["bucket_index"] - 1 for b in buckets]
        ax.plot(x_idx, density, label=s["area_name"], color=color, linewidth=1.4)

    tick_step = max(1, len(bucket_labels) // 10)
    ax.set_xticks(range(0, len(bucket_labels), tick_step))
    ax.set_xticklabels(bucket_labels[::tick_step], rotation=30, ha="right", fontsize=7)
    ax.set_xlabel(label)
    ax.set_ylabel("Normalised frequency")
    ax.set_title(f"{label} – Distribution Overlay (top {MAX_AREAS_PER_CHART} areas by count)")
    ax.legend(fontsize=7, ncol=2, loc="upper right")
    fig.tight_layout()
    _savefig(fig, out_dir / "distribution.png")


def plot_ecdf(metric: str, dist_data: dict, summary_df: pd.DataFrame, out_dir: Path):
    label = METRIC_LABELS[metric]
    series = dist_data["series"]
    if not series:
        return

    top_names = set(_top_area_names(summary_df))
    series = [s for s in series if s["area_name"] in top_names]
    if not series:
        return

    bucket_labels = dist_data["bucket_labels"]
    colors = _get_color_cycle(len(series))

    fig, ax = plt.subplots(figsize=(13, 6))
    for s, color in zip(series, colors):
        buckets = sorted(s["buckets"], key=lambda b: b["bucket_index"])
        if not buckets:
            continue
        counts = np.array([b["count"] for b in buckets], dtype=float)
        total = counts.sum()
        if total == 0:
            continue
        cdf = np.cumsum(counts) / total
        x_idx = [b["bucket_index"] - 1 for b in buckets]
        ax.plot(x_idx, cdf, label=s["area_name"], color=color, linewidth=1.4)

    tick_step = max(1, len(bucket_labels) // 10)
    ax.set_xticks(range(0, len(bucket_labels), tick_step))
    ax.set_xticklabels(bucket_labels[::tick_step], rotation=30, ha="right", fontsize=7)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax.set_xlabel(label)
    ax.set_ylabel("Cumulative %")
    ax.set_title(f"{label} – Empirical CDF (top {MAX_AREAS_PER_CHART} areas)")
    ax.legend(fontsize=7, ncol=2, loc="lower right")
    fig.tight_layout()
    _savefig(fig, out_dir / "ecdf.png")


def plot_boxplot(metric: str, summary_df: pd.DataFrame, out_dir: Path):
    label = METRIC_LABELS[metric]
    df = summary_df.dropna(subset=["p25", "median", "p75", "min", "max"])
    if df.empty:
        print(f"    [skip] no percentile data for boxplot")
        return

    df = df.nlargest(MAX_AREAS_PER_CHART, "n")
    bxp_stats = [
        {
            "med": row["median"], "q1": row["p25"], "q3": row["p75"],
            "whislo": row["min"], "whishi": row["max"],
            "fliers": [], "label": row["area_name"],
        }
        for _, row in df.iterrows()
    ]

    fig, ax = plt.subplots(figsize=(max(10, len(bxp_stats) * 0.65), 7))
    bp = ax.bxp(bxp_stats, showfliers=False, patch_artist=True, vert=True)
    colors = _get_color_cycle(len(bxp_stats))
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    ax.set_xticklabels([s["label"] for s in bxp_stats], rotation=45, ha="right", fontsize=7)
    ax.set_ylabel(label)
    ax.set_title(f"{label} – Box Plot by Area (top {MAX_AREAS_PER_CHART} areas)")
    fig.tight_layout()
    _savefig(fig, out_dir / "boxplot.png")


# ---------------------------------------------------------------------------
# Summary table image
# ---------------------------------------------------------------------------

def plot_summary_table(metric: str, summary_df: pd.DataFrame, out_dir: Path):
    label = METRIC_LABELS[metric]
    if summary_df.empty:
        return

    df = summary_df.nlargest(40, "n").reset_index(drop=True)

    def fmt(v):
        if v is None:
            return "N/A"
        try:
            f = float(v)
        except (TypeError, ValueError):
            return str(v)
        if np.isnan(f):
            return "N/A"
        if isinstance(v, int):
            return f"{v:,}"
        return f"{f:,.1f}"

    col_keys = ["area_name", "n", "min", "p25", "median", "mean", "p75", "p90", "max", "stddev", "iqr", "cv"]
    col_headers = ["Area", "N", "Min", "P25", "Median", "Mean", "P75", "P90", "Max", "Std", "IQR", "CV"]

    table_data = [
        [fmt(row.get(k)) if k != "n" else f"{int(row['n']):,}" for k in col_keys]
        for _, row in df.iterrows()
    ]

    n_rows = len(table_data)
    n_cols = len(col_headers)
    fig_h = max(4, 0.3 * n_rows + 1.5)
    fig_w = max(14, n_cols * 1.3)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")
    tbl = ax.table(cellText=table_data, colLabels=col_headers, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.auto_set_column_width(col=list(range(n_cols)))

    for j in range(n_cols):
        cell = tbl[0, j]
        cell.set_facecolor("#2C3E50")
        cell.set_text_props(color="white", fontweight="bold")

    for i in range(1, n_rows + 1):
        bg = "#F2F4F4" if i % 2 == 0 else "white"
        for j in range(n_cols):
            tbl[i, j].set_facecolor(bg)

    ax.set_title(f"{label} – Summary Statistics by Area", pad=10, fontsize=11, fontweight="bold")
    fig.tight_layout()
    _savefig(fig, out_dir / "summary_table.png")


# ---------------------------------------------------------------------------
# Monthly trend plot
# ---------------------------------------------------------------------------

def plot_trend_monthly(metric: str, trend_rows: list, summary_df: pd.DataFrame, out_dir: Path):
    label = METRIC_LABELS[metric]
    if not trend_rows:
        print(f"    [skip] no trend data")
        return

    # Group by area
    area_map: dict = {}
    for r in trend_rows:
        key = r["area_name"]
        area_map.setdefault(key, []).append(r)

    # Top areas by total listing count from summary
    top_names = set(_top_area_names(summary_df))
    area_map = {k: v for k, v in area_map.items() if k in top_names}
    if not area_map:
        return

    # Sort by volume
    area_sorted = sorted(area_map.items(), key=lambda kv: sum(p["n"] for p in kv[1]), reverse=True)
    colors = _get_color_cycle(len(area_sorted))

    # Build a shared sorted period list and map to numeric indices to
    # avoid matplotlib's buggy date auto-detection on "YYYY-MM" strings.
    all_periods = sorted({r["period"] for r in trend_rows if r["period"]})
    period_idx = {p: i for i, p in enumerate(all_periods)}

    fig, ax = plt.subplots(figsize=(14, 6))

    for (name, pts), color in zip(area_sorted, colors):
        pts_sorted = sorted(pts, key=lambda p: p["period"])
        x_num = [period_idx[p["period"]] for p in pts_sorted if p["period"] in period_idx]
        y = [p["median"] if p["median"] is not None else p["mean"] for p in pts_sorted]
        valid = [(xi, yi) for xi, yi in zip(x_num, y) if yi is not None]
        if not valid:
            continue
        vx, vy = zip(*valid)
        ax.plot(vx, vy, label=name, color=color, linewidth=1.5, marker="o", markersize=3)

        # IQR band
        band_pts = [(period_idx[p["period"]], p["p25"], p["p75"]) for p in pts_sorted
                    if p["period"] in period_idx and p["p25"] is not None and p["p75"] is not None]
        if len(band_pts) >= 2:
            bx, blo, bhi = zip(*band_pts)
            ax.fill_between(bx, blo, bhi, color=color, alpha=0.08)

    tick_step = max(1, len(all_periods) // 12)
    tick_positions = list(range(0, len(all_periods), tick_step))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([all_periods[i] for i in tick_positions], rotation=30, ha="right", fontsize=7)
    ax.set_xlabel("Month")
    ax.set_ylabel(label)
    ax.set_title(f"{label} - Monthly Trend (median +/- IQR, top {MAX_AREAS_PER_CHART} areas)")
    ax.legend(fontsize=7, ncol=2, loc="upper left")
    fig.tight_layout()
    _savefig(fig, out_dir / "trend_monthly.png")


# ---------------------------------------------------------------------------
# Scatter relationship plot
# ---------------------------------------------------------------------------

def plot_relationship_scatter(x_metric: str, y_metric: str, points: list, out_stem: str, out_dir: Path):
    x_label = METRIC_LABELS[x_metric]
    y_label = METRIC_LABELS[y_metric]
    if not points:
        print(f"    [skip] no relationship data")
        return

    area_map: dict = {}
    for pt in points:
        name = pt.get("area_name") or "unknown"
        area_map.setdefault(name, []).append((pt["x"], pt["y"]))

    area_sorted = sorted(area_map.items(), key=lambda kv: len(kv[1]), reverse=True)[:MAX_AREAS_PER_CHART]
    colors = _get_color_cycle(len(area_sorted))

    fig, ax = plt.subplots(figsize=(10, 7))
    for (name, pts), color in zip(area_sorted, colors):
        xs, ys = zip(*pts)
        ax.scatter(xs, ys, label=name, color=color, s=12, alpha=0.5, linewidths=0)

    all_x = np.array([pt["x"] for pt in points], dtype=float)
    all_y = np.array([pt["y"] for pt in points], dtype=float)
    mask = np.isfinite(all_x) & np.isfinite(all_y)
    if mask.sum() >= 2:
        m, b = np.polyfit(all_x[mask], all_y[mask], 1)
        x_line = np.linspace(all_x[mask].min(), all_x[mask].max(), 200)
        ax.plot(x_line, m * x_line + b, color="black", linewidth=1.5, linestyle="--", label="OLS trend")

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(f"{y_label} vs {x_label}")
    ax.legend(fontsize=7, ncol=2, loc="upper left")
    fig.tight_layout()
    _savefig(fig, out_dir / f"{out_stem}.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import time as _time

    print("Connecting to database …")
    print(f"Applying sqm filter: s.sqm > {SQM_MIN} AND s.sqm < {SQM_MAX}")
    db = get_db_connection()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Per-metric plots
    # ------------------------------------------------------------------
    for metric in METRICS:
        print(f"\n{'='*60}")
        print(f"Metric: {metric}")
        print(f"{'='*60}")
        out = OUTPUT_DIR / metric
        out.mkdir(parents=True, exist_ok=True)

        t0 = _time.time()
        print(f"  fetching summary stats …", flush=True)
        try:
            summary_df = fetch_summary_df(db, metric)
        except Exception as exc:
            print(f"  [ERROR] summary query failed: {exc}")
            summary_df = pd.DataFrame()

        t1 = _time.time()
        print(f"  done in {t1-t0:.1f}s – {len(summary_df)} areas", flush=True)

        print(f"  fetching distribution …", flush=True)
        try:
            dist_data = fetch_distribution(db, metric)
        except Exception as exc:
            print(f"  [ERROR] distribution query failed: {exc}")
            dist_data = {"bucket_edges": [], "bucket_labels": [], "series": []}

        t2 = _time.time()
        print(f"  done in {t2-t1:.1f}s – {len(dist_data['series'])} series", flush=True)

        print(f"  fetching monthly trend …", flush=True)
        try:
            trend_rows = fetch_trend(db, metric)
        except Exception as exc:
            print(f"  [ERROR] trend query failed: {exc}")
            trend_rows = []

        t3 = _time.time()
        print(f"  done in {t3-t2:.1f}s – {len(trend_rows)} rows", flush=True)

        # --- plots ---
        print("  plotting distribution overlay …", flush=True)
        plot_distribution_overlay(metric, dist_data, summary_df, out)

        print("  plotting ECDF …", flush=True)
        plot_ecdf(metric, dist_data, summary_df, out)

        print("  plotting box plot …", flush=True)
        plot_boxplot(metric, summary_df, out)

        print("  plotting summary table …", flush=True)
        plot_summary_table(metric, summary_df, out)

        print("  plotting monthly trend …", flush=True)
        plot_trend_monthly(metric, trend_rows, summary_df, out)

    # ------------------------------------------------------------------
    # Relationship / scatter plots
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Relationship plots")
    print(f"{'='*60}")
    rel_out = OUTPUT_DIR / "relationships"
    rel_out.mkdir(parents=True, exist_ok=True)

    for x_m, y_m, stem in REL_PAIRS:
        print(f"  fetching {x_m} vs {y_m} …", flush=True)
        try:
            points = fetch_relationship(db, x_m, y_m)
        except Exception as exc:
            print(f"  [ERROR] relationship query failed: {exc}")
            continue
        print(f"  got {len(points)} points – plotting {stem} …", flush=True)
        plot_relationship_scatter(x_m, y_m, points, stem, rel_out)

    print(f"\nDone. All images saved under: {OUTPUT_DIR}")


if __name__ == "__main__":
    try:
        plt.style.use(STYLE)
    except OSError:
        pass
    main()
