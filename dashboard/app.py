from __future__ import annotations

from functools import lru_cache
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback_context, dcc, html
from dash.dash_table import DataTable
from dash.dash_table.Format import Format, Group, Scheme, Symbol

DATA_PATH = Path(__file__).resolve().parents[1] / "excel_db" / "all_assets.xlsx"


COORD_DMS_PATTERN = re.compile(
    r'(\d+(?:\.\d+)?)°\s*(\d+(?:\.\d+)?)\'\s*(\d+(?:\.\d+)?)"?\s*([NSEW])',
    re.IGNORECASE,
)
def dms_to_decimal(degrees: float, minutes: float, seconds: float, direction: str) -> float:
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction.upper() in {"S", "W"}:
        decimal *= -1
    return decimal


def parse_coordinate_pair(value: str):
    if not isinstance(value, str):
        return (None, None)
    text = value.strip()
    if not text:
        return (None, None)

    dms_matches = list(COORD_DMS_PATTERN.finditer(text))
    if len(dms_matches) >= 2:
        lat_match, lon_match = dms_matches[:2]
        lat = dms_to_decimal(
            float(lat_match.group(1)),
            float(lat_match.group(2)),
            float(lat_match.group(3)),
            lat_match.group(4),
        )
        lon = dms_to_decimal(
            float(lon_match.group(1)),
            float(lon_match.group(2)),
            float(lon_match.group(3)),
            lon_match.group(4),
        )
        return (lat, lon)

    if "," in text:
        parts = text.split(",")
        if len(parts) >= 2:
            return (
                pd.to_numeric(parts[0], errors="coerce"),
                pd.to_numeric(parts[1], errors="coerce"),
            )

    return (None, None)


@lru_cache(maxsize=1)
def load_dataset() -> pd.DataFrame:
    df = pd.read_excel(DATA_PATH)
    df = df.copy()

    # Use existing lat/lon columns if available, otherwise parse from coords
    if "lat" not in df.columns or df["lat"].isna().all():
        if "lon" not in df.columns or df["lon"].isna().all():
            if "coords" in df.columns:
                coord_pairs = df["coords"].apply(parse_coordinate_pair)
                df["lat"] = coord_pairs.apply(lambda pair: pair[0])
                df["lon"] = coord_pairs.apply(lambda pair: pair[1])
    
    # Ensure lat/lon are numeric
    df["lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
    df["lon"] = pd.to_numeric(df.get("lon"), errors="coerce")

    df["price_per_sqm"] = pd.to_numeric(df.get("price/sqm"), errors="coerce")
    df["comparison_average"] = pd.to_numeric(df.get("comparison_average"), errors="coerce")
    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["sqm"] = pd.to_numeric(df.get("sqm"), errors="coerce")
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")  # Changed from "Price" to "price"
    df["AuctionDate"] = pd.to_datetime(df.get("AuctionDate"), errors="coerce")
    df["searched_radius"] = pd.to_numeric(df.get("searched_radius"), errors="coerce")
    df["#assets"] = pd.to_numeric(df.get("#assets"), errors="coerce")
    df["comparison_min"] = pd.to_numeric(df.get("comparison_min"), errors="coerce")
    df["comparison_average"] = pd.to_numeric(df.get("comparison_average"), errors="coerce")
    df["comparison_median"] = pd.to_numeric(df.get("comparison_median"), errors="coerce")
    df["comparison_max"] = pd.to_numeric(df.get("comparison_max"), errors="coerce")

    # Changed from "precent_under_market" to "price_under_market"
    df["price-market_discount"] = pd.to_numeric(df.get("price_under_market"), errors="coerce") * 100
    comparison_safe = df["comparison_average"].replace({0: pd.NA})
    df["price_avg_discount_pct"] = (
        (comparison_safe - df["price_per_sqm"]) / comparison_safe
    ) * 100

    # Use already translated columns from the Excel file; no further translation or parsing needed.
    df["portfolio_label"] = df["Portfolio"].fillna("Unknown")
    df["category_label"] = df["Category"].fillna("Unspecified")
    df["municipality_label"] = df["Municipality"].fillna("—")
    df["title_display"] = df["Title"].fillna("")
    # Load DebtorDescr column from Excel as description, fill NaN with empty string
    if "DebtorDescr" in df.columns:
        df["description"] = df["DebtorDescr"].fillna("")
    else:
        df["description"] = ""

    return df


df = load_dataset()


def safe_int(value, fallback):
    return int(value) if pd.notna(value) else fallback


def safe_float(value, fallback):
    return float(value) if pd.notna(value) else fallback


portfolio_options = [{"label": name, "value": name} for name in sorted(df["portfolio_label"].unique())]

# Detect source column name once (prefer lowercase 'source', then 'Source')
SOURCE_COLUMN = "source" if "source" in df.columns else ("Source" if "Source" in df.columns else None)
if SOURCE_COLUMN is not None:
    source_options = [{"label": name, "value": name} for name in sorted(df[SOURCE_COLUMN].dropna().unique())]
else:
    source_options = []

municipality_options = [{"label": name, "value": name} for name in sorted(df["municipality_label"].unique())]

price_min = safe_float(df["price_per_sqm"].min(skipna=True), 0.0)
price_max = safe_float(df["price_per_sqm"].max(skipna=True), 1.0)
discount_min = safe_float(df["price-market_discount"].min(skipna=True), -100.0)
discount_max = safe_float(df["price-market_discount"].max(skipna=True), 100.0)

# Create integer marks - only show min and max to avoid decimal clutter
price_marks = {
    price_min: f"{int(round(price_min)):,}",
    price_max: f"{int(round(price_max)):,}"
}
discount_marks = {
    discount_min: f"{int(round(discount_min))}",
    discount_max: f"{int(round(discount_max))}"
}

app = Dash(
    __name__,
    title="VAR Opportunity Explorer",
    external_stylesheets=[dbc.themes.MINTY],
)
server = app.server

CUSTOM_BODY_STYLE = """
body {
    background-color: #d48eb8;
}
.link-btn {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 999px;
    font-size: 13px;
    text-decoration: none;
    color: #ffffff !important;
    font-weight: 600;
    background-color: #1d7d8d;
}
.link-btn.secondary {
    background-color: #23b5b5;
}
"""

app.index_string = f"""
<!DOCTYPE html>
<html lang="en">
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{CUSTOM_BODY_STYLE}</style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
"""

CARD_BG = "#ffffff"
BACKGROUND = "#bccbe0"
ACCENT = "#1d7d8d"
ACCENT_SOFT = "#23b5b5"
TEXT_MUTED = "#6c757d"
COLOR_SEQUENCE = px.colors.qualitative.Prism
COLOR_SCALE = px.colors.sequential.Tealgrn

px.defaults.template = "plotly_white"

# Geography map: polygon + statistics (see API /geography/* routes)
GEOGRAPHY_API_BASE = os.environ.get("REALESTATE_API_BASE", "http://localhost:8000").rstrip("/")
GEO_LAYER_NEIGHBORHOODS = "neighborhoods"
GEO_LAYER_MUNICIPALITIES = "municipalities"


def _geo_request_json(path: str) -> Tuple[Optional[Any], Optional[str]]:
    """GET JSON from the geography API. Returns (data, error_message)."""
    url = f"{GEOGRAPHY_API_BASE}{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "RealEstateAI-dashboard/1.0",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode()
            if resp.status != 200:
                return None, f"HTTP {resp.status} from {url}"
            return json.loads(raw), None
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode(errors="replace")
        except OSError:
            detail = str(e)
        if e.code == 404:
            return None, "not_found"
        return None, f"HTTP {e.code}: {detail[:300]}"
    except urllib.error.URLError as e:
        return None, f"Network error: {e.reason!s}"
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON: {e}"
    except Exception as e:
        return None, str(e)


def fetch_athens_neighborhoods_geojson() -> Tuple[Optional[dict], Optional[str]]:
    return _geo_request_json("/geography/athens-neighborhoods")


def fetch_attica_municipalities_geojson() -> Tuple[Optional[dict], Optional[str]]:
    return _geo_request_json("/geography/attica-municipalities")


def fetch_neighborhood_statistics(name_en: str) -> Tuple[Optional[dict], Optional[str]]:
    q = urllib.parse.urlencode({"neighborhood_name_en": name_en})
    return _geo_request_json(f"/geography/neighborhood-statistics?{q}")


def fetch_municipality_statistics(name_en: str) -> Tuple[Optional[dict], Optional[str]]:
    q = urllib.parse.urlencode({"municipality_name_en": name_en})
    return _geo_request_json(f"/geography/municipality-statistics?{q}")


def _geo_empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        annotations=[
            dict(
                text=message,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color=TEXT_MUTED),
            )
        ],
        xaxis_visible=False,
        yaxis_visible=False,
        paper_bgcolor=CARD_BG,
        margin=dict(l=0, r=0, t=0, b=0),
        height=420,
    )
    return fig


def _geojson_to_polygon_map_figure(geojson: Optional[dict], err: Optional[str], layer_label: str) -> go.Figure:
    if err:
        return _geo_empty_figure(f"Could not load {layer_label}: {err}")
    if not geojson:
        return _geo_empty_figure(f"No data for {layer_label}.")
    features = geojson.get("features") or []
    if not features:
        return _geo_empty_figure(f"No polygons returned for {layer_label}.")

    for i, feat in enumerate(features):
        props = feat.setdefault("properties", {})
        if not props.get("name_en"):
            props["name_en"] = f"__unnamed_{i}"

    ids: List[str] = []
    for feat in features:
        props = feat.get("properties") or {}
        ids.append(str(props.get("name_en", "")))

    plot_df = pd.DataFrame({"id": ids, "z": [1.0] * len(ids)})

    fig = px.choropleth_mapbox(
        plot_df,
        geojson=geojson,
        locations="id",
        color="z",
        featureidkey="properties.name_en",
        mapbox_style="carto-positron",
        color_continuous_scale=[[0, "#cfe8eb"], [1, "#1d7d8d"]],
        zoom=10,
        center={"lat": 37.9838, "lon": 23.7275},
        opacity=0.45,
        height=420,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        coloraxis_showscale=False,
        paper_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        mapbox=dict(
            style="carto-positron",
            center={"lat": 37.9838, "lon": 23.7275},
            zoom=10,
        ),
    )
    fig.update_traces(marker_line_width=0.8, marker_line_color="#333333")
    return fig


def _geo_stats_placeholder() -> html.Div:
    return html.Div(
        html.P(
            "Click a polygon to load statistics from the API.",
            className="text-muted mb-0 small",
        )
    )


def _geo_stats_error(message: str) -> html.Div:
    return html.Div(
        html.P(message, className="text-danger mb-0 small"),
    )


def _geo_stats_success(area_name: str, stats: dict) -> html.Div:
    def fmt_num(x: Any) -> str:
        if x is None:
            return "—"
        try:
            f = float(x)
            if abs(f) >= 1000 or (abs(f) > 0 and abs(f) < 0.01):
                return f"{f:,.4f}"
            return f"{f:,.2f}"
        except (TypeError, ValueError):
            return str(x)

    return html.Div(
        [
            html.H6(area_name, className="text-uppercase small mb-2", style={"color": TEXT_MUTED}),
            html.Ul(
                [
                    html.Li([html.Strong("No. assets: "), " ", f"{stats.get('no_assets', '—')}"]),
                    html.Li([html.Strong("Min: "), " ", fmt_num(stats.get("min"))]),
                    html.Li([html.Strong("Max: "), " ", fmt_num(stats.get("max"))]),
                    html.Li([html.Strong("Mean: "), " ", fmt_num(stats.get("mean"))]),
                    html.Li([html.Strong("Median: "), " ", fmt_num(stats.get("median"))]),
                    html.Li([html.Strong("Std: "), " ", fmt_num(stats.get("std"))]),
                ],
                className="small mb-0 ps-3",
            ),
        ]
    )


def make_link_button(label: str, url: str | None, variant: str = "primary") -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    css_class = "link-btn" if variant == "primary" else "link-btn secondary"
    safe_url = url.strip()
    return f"<a href='{safe_url}' target='_blank' class='{css_class}'>{label}</a>"


CURRENCY_FORMAT = Format(
    group=Group.yes,
    precision=0,
    scheme=Scheme.fixed,
    symbol=Symbol.yes,
    symbol_prefix="€",
)
INTEGER_FORMAT = Format(group=Group.yes, precision=0, scheme=Scheme.fixed)
PERCENT_FORMAT = Format(
    precision=1,
    scheme=Scheme.fixed,
    symbol=Symbol.yes,
    symbol_suffix="%",
)
DECIMAL_FORMAT = Format(precision=1, scheme=Scheme.fixed)


def kpi_card(title: str, value_id: str, suffix: str = "") -> dbc.Card:
    return dbc.Card(
        dbc.CardBody(
            [
                html.P(title, className="text-uppercase small mb-1", style={"color": TEXT_MUTED}),
                html.H3(id=value_id, className="mb-0 fw-bold", style={"color": ACCENT}),
                html.Span(suffix, className="text-muted small ms-2"),
            ]
        ),
        className="shadow-sm border-0",
        style={"background": CARD_BG, "borderLeft": f"5px solid {ACCENT_SOFT}"},
    )


app.layout = dbc.Container(
    [
        html.Div(
            [
                html.H1("VAR Platform", className="fw-bold", style={"color": ACCENT}),
                html.P(
                    "Explore our asset universe, understand geographic concentration, "
                    "and discover pricing gaps vs. the market.",
                    className="lead",
                ),
            ],
            className="py-4",
        ),
        dbc.Card(
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Small("Source"),
                                    dcc.Dropdown(
                                        id="source-filter",
                                        options=source_options,
                                        multi=True,
                                        placeholder="Select data sources",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Portfolios"),
                                    dcc.Dropdown(
                                        id="portfolio-filter",
                                        options=portfolio_options,
                                        multi=True,
                                        placeholder="Select one or more portfolios",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Municipalities"),
                                    dcc.Dropdown(
                                        id="municipality-search",
                                        options=municipality_options,
                                        multi=True,
                                        placeholder="Select municipalities",
                                    ),
                                ],
                                md=4,
                            ),
                        ],
                        className="g-3 mb-3",
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Small("Price per sqm (€)"),
                                    dcc.RangeSlider(
                                        id="price-range",
                                        min=price_min,
                                        max=price_max,
                                        value=[price_min, price_max],
                                        tooltip={"placement": "bottom", "always_visible": False, "template": "{value:.0f}"},
                                        step=max(1.0, (price_max - price_min) / 12) if price_max != price_min else 1.0,
                                    ),
                                    html.Div(id="price-range-label", className="small text-muted mt-2"),
                                ],
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Discount (%)"),
                                    dcc.RangeSlider(
                                        id="discount-range",
                                        min=discount_min,
                                        max=discount_max,
                                        value=[discount_min, discount_max],
                                        marks=discount_marks,
                                        tooltip={"placement": "bottom", "always_visible": False, "template": "{value:.0f}"},
                                        step=max(0.1, (discount_max - discount_min) / 12) if discount_max != discount_min else 0.1,
                                    ),
                                    html.Div(id="discount-range-label", className="small text-muted mt-2"),
                                ],
                                md=6,
                            ),
                        ],
                        className="g-3",
                    ),
                ]
            ),
            className="shadow-sm mb-4 border-0",
            style={"background": CARD_BG},
        ),
        dbc.Row(
            [
                dbc.Col(kpi_card("Assets", "kpi-assets"), md=3),
                dbc.Col(kpi_card("Avg price", "kpi-avg-price", "€"), md=3),
                dbc.Col(kpi_card("Median €/sqm", "kpi-median-sqm", "€"), md=3),
                dbc.Col(kpi_card("Avg discount vs market", "kpi-discount", "%"), md=3),
            ],
            className="g-3 mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Geographic concentration", className="mb-3"),
                                dcc.Graph(
                                    id="map-figure",
                                    config={
                                        "displayModeBar": True,
                                        "modeBarButtonsToAdd": ["zoomInMapbox", "zoomOutMapbox"],
                                        "displaylogo": False,
                                    },
                                    style={"height": "420px"},
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Price vs. size", className="mb-3"),
                                dcc.Graph(
                                    id="scatter-figure",
                                    config={"displayModeBar": False},
                                    style={"height": "420px"},
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Geography boundaries & area statistics", className="mb-2"),
                                html.P(
                                    "Polygons from the API; click a region for Spitogatos €/sqm stats.",
                                    className="text-muted small mb-3",
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                html.Small("Layer", className="d-block text-muted mb-1"),
                                                dbc.RadioItems(
                                                    id="geo-layer-radio",
                                                    options=[
                                                        {"label": "Athens neighborhoods", "value": GEO_LAYER_NEIGHBORHOODS},
                                                        {"label": "Attica municipalities", "value": GEO_LAYER_MUNICIPALITIES},
                                                    ],
                                                    value=GEO_LAYER_NEIGHBORHOODS,
                                                    inline=True,
                                                    className="mb-0",
                                                ),
                                            ],
                                            md=6,
                                        ),
                                        dbc.Col(
                                            [
                                                html.Small("API base", className="d-block text-muted mb-1"),
                                                html.Code(
                                                    GEOGRAPHY_API_BASE,
                                                    className="small",
                                                    style={"wordBreak": "break-all"},
                                                ),
                                            ],
                                            md=6,
                                        ),
                                    ],
                                    className="g-2 mb-3 align-items-end",
                                ),
                                dcc.Graph(
                                    id="geo-polygon-map",
                                    config={
                                        "displayModeBar": True,
                                        "modeBarButtonsToAdd": ["zoomInMapbox", "zoomOutMapbox"],
                                        "displaylogo": False,
                                    },
                                    style={"height": "420px"},
                                ),
                                html.Div(
                                    [
                                        html.Small("Selected area", className="text-muted d-block mb-1"),
                                        html.Div(
                                            id="geo-stats-panel",
                                            children=_geo_stats_placeholder(),
                                            className="border rounded p-3",
                                            style={"background": "#f8fafc", "minHeight": "120px"},
                                        ),
                                    ],
                                    className="mt-2",
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=12,
                ),
            ],
            className="g-3 mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Discount distribution by price buckets", className="mb-3"),
                                dcc.Graph(
                                    id="hist-figure",
                                    config={"displayModeBar": False},
                                    style={"height": "380px"},
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Discount distribution by municipality", className="mb-3"),
                                dcc.Graph(
                                    id="municipality-hist",
                                    config={"displayModeBar": False},
                                    style={"height": "380px"},
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Opportunity table", className="mb-3"),
                                DataTable(
                                    id="asset-table",
                                    columns=[
                                        {"name": "Municipality", "id": "municipality_label"},
                                        {"name": "Category", "id": "category_label"},
                                        {"name": "Auction Date", "id": "auction_date"},
                                         {"name": "Level", "id": "level"},
                                        {"name": "Reached radius (km)", "id": "searched_radius", "type": "numeric", "format": DECIMAL_FORMAT},
                                        {"name": "#assets", "id": "num_assets", "type": "numeric", "format": INTEGER_FORMAT},
                                        {"name": "Links", "id": "links", "presentation": "markdown"},
                                        {"name": "Price (€)", "id": "price", "type": "numeric",
                                         "format": CURRENCY_FORMAT},
                                        {"name": "sqm", "id": "sqm", "type": "numeric", "format": INTEGER_FORMAT},
                                        {"name": "€/sqm", "id": "price_per_sqm", "type": "numeric",
                                         "format": INTEGER_FORMAT},
                                        {"name": "Discount %", "id": "price_avg_discount_pct", "type": "numeric",
                                         "format": PERCENT_FORMAT},
                                        {"name": "Comparison min (€)", "id": "comparison_min", "type": "numeric", "format": CURRENCY_FORMAT},
                                        {"name": "Comparison avg (€)", "id": "comparison_average", "type": "numeric", "format": CURRENCY_FORMAT},
                                        {"name": "Comparison median (€)", "id": "comparison_median", "type": "numeric", "format": CURRENCY_FORMAT},
                                        {"name": "Comparison max (€)", "id": "comparison_max", "type": "numeric", "format": CURRENCY_FORMAT},
                                        {"name": "Description", "id": "description"},
                                    ],
                                    data=[],
                                    page_size=10,
                                    sort_action="native",
                                    markdown_options={"html": True, "link_target": "_blank"},
                                    fixed_rows={"headers": True},
                                    style_table={
                                        "overflowX": "auto",
                                        "overflowY": "auto",
                                        "maxHeight": "480px",
                                    },
                                    style_cell={
                                        "textAlign": "left",
                                        "fontSize": 14,
                                        "padding": "0.5rem",
                                        "backgroundColor": CARD_BG,
                                        "maxWidth": "280px",
                                        "overflow": "hidden",
                                        "textOverflow": "ellipsis",
                                        "whiteSpace": "nowrap",
                                    },
                                    style_header={
                                        "backgroundColor": ACCENT,
                                        "color": "white",
                                        "fontWeight": "600",
                                        "textTransform": "uppercase",
                                        "whiteSpace": "nowrap",
                                    },
                                    style_data_conditional=[
                                        {
                                            "if": {"filter_query": "{price_per_sqm} < {comparison_min}"},
                                            "backgroundColor": "#e0f5e9",
                                        },
                                        {
                                            "if": {"row_index": "odd"},
                                            "backgroundColor": "#f2f6fb",
                                        },
                                        {
                                            "if": {"column_id": "description"},
                                            "maxWidth": "320px",
                                            "whiteSpace": "nowrap",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                        {
                                            "if": {"column_id": "links"},
                                            "maxWidth": "200px",
                                        },
                                    ],
                                ),
                            ]
                        ),
                        className="shadow-sm border-0",
                        style={"background": CARD_BG},
                    ),
                    md=12,
                ),
            ],
            className="g-3 mb-5",
        ),
    ],
    fluid=True,
    className="pb-5",
    style={"backgroundColor": BACKGROUND, "minHeight": "100vh"},
)


def apply_filters(
        dataframe: pd.DataFrame,
        portfolios: Sequence[str] | None,
        sources: Sequence[str] | None,
        municipalities: Sequence[str] | None,
        price_bounds: Sequence[float],
        discount_bounds: Sequence[float],
) -> pd.DataFrame:
    filtered = dataframe
    if portfolios:
        filtered = filtered[filtered["portfolio_label"].isin(portfolios)]
    if sources and SOURCE_COLUMN is not None:
        filtered = filtered[filtered[SOURCE_COLUMN].isin(sources)]
    if municipalities:
        filtered = filtered[filtered["municipality_label"].isin(municipalities)]

    min_price, max_price = price_bounds
    filtered = filtered[
        filtered["price_per_sqm"].between(min_price, max_price, inclusive="both")
    ]

    min_discount, max_discount = discount_bounds
    filtered = filtered[
        (filtered["price-market_discount"].fillna(discount_min) >= min_discount)
        & (filtered["price-market_discount"].fillna(discount_max) <= max_discount)
        ]

    return filtered


@app.callback(
    Output("portfolio-filter", "options"),
    Output("portfolio-filter", "value"),
    Input("source-filter", "value"),
    State("portfolio-filter", "value"),
)
def update_portfolio_options(selected_sources, current_portfolios):
    """Update portfolio options based on selected sources and clear invalid selections."""
    if not selected_sources or SOURCE_COLUMN is None:
        # If no sources selected or no source column, show all portfolios
        all_portfolios = sorted(df["portfolio_label"].unique())
        portfolio_options = [{"label": name, "value": name} for name in all_portfolios]
        return portfolio_options, current_portfolios
    
    # Filter dataframe by selected sources
    filtered_df = df[df[SOURCE_COLUMN].isin(selected_sources)]
    
    # Get unique portfolios from filtered data
    available_portfolios = sorted(filtered_df["portfolio_label"].unique())
    portfolio_options = [{"label": name, "value": name} for name in available_portfolios]
    
    # Clear portfolio selection if current selection is not in available portfolios
    if current_portfolios:
        valid_portfolios = [p for p in current_portfolios if p in available_portfolios]
        return portfolio_options, valid_portfolios if valid_portfolios else None
    
    return portfolio_options, current_portfolios


@app.callback(
    Output("price-range-label", "children"),
    Output("discount-range-label", "children"),
    Input("price-range", "value"),
    Input("discount-range", "value"),
)
def update_range_labels(price_range: List[float], discount_range: List[float]):
    price_text = f"Showing assets between €{price_range[0]:,.0f}/sqm and €{price_range[1]:,.0f}/sqm"
    discount_text = f"Discount from {discount_range[0]:.1f}% to {discount_range[1]:.1f}%"
    return price_text, discount_text


@app.callback(
    Output("map-figure", "figure"),
    Output("scatter-figure", "figure"),
    Output("hist-figure", "figure"),
    Output("municipality-hist", "figure"),
    Output("asset-table", "data"),
    Output("kpi-assets", "children"),
    Output("kpi-avg-price", "children"),
    Output("kpi-median-sqm", "children"),
    Output("kpi-discount", "children"),
    Input("portfolio-filter", "value"),
    Input("source-filter", "value"),
    Input("municipality-search", "value"),
    Input("price-range", "value"),
    Input("discount-range", "value"),
)
def update_visuals(portfolios, sources, municipalities, price_range, discount_range):
    filtered = apply_filters(df, portfolios, sources, municipalities, price_range, discount_range)
    if filtered.empty:
        empty_fig = go.Figure().update_layout(
            xaxis_showgrid=False,
            yaxis_showgrid=False,
            xaxis_visible=False,
            yaxis_visible=False,
            annotations=[
                dict(
                    text="No data for current filters",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=14, color="#6c757d"),
                )
            ],
        )
        return (
            empty_fig,
            empty_fig,
            empty_fig,
            empty_fig,
            [],
            "0",
            "—",
            "—",
            "—",
        )

    map_df = filtered.dropna(subset=["lat", "lon"]).copy()
    # Set all markers to the same fixed size
    fixed_marker_size = 12
    map_df["marker_size"] = fixed_marker_size
    
    map_fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="price-market_discount",
        hover_name="title_display",
        hover_data={
            "portfolio_label": True,
            "municipality_label": True,
            "price": ":,.0f",
            "price_per_sqm": ":,.0f",
            "price-market_discount": ":.1f",
        },
        size="marker_size",
        size_max=fixed_marker_size,  # All markers are the same size
        color_continuous_scale=COLOR_SCALE,
        zoom=5,
        height=400,
    )
    map_fig.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
    )
    map_fig.update_coloraxes(
        colorbar=dict(
            title="Discount vs market (%)",
            yanchor="top",
            y=0.95,
            x=0.98,
        )
    )

    scatter_df = (
        filtered.groupby("municipality_label", as_index=False)
        .agg(
            sqm=("sqm", "sum"),
            price=("price", "sum"),
            price_per_sqm=("price_per_sqm", "mean"),
        )
        .rename(columns={"municipality_label": "municipality_name"})
    )
    scatter_fig = px.scatter(
        scatter_df,
        x="sqm",
        y="price",
        color="municipality_name",
        size="price_per_sqm",
        hover_data=["price_per_sqm"],
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    scatter_fig.update_layout(
        xaxis_title="Total size (sqm)",
        yaxis_title="Total price (€)",
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        legend_title="Municipality",
    )

    price_bins = pd.qcut(filtered["price"], q=min(5, len(filtered)), duplicates="drop")
    hist_fig = px.histogram(
        filtered.assign(price_bucket=price_bins),
        x="price-market_discount",
        nbins=30,
        color="price_bucket",
        barmode="overlay",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    hist_fig.update_layout(
        xaxis_title="Discount vs market (%)",
        yaxis_title="Count",
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
    )

    municipality_hist = px.histogram(
        filtered,
        x="price-market_discount",
        color="municipality_label",
        nbins=30,
        barmode="overlay",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    municipality_hist.update_layout(
        xaxis_title="Discount vs market (%)",
        yaxis_title="Count",
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        legend_title="Municipality",
    )

    table_data = filtered.assign(
        links=filtered.apply(
            lambda row: " ".join(
                link
                for link in [
                    make_link_button("Spitogatos", row.get("spitogatos_url")),
                    make_link_button("eAuction", row.get("eauctions_url"), variant="secondary"),
                    make_link_button("Reonline", row.get("reonline_url"), variant="secondary"),
                    make_link_button("Altamira", row.get("altamira_url"), variant="secondary"),
                    make_link_button("CPS", row.get("cps_url"), variant="secondary"),
                    make_link_button("Cerved", row.get("cerved_url"), variant="secondary"),
                    make_link_button("Reinvest", row.get("reinvest_url"), variant="secondary"),
                ]
                if link
            ),
            axis=1,
        ),
        auction_date=filtered["AuctionDate"].dt.strftime("%Y-%m-%d"),
        num_assets=filtered["#assets"],
        description=filtered["description"],
    )[
        [
            "municipality_label",
            "category_label",
            "auction_date",
            "level",
            "searched_radius",
            "num_assets",
            "links",
            "price",
            "sqm",
            "price_per_sqm",
            "price_avg_discount_pct",
            "comparison_min",
            "comparison_average",
            "comparison_median",
            "comparison_max",
            "description",
        ]
    ]
    table_data = (
        table_data.fillna({"price_avg_discount_pct": 0})
        .round(
            {
                "price_per_sqm": 0,
                "searched_radius": 1,
                "num_assets": 0,
                "price_avg_discount_pct": 1,
                "comparison_min": 0,
                "comparison_average": 0,
                "comparison_median": 0,
                "comparison_max": 0,
            }
        )
        .to_dict("records")
    )

    total_assets = f"{len(filtered):,}"
    avg_price = f"{filtered['price'].mean():,.0f}"
    median_sqm_price = f"{filtered['price_per_sqm'].median():,.0f}"
    avg_discount = f"{filtered['price-market_discount'].mean():.1f}"

    return (
        map_fig,
        scatter_fig,
        hist_fig,
        municipality_hist,
        table_data,
        total_assets,
        avg_price,
        median_sqm_price,
        avg_discount,
    )


@app.callback(
    Output("geo-polygon-map", "figure"),
    Input("geo-layer-radio", "value"),
)
def update_geo_polygon_map(layer: str):
    if layer == GEO_LAYER_MUNICIPALITIES:
        data, err = fetch_attica_municipalities_geojson()
        return _geojson_to_polygon_map_figure(data, err, "Attica municipalities")
    data, err = fetch_athens_neighborhoods_geojson()
    return _geojson_to_polygon_map_figure(data, err, "Athens neighborhoods")


@app.callback(
    Output("geo-stats-panel", "children"),
    Input("geo-polygon-map", "clickData"),
    Input("geo-layer-radio", "value"),
)
def update_geo_stats_panel(click_data: Optional[dict], layer: str):
    ctx = callback_context
    if ctx.triggered:
        trig = ctx.triggered[0]["prop_id"].split(".")[0]
        if trig == "geo-layer-radio":
            return _geo_stats_placeholder()

    if not click_data or not click_data.get("points"):
        return _geo_stats_placeholder()

    pt = click_data["points"][0]
    location = pt.get("location")
    if location is None or str(location).startswith("__unnamed_"):
        return _geo_stats_error("This polygon has no English name in the API; statistics cannot be loaded.")

    name_en = str(location)

    if layer == GEO_LAYER_MUNICIPALITIES:
        stats, err = fetch_municipality_statistics(name_en)
        label = "Municipality"
    else:
        stats, err = fetch_neighborhood_statistics(name_en)
        label = "Neighborhood"

    if err == "not_found":
        return _geo_stats_error(
            f"No Spitogatos assets with valid €/sqm for area “{name_en}” (404)."
        )
    if err:
        return _geo_stats_error(f"Statistics failed: {err}")
    if not stats:
        return _geo_stats_error("No statistics returned.")

    return _geo_stats_success(f"{label}: {name_en}", stats)


# ===========================================================================
# Spitogatos Analytics section helpers
# ===========================================================================

_ANALYTICS_METRICS = [
    {"label": "Listing Age (days since upload)", "value": "upload_time"},
    {"label": "Floor Number", "value": "floor_number"},
    {"label": "Price (€)", "value": "price"},
    {"label": "Area (sqm)", "value": "sqm"},
    {"label": "Price per sqm (€/sqm)", "value": "price_per_sqm"},
    {"label": "New Development (0/1)", "value": "new_development"},
]

_ANALYTICS_GRANULARITY = [
    {"label": "Monthly", "value": "month"},
    {"label": "Weekly", "value": "week"},
    {"label": "Daily", "value": "day"},
]

_RELATIONSHIP_PAIRS = [
    {"label": "Price vs sqm", "value": "price|sqm"},
    {"label": "Price/sqm vs Floor", "value": "price_per_sqm|floor_number"},
    {"label": "Price vs Listing Age", "value": "price|upload_time"},
    {"label": "sqm vs Listing Age", "value": "sqm|upload_time"},
    {"label": "Price/sqm vs Listing Age", "value": "price_per_sqm|upload_time"},
]

ANALYTICS_API_BASE = os.environ.get("REALESTATE_API_BASE", "http://localhost:8000").rstrip("/")


def _fetch_analytics(path: str) -> Tuple[Optional[Any], Optional[str]]:
    return _geo_request_json(path)


def _fetch_table_distribution(metric: str, n_buckets: int = 20) -> Tuple[Optional[dict], Optional[str]]:
    q = urllib.parse.urlencode({"metric": metric, "n_buckets": n_buckets})
    return _geo_request_json(f"/spitogatos/analytics/table-distribution?{q}")


def _fetch_trends(metric: str, granularity: str) -> Tuple[Optional[dict], Optional[str]]:
    q = urllib.parse.urlencode({"metric": metric, "granularity": granularity})
    return _geo_request_json(f"/spitogatos/analytics/trends?{q}")


def _fetch_relationships(x_metric: str, y_metric: str) -> Tuple[Optional[dict], Optional[str]]:
    q = urllib.parse.urlencode({"x_metric": x_metric, "y_metric": y_metric, "sample_limit": 3000})
    return _geo_request_json(f"/spitogatos/analytics/relationships?{q}")


def _analytics_empty(msg: str = "No data — select a metric and click Refresh.") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        annotations=[dict(text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
                          showarrow=False, font=dict(size=14, color=TEXT_MUTED))],
        xaxis_visible=False, yaxis_visible=False,
        margin=dict(l=0, r=0, t=30, b=0), height=380,
    )
    return fig


# ===========================================================================
# Analytics layout — injected into main app.layout after existing content
# ===========================================================================

_analytics_section = dbc.Card(
    dbc.CardBody([
        html.H4("Spitogatos Market Analytics", className="mb-3 fw-bold", style={"color": ACCENT}),
        html.P(
            "Explore distribution, time trends, and variable relationships for all Spitogatos listings, "
            "grouped by Athens neighborhoods and Attica municipalities.",
            className="text-muted mb-3",
        ),
        # Controls row
        dbc.Row([
            dbc.Col([
                html.Small("Metric", className="d-block text-muted mb-1"),
                dcc.Dropdown(
                    id="analytics-metric",
                    options=_ANALYTICS_METRICS,
                    value="price_per_sqm",
                    clearable=False,
                ),
            ], md=4),
            dbc.Col([
                html.Small("Areas to highlight (distribution / trend)", className="d-block text-muted mb-1"),
                dcc.Dropdown(
                    id="analytics-area-select",
                    options=[],
                    multi=True,
                    placeholder="All areas shown — select to highlight",
                ),
            ], md=5),
            dbc.Col([
                html.Small("Trend granularity", className="d-block text-muted mb-1"),
                dcc.Dropdown(
                    id="analytics-granularity",
                    options=_ANALYTICS_GRANULARITY,
                    value="month",
                    clearable=False,
                ),
            ], md=2),
            dbc.Col([
                html.Br(),
                dbc.Button("Refresh", id="analytics-refresh-btn", color="primary",
                           className="w-100", n_clicks=0),
            ], md=1),
        ], className="g-3 mb-3"),

        # View tabs
        dbc.Tabs(id="analytics-view-tabs", active_tab="view-dist", children=[
            dbc.Tab(label="Distribution & Table", tab_id="view-dist"),
            dbc.Tab(label="Time Trends", tab_id="view-trend"),
            dbc.Tab(label="Relationships", tab_id="view-rel"),
        ], className="mb-3"),

        dcc.Store(id="analytics-td-store"),
        dcc.Store(id="analytics-trend-store"),
        dcc.Store(id="analytics-rel-store"),
        dcc.Store(id="analytics-area-options-store"),

        # Distribution view
        html.Div(id="analytics-dist-view", children=[
            dbc.Row([
                dbc.Col([
                    html.H6("Distribution (select areas to add/remove traces)",
                            className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-dist-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=7),
                dbc.Col([
                    html.H6("Box Plot (spread & outliers per area)",
                            className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-box-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=5),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([
                    html.H6("ECDF (empirical CDF comparison)",
                            className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-ecdf-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=6),
                dbc.Col([
                    html.H6("Summary statistics table",
                            className="mb-2 text-muted small"),
                    html.Div(id="analytics-summary-table"),
                ], md=6),
            ], className="g-3"),
        ]),

        # Trend view
        html.Div(id="analytics-trend-view", style={"display": "none"}, children=[
            dbc.Row([
                dbc.Col([
                    html.H6("Median trend over time", className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-trend-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=8),
                dbc.Col([
                    html.H6("Percentile band (P25–P75)", className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-band-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=4),
            ], className="g-3"),
        ]),

        # Relationship view
        html.Div(id="analytics-rel-view", style={"display": "none"}, children=[
            dbc.Row([
                dbc.Col([
                    html.Small("Variable pair", className="d-block text-muted mb-1"),
                    dcc.Dropdown(
                        id="analytics-rel-pair",
                        options=_RELATIONSHIP_PAIRS,
                        value="price|sqm",
                        clearable=False,
                    ),
                ], md=4),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([
                    html.H6("Scatter plot", className="mb-2 text-muted small"),
                    dcc.Graph(id="analytics-scatter-graph", figure=_analytics_empty(),
                              config={"displayModeBar": True, "displaylogo": False}),
                ], md=12),
            ], className="g-3"),
        ]),
    ]),
    className="shadow-sm border-0 mb-4",
    style={"background": CARD_BG},
)

app.layout.children.append(_analytics_section)  # type: ignore[union-attr]


# ===========================================================================
# Analytics callbacks
# ===========================================================================

@app.callback(
    Output("analytics-td-store", "data"),
    Output("analytics-trend-store", "data"),
    Output("analytics-area-options-store", "data"),
    Input("analytics-refresh-btn", "n_clicks"),
    State("analytics-metric", "value"),
    State("analytics-granularity", "value"),
    prevent_initial_call=True,
)
def refresh_analytics_stores(n_clicks, metric, granularity):
    if not metric:
        return None, None, []

    td_data, td_err = _fetch_table_distribution(metric)
    trend_data, trend_err = _fetch_trends(metric, granularity)

    # Build area options from summary rows
    area_opts = []
    if td_data and "summary_rows" in td_data:
        for row in td_data["summary_rows"]:
            prefix = "N" if row["area_type"] == "neighborhood" else "M"
            area_opts.append({
                "label": f"[{prefix}] {row['area_name']}",
                "value": f"{row['area_type']}::{row['area_name']}",
            })

    return td_data or {}, trend_data or {}, area_opts


@app.callback(
    Output("analytics-area-select", "options"),
    Input("analytics-area-options-store", "data"),
)
def update_area_select_options(opts):
    return opts or []


@app.callback(
    Output("analytics-rel-store", "data"),
    Input("analytics-rel-pair", "value"),
    State("analytics-refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_relationship_store(pair, _n):
    if not pair or "|" not in pair:
        return {}
    x_m, y_m = pair.split("|", 1)
    rel_data, rel_err = _fetch_relationships(x_m, y_m)
    return rel_data or {}


@app.callback(
    Output("analytics-dist-view", "style"),
    Output("analytics-trend-view", "style"),
    Output("analytics-rel-view", "style"),
    Input("analytics-view-tabs", "active_tab"),
)
def toggle_analytics_views(active_tab):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if active_tab == "view-dist" else hide,
        show if active_tab == "view-trend" else hide,
        show if active_tab == "view-rel" else hide,
    )


@app.callback(
    Output("analytics-dist-graph", "figure"),
    Output("analytics-box-graph", "figure"),
    Output("analytics-ecdf-graph", "figure"),
    Output("analytics-summary-table", "children"),
    Input("analytics-td-store", "data"),
    Input("analytics-area-select", "value"),
    State("analytics-metric", "value"),
)
def update_distribution_views(td_data, selected_areas, metric):
    if not td_data or "distribution" not in td_data:
        empty = _analytics_empty()
        return empty, empty, empty, html.P("No data loaded. Click Refresh.", className="text-muted small")

    dist = td_data["distribution"]
    summary_rows = td_data.get("summary_rows", [])
    bucket_labels = dist.get("bucket_labels", [])
    series = dist.get("series", [])
    metric_label = next((m["label"] for m in _ANALYTICS_METRICS if m["value"] == metric), metric)

    selected_keys = set(selected_areas or [])

    # ---------- Distribution line chart (KDE-style using density) ----------
    dist_fig = go.Figure()
    palette = px.colors.qualitative.Prism
    for idx, s in enumerate(series):
        key = f"{s['area_type']}::{s['area_name']}"
        if selected_keys and key not in selected_keys:
            continue
        buckets = sorted(s["buckets"], key=lambda b: b["bucket_index"])
        xs = [bucket_labels[b["bucket_index"] - 1] if 0 < b["bucket_index"] <= len(bucket_labels)
              else str(b["bucket_index"]) for b in buckets]
        ys = [b.get("density") or 0 for b in buckets]
        prefix = "N" if s["area_type"] == "neighborhood" else "M"
        dist_fig.add_trace(go.Scatter(
            x=xs, y=ys,
            mode="lines",
            name=f"[{prefix}] {s['area_name']}",
            line=dict(color=palette[idx % len(palette)], width=2),
        ))
    dist_fig.update_layout(
        xaxis_title=metric_label,
        yaxis_title="Density",
        paper_bgcolor=CARD_BG, plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        legend=dict(orientation="v", x=1.02, y=1),
        margin=dict(l=40, r=10, t=30, b=40),
        height=380,
    )

    # ---------- Box plot from summary rows ----------
    box_fig = go.Figure()
    for idx, row in enumerate(summary_rows):
        key = f"{row['area_type']}::{row['area_name']}"
        if selected_keys and key not in selected_keys:
            continue
        prefix = "N" if row["area_type"] == "neighborhood" else "M"
        box_fig.add_trace(go.Box(
            name=f"[{prefix}] {row['area_name']}",
            q1=[row.get("p25") or 0],
            median=[row.get("median") or 0],
            q3=[row.get("p75") or 0],
            lowerfence=[row.get("min") or 0],
            upperfence=[row.get("max") or 0],
            mean=[row.get("mean") or 0],
            marker_color=palette[idx % len(palette)],
        ))
    box_fig.update_layout(
        yaxis_title=metric_label,
        paper_bgcolor=CARD_BG, plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        showlegend=False,
        margin=dict(l=40, r=10, t=30, b=40),
        height=380,
    )

    # ---------- ECDF from distribution buckets ----------
    ecdf_fig = go.Figure()
    for idx, s in enumerate(series):
        key = f"{s['area_type']}::{s['area_name']}"
        if selected_keys and key not in selected_keys:
            continue
        buckets = sorted(s["buckets"], key=lambda b: b["bucket_index"])
        counts = [b["count"] for b in buckets]
        total = sum(counts)
        if total == 0:
            continue
        xs = [bucket_labels[b["bucket_index"] - 1] if 0 < b["bucket_index"] <= len(bucket_labels)
              else str(b["bucket_index"]) for b in buckets]
        cumulative = []
        running = 0
        for c in counts:
            running += c
            cumulative.append(running / total)
        prefix = "N" if s["area_type"] == "neighborhood" else "M"
        ecdf_fig.add_trace(go.Scatter(
            x=xs, y=cumulative,
            mode="lines",
            name=f"[{prefix}] {s['area_name']}",
            line=dict(color=palette[idx % len(palette)], width=2),
        ))
    ecdf_fig.update_layout(
        xaxis_title=metric_label,
        yaxis_title="Cumulative probability",
        paper_bgcolor=CARD_BG, plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        legend=dict(orientation="v", x=1.02, y=1),
        margin=dict(l=40, r=10, t=30, b=40),
        height=380,
    )

    # ---------- Summary table ----------
    cols_def = [
        {"name": "Type", "id": "area_type"},
        {"name": "Area", "id": "area_name"},
        {"name": "N", "id": "n"},
        {"name": "Min", "id": "min"},
        {"name": "P10", "id": "p10"},
        {"name": "P25", "id": "p25"},
        {"name": "Median", "id": "median"},
        {"name": "Mean", "id": "mean"},
        {"name": "P75", "id": "p75"},
        {"name": "P90", "id": "p90"},
        {"name": "Max", "id": "max"},
        {"name": "Std", "id": "stddev"},
        {"name": "IQR", "id": "iqr"},
        {"name": "CV", "id": "cv"},
        {"name": "Skew", "id": "skewness"},
        {"name": "Kurt", "id": "kurtosis"},
    ]

    def _fmt(v):
        if v is None:
            return "—"
        try:
            f = float(v)
            return f"{f:,.2f}"
        except Exception:
            return str(v)

    table_rows = []
    for row in summary_rows:
        table_rows.append({k: _fmt(row.get(k)) if k not in ("area_type", "area_name", "n") else row.get(k)
                           for k in ["area_type", "area_name", "n", "min", "p10", "p25",
                                     "median", "mean", "p75", "p90", "max", "stddev",
                                     "iqr", "cv", "skewness", "kurtosis"]})

    summary_table = DataTable(
        id="analytics-summary-datatable",
        data=table_rows,
        columns=cols_def,
        sort_action="native",
        filter_action="native",
        page_size=15,
        fixed_rows={"headers": True},
        style_table={
            "overflowX": "auto",
            "overflowY": "auto",
            "maxHeight": "420px",
        },
        style_cell={
            "fontSize": 12,
            "fontFamily": "inherit",
            "padding": "4px 8px",
            "textAlign": "left",
            "minWidth": "70px",
            "maxWidth": "140px",
            "whiteSpace": "nowrap",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
        },
        style_header={
            "backgroundColor": ACCENT,
            "color": "white",
            "fontWeight": "bold",
            "fontSize": 12,
            "whiteSpace": "nowrap",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f5f5f5"},
        ],
    )

    return dist_fig, box_fig, ecdf_fig, summary_table


@app.callback(
    Output("analytics-trend-graph", "figure"),
    Output("analytics-band-graph", "figure"),
    Input("analytics-trend-store", "data"),
    Input("analytics-area-select", "value"),
    State("analytics-metric", "value"),
)
def update_trend_views(trend_data, selected_areas, metric):
    if not trend_data or "series" not in trend_data:
        empty = _analytics_empty()
        return empty, empty

    series = trend_data["series"]
    metric_label = next((m["label"] for m in _ANALYTICS_METRICS if m["value"] == metric), metric)
    selected_keys = set(selected_areas or [])
    palette = px.colors.qualitative.Prism

    trend_fig = go.Figure()
    band_fig = go.Figure()

    for idx, s in enumerate(series):
        key = f"{s['area_type']}::{s['area_name']}"
        if selected_keys and key not in selected_keys:
            continue
        pts = sorted(s["points"], key=lambda p: p["period"])
        periods = [p["period"] for p in pts]
        medians = [p.get("median") for p in pts]
        p25s = [p.get("p25") for p in pts]
        p75s = [p.get("p75") for p in pts]
        color = palette[idx % len(palette)]
        prefix = "N" if s["area_type"] == "neighborhood" else "M"
        label = f"[{prefix}] {s['area_name']}"

        trend_fig.add_trace(go.Scatter(
            x=periods, y=medians, mode="lines+markers",
            name=label, line=dict(color=color, width=2),
            marker=dict(size=4),
        ))

        # Band: fill between p25 and p75 for first/few selected areas for clarity
        if not selected_keys or key in selected_keys:
            band_fig.add_trace(go.Scatter(
                x=periods + periods[::-1],
                y=p75s + (p25s[::-1] if p25s else []),
                fill="toself",
                fillcolor=color.replace("rgb", "rgba").replace(")", ", 0.15)") if color.startswith("rgb") else color,
                line=dict(color="rgba(0,0,0,0)"),
                name=f"{label} P25–P75",
                showlegend=False,
            ))
            band_fig.add_trace(go.Scatter(
                x=periods, y=medians, mode="lines",
                name=label, line=dict(color=color, width=2),
            ))

    for fig in (trend_fig, band_fig):
        fig.update_layout(
            xaxis_title="Period",
            yaxis_title=metric_label,
            paper_bgcolor=CARD_BG, plot_bgcolor=CARD_BG,
            font=dict(color="#1d1d1f"),
            legend=dict(orientation="v", x=1.02, y=1),
            margin=dict(l=40, r=10, t=30, b=40),
            height=380,
        )

    return trend_fig, band_fig


@app.callback(
    Output("analytics-scatter-graph", "figure"),
    Input("analytics-rel-store", "data"),
    State("analytics-rel-pair", "value"),
)
def update_relationship_view(rel_data, pair):
    if not rel_data or "points" not in rel_data or not rel_data["points"]:
        return _analytics_empty("No relationship data. Select a variable pair and click Refresh.")

    points = rel_data["points"]
    x_metric, y_metric = (pair or "price|sqm").split("|", 1)
    x_label = next((m["label"] for m in _ANALYTICS_METRICS if m["value"] == x_metric), x_metric)
    y_label = next((m["label"] for m in _ANALYTICS_METRICS if m["value"] == y_metric), y_metric)

    df_rel = pd.DataFrame(points)
    df_rel["x"] = pd.to_numeric(df_rel["x"], errors="coerce")
    df_rel["y"] = pd.to_numeric(df_rel["y"], errors="coerce")
    df_rel = df_rel.dropna(subset=["x", "y"])

    color_col = "area_name" if "area_name" in df_rel.columns and df_rel["area_name"].notna().any() else None

    try:
        import statsmodels  # noqa: F401
        _trendline = "ols" if len(df_rel) >= 10 else None
    except ImportError:
        _trendline = None

    scatter_fig = px.scatter(
        df_rel,
        x="x", y="y",
        color=color_col,
        opacity=0.5,
        color_discrete_sequence=px.colors.qualitative.Prism,
        labels={"x": x_label, "y": y_label},
        trendline=_trendline,
        trendline_color_override="#1d7d8d",
    )
    scatter_fig.update_traces(marker=dict(size=4), selector=dict(mode="markers"))
    scatter_fig.update_layout(
        paper_bgcolor=CARD_BG, plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
        legend=dict(orientation="v", x=1.02, y=1),
        margin=dict(l=40, r=10, t=30, b=40),
        height=480,
    )
    return scatter_fig


if __name__ == "__main__":
    app.run(debug=True)
