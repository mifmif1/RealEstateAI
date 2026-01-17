from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Sequence
import re

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html
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
                                    style_table={"overflowX": "auto"},
                                    style_cell={
                                        "textAlign": "left",
                                        "fontSize": 14,
                                        "padding": "0.5rem",
                                        "backgroundColor": CARD_BG,
                                    },
                                    style_header={
                                        "backgroundColor": ACCENT,
                                        "color": "white",
                                        "fontWeight": "600",
                                        "textTransform": "uppercase",
                                    },
                                    style_data_conditional=[
                                        {
                                            "if": {"filter_query": "{price_per_sqm} < {comparison_min}"},
                                            "backgroundColor": "#e0f5e9",
                                        },
                                        {
                                            "if": {"row_index": "odd"},
                                            "backgroundColor": "#f2f6fb",
                                        }
                                    ],
                                ),
                            ]
                        ),
                        className="shadow-sm h-100 border-0",
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


if __name__ == "__main__":
    app.run(debug=True)
