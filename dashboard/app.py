from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Sequence

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html
from dash.dash_table import DataTable
from dash.dash_table.Format import Format, Group, Scheme, Symbol

DATA_PATH = Path(__file__).resolve().parents[1] / "byhand" / "dovalue_clear.xlsx"


@lru_cache(maxsize=1)
def load_dataset() -> pd.DataFrame:
    df = pd.read_excel(DATA_PATH)
    df = df.copy()

    coord_parts = df["coords"].astype(str).str.split(",", expand=True)
    df["lat"] = pd.to_numeric(coord_parts[0], errors="coerce")
    df["lon"] = pd.to_numeric(coord_parts[1], errors="coerce")

    df["price_per_sqm"] = pd.to_numeric(df.get("price/sqm"), errors="coerce")
    df["comparison_average"] = pd.to_numeric(df.get("comparison_average"), errors="coerce")
    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["sqm"] = pd.to_numeric(df.get("sqm"), errors="coerce")
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")

    comparison = df["comparison_average"].replace(0, pd.NA)
    df["discount_vs_market"] = ((comparison - df["price_per_sqm"]) / comparison) * 100
    df["discount_vs_market"] = df["discount_vs_market"].clip(lower=-100, upper=100)

    df["portfolio_label"] = df["Portfolio"].fillna("Unknown")
    df["category_label"] = df["CategoryGR"].fillna("Unspecified")
    df["municipality_label"] = df["Municipality"].fillna("—")

    return df


df = load_dataset()


def safe_int(value, fallback):
    return int(value) if pd.notna(value) else fallback


portfolio_options = [{"label": name, "value": name} for name in sorted(df["portfolio_label"].unique())]
category_options = [{"label": name, "value": name} for name in sorted(df["category_label"].unique())]

price_min = safe_int(df["price_per_sqm"].min(skipna=True), 0)
price_max = safe_int(df["price_per_sqm"].max(skipna=True), 1)
score_min = safe_int(df["score"].min(skipna=True), 0)
score_max = safe_int(df["score"].max(skipna=True), 1)

app = Dash(
    __name__,
    title="DoValue Opportunity Explorer",
    external_stylesheets=[dbc.themes.MINTY],
)
server = app.server

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


def kpi_card(title: str, value_id: str, suffix: str = "") -> dbc.Card:
    return dbc.Card(
        dbc.CardBody(
            [
                html.P(title, className="text-uppercase text-muted small mb-1"),
                html.H3(id=value_id, className="mb-0 fw-bold"),
                html.Span(suffix, className="text-muted small ms-2"),
            ]
        ),
        className="shadow-sm",
    )


app.layout = dbc.Container(
    [
        html.Div(
            [
                html.H1("DoValue Opportunity Explorer", className="fw-bold"),
                html.P(
                    "Explore the doValue asset universe, understand geographic concentration, "
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
                                    html.Small("Categories"),
                                    dcc.Dropdown(
                                        id="category-filter",
                                        options=category_options,
                                        multi=True,
                                        placeholder="Select asset categories",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Search Municipality"),
                                    dcc.Input(
                                        id="municipality-search",
                                        type="text",
                                        placeholder="e.g. Athens",
                                        className="form-control",
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
                                        tooltip={"placement": "bottom", "always_visible": False},
                                        step=50,
                                    ),
                                    html.Div(id="price-range-label", className="small text-muted mt-2"),
                                ],
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.Small("Score"),
                                    dcc.RangeSlider(
                                        id="score-range",
                                        min=score_min,
                                        max=score_max,
                                        value=[score_min, score_max],
                                        tooltip={"placement": "bottom", "always_visible": False},
                                    ),
                                    html.Div(id="score-range-label", className="small text-muted mt-2"),
                                ],
                                md=6,
                            ),
                        ],
                        className="g-3",
                    ),
                ]
            ),
            className="shadow-sm mb-4",
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
                                dcc.Graph(id="map-figure", config={"displayModeBar": False}),
                            ]
                        ),
                        className="shadow-sm h-100",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Price vs. size", className="mb-3"),
                                dcc.Graph(id="scatter-figure", config={"displayModeBar": False}),
                            ]
                        ),
                        className="shadow-sm h-100",
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
                                html.H5("Discount distribution", className="mb-3"),
                                dcc.Graph(id="hist-figure", config={"displayModeBar": False}),
                            ]
                        ),
                        className="shadow-sm h-100",
                    ),
                    md=4,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H5("Opportunity table", className="mb-3"),
                                DataTable(
                                    id="asset-table",
                                    columns=[
                                        {"name": "Portfolio", "id": "portfolio_label"},
                                        {"name": "Municipality", "id": "municipality_label"},
                                        {"name": "Category", "id": "category_label"},
                                        {"name": "Price (€)", "id": "price", "type": "numeric", "format": CURRENCY_FORMAT},
                                        {"name": "sqm", "id": "sqm", "type": "numeric", "format": INTEGER_FORMAT},
                                        {"name": "€/sqm", "id": "price_per_sqm", "type": "numeric", "format": INTEGER_FORMAT},
                                        {"name": "Discount %", "id": "discount_vs_market", "type": "numeric", "format": PERCENT_FORMAT},
                                        {"name": "Score", "id": "score", "type": "numeric"},
                                    ],
                                    data=[],
                                    page_size=10,
                                    sort_action="native",
                                    style_table={"overflowX": "auto"},
                                    style_cell={"textAlign": "left", "fontSize": 14},
                                ),
                            ]
                        ),
                        className="shadow-sm h-100",
                    ),
                    md=8,
                ),
            ],
            className="g-3 mb-5",
        ),
    ],
    fluid=True,
    className="pb-5",
)


def apply_filters(
    dataframe: pd.DataFrame,
    portfolios: Sequence[str] | None,
    categories: Sequence[str] | None,
    municipality: str | None,
    price_bounds: Sequence[float],
    score_bounds: Sequence[float],
) -> pd.DataFrame:
    filtered = dataframe
    if portfolios:
        filtered = filtered[filtered["portfolio_label"].isin(portfolios)]
    if categories:
        filtered = filtered[filtered["category_label"].isin(categories)]
    if municipality:
        filtered = filtered[
            filtered["municipality_label"].str.contains(municipality, case=False, na=False)
        ]

    min_price, max_price = price_bounds
    filtered = filtered[
        filtered["price_per_sqm"].between(min_price, max_price, inclusive="both")
    ]

    min_score, max_score = score_bounds
    filtered = filtered[
        (filtered["score"].fillna(score_min) >= min_score)
        & (filtered["score"].fillna(score_max) <= max_score)
    ]

    return filtered


@app.callback(
    Output("price-range-label", "children"),
    Output("score-range-label", "children"),
    Input("price-range", "value"),
    Input("score-range", "value"),
)
def update_range_labels(price_range: List[float], score_range: List[float]):
    price_text = f"Showing assets between €{price_range[0]:,.0f}/sqm and €{price_range[1]:,.0f}/sqm"
    score_text = f"Score from {score_range[0]:.1f} to {score_range[1]:.1f}"
    return price_text, score_text


@app.callback(
    Output("map-figure", "figure"),
    Output("scatter-figure", "figure"),
    Output("hist-figure", "figure"),
    Output("asset-table", "data"),
    Output("kpi-assets", "children"),
    Output("kpi-avg-price", "children"),
    Output("kpi-median-sqm", "children"),
    Output("kpi-discount", "children"),
    Input("portfolio-filter", "value"),
    Input("category-filter", "value"),
    Input("municipality-search", "value"),
    Input("price-range", "value"),
    Input("score-range", "value"),
)
def update_visuals(portfolios, categories, municipality, price_range, score_range):
    filtered = apply_filters(df, portfolios, categories, municipality, price_range, score_range)
    if filtered.empty:
        return (
            px.scatter_geo(),
            px.scatter(),
            px.histogram(),
            [],
            "0",
            "—",
            "—",
            "—",
        )

    map_df = filtered.dropna(subset=["lat", "lon"])
    map_fig = px.scatter_geo(
        map_df,
        lat="lat",
        lon="lon",
        color="discount_vs_market",
        hover_name="TitleGR",
        hover_data={
            "Portfolio": True,
            "Municipality": True,
            "price": ":,.0f",
            "price_per_sqm": ":,.0f",
            "discount_vs_market": ":.1f",
        },
        size="price",
        color_continuous_scale="Tealrose",
    )
    map_fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        geo=dict(showland=True, showcountries=True, landcolor="#f8f9fa"),
    )

    scatter_fig = px.scatter(
        filtered,
        x="sqm",
        y="price",
        color="portfolio_label",
        size="price_per_sqm",
        hover_data=["TitleGR", "Municipality", "price_per_sqm", "score"],
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    scatter_fig.update_layout(xaxis_title="Size (sqm)", yaxis_title="Price (€)")

    hist_fig = px.histogram(
        filtered,
        x="discount_vs_market",
        nbins=30,
        color="portfolio_label",
        barmode="overlay",
    )
    hist_fig.update_layout(xaxis_title="Discount vs market (%)", yaxis_title="Count")

    table_columns = [
        "portfolio_label",
        "municipality_label",
        "category_label",
        "price",
        "sqm",
        "price_per_sqm",
        "discount_vs_market",
        "score",
    ]
    table_data = (
        filtered[table_columns]
        .fillna({"discount_vs_market": 0})
        .round({"price_per_sqm": 0, "discount_vs_market": 1})
        .to_dict("records")
    )

    total_assets = f"{len(filtered):,}"
    avg_price = f"{filtered['price'].mean():,.0f}"
    median_sqm_price = f"{filtered['price_per_sqm'].median():,.0f}"
    avg_discount = f"{filtered['discount_vs_market'].mean():.1f}"

    return (
        map_fig,
        scatter_fig,
        hist_fig,
        table_data,
        total_assets,
        avg_price,
        median_sqm_price,
        avg_discount,
    )


if __name__ == "__main__":
    app.run_server(debug=True)

