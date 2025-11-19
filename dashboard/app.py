from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Sequence

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

    df["revaluation-market_discount"] = pd.to_numeric(df.get("revaluation_under_market"), errors="coerce") * 100
    df["price-market_discount"] = pd.to_numeric(df.get("precent_under_market"), errors="coerce") * 100
    revaluation_amount = pd.to_numeric(df.get("revaluation"), errors="coerce")
    sqm_safe = df["sqm"].replace({0: pd.NA})
    df["revaluation_price_per_sqm"] = revaluation_amount.divide(sqm_safe)
    revaluation_sqm_safe = df["revaluation_price_per_sqm"].replace({0: pd.NA})
    df["price_revaluation_ratio"] = df["price_per_sqm"].divide(revaluation_sqm_safe)
    df["price_revaluation_discount"] = (1 - df["price_revaluation_ratio"]) * 100

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
RATIO_FORMAT = Format(precision=2, scheme=Scheme.fixed)


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
                                        step=(price_max - price_min) / 10,
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
                                html.H5("Discount distribution", className="mb-3"),
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
                                        {"name": "Municipality", "id": "municipality_label"},
                                        {"name": "Category", "id": "category_label"},
                                        {"name": "Links", "id": "links", "presentation": "markdown"},
                                        {"name": "Price (€)", "id": "price", "type": "numeric",
                                         "format": CURRENCY_FORMAT},
                                        {"name": "sqm", "id": "sqm", "type": "numeric", "format": INTEGER_FORMAT},
                                        {"name": "€/sqm", "id": "price_per_sqm", "type": "numeric",
                                         "format": INTEGER_FORMAT},
                                        {"name": "Reval €/sqm", "id": "revaluation_price_per_sqm", "type": "numeric",
                                         "format": INTEGER_FORMAT},
                                        {"name": "Discount %", "id": "price_revaluation_discount", "type": "numeric",
                                         "format": PERCENT_FORMAT},
                                        {"name": "Score", "id": "score", "type": "numeric", "format": RATIO_FORMAT},
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
                    md=8,
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
        color="price-market_discount",
        hover_name="TitleGR",
        hover_data={
            "Portfolio": True,
            "Municipality": True,
            "price": ":,.0f",
            "price_per_sqm": ":,.0f",
            "price-market_discount": ":.1f",
        },
        size="price",
        color_continuous_scale=COLOR_SCALE,
    )
    map_fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        geo=dict(showland=True, showcountries=True, landcolor="#e6f0f2"),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
    )

    scatter_fig = px.scatter(
        filtered,
        x="sqm",
        y="price",
        color="portfolio_label",
        size="price_per_sqm",
        hover_data=["TitleGR", "Municipality", "price_per_sqm", "score"],
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    scatter_fig.update_layout(
        xaxis_title="Size (sqm)",
        yaxis_title="Price (€)",
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color="#1d1d1f"),
    )

    hist_fig = px.histogram(
        filtered,
        x="price-market_discount",
        nbins=30,
        color="portfolio_label",
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

    table_data = filtered.assign(
        links=filtered.apply(
            lambda row: " ".join(
                link
                for link in [
                    make_link_button("Spitogatos", row.get("spitogatos_url")),
                    make_link_button("eAuction", row.get("eauctions_url"), variant="secondary"),
                ]
                if link
            ),
            axis=1,
        )
    )[
        [
            "municipality_label",
            "category_label",
            "links",
            "price",
            "sqm",
            "price_per_sqm",
            "revaluation_price_per_sqm",
            "price_revaluation_discount",
            "score",
        ]
    ]
    table_data = (
        table_data.fillna({"price_revaluation_discount": 0})
        .round(
            {
                "price_per_sqm": 0,
                "revaluation_price_per_sqm": 0,
                "price_revaluation_discount": 1,
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
        table_data,
        total_assets,
        avg_price,
        median_sqm_price,
        avg_discount,
    )


if __name__ == "__main__":
    app.run(debug=True)
