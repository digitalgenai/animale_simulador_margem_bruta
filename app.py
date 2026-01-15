from __future__ import annotations

import io
import logging
from typing import Any, Dict, List, Tuple

import pandas as pd
import numpy as np

from dash import Dash, html, dcc, Input, Output, State, ctx, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from core.config import (
    BASE_SIMULADOR_PATH,
    TAXA_DEDUCAO_FATURAMENTO,
    col_conc_1,
    col_conc_2,
    NOME_CONC_1,
    NOME_CONC_2,
    COLUNA_AGREGACAO_PRINCIPAL,
    LISTA_MESES_ANO,
)
from core.data_loader import load_base_data
from core.view_builders import (
    compute_summary,
    build_tab1_rows,
    build_tab2_rows,
    build_tab3_rows,
    build_history_payload,
)
from core.calculations import calcular_custo_necessario

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simulador_web")

# --- Carga global (equivalente ao bloco try do desktop) ---
try:
    df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global = load_base_data()
except Exception as e:
    # No desktop era sg.popup_error + sys.exit; aqui levantamos erro explícito no log e exibimos na tela.
    logger.exception("Erro inicialização ao carregar '%s': %s", BASE_SIMULADOR_PATH, e)
    df_base = pd.DataFrame()
    bench_ano, bench_6m, bench_3m = {}, {}, {}
    lista_fornecedores, lista_categorias_global = ["SEM DADOS"], []


# ---------- Helpers ----------
def _safe_float_percent(val: Any, default: float) -> float:
    """
    Recebe string tipo "30.0" ou "30,0" e devolve FRAÇÃO (0.30).
    """
    try:
        if val is None:
            return default
        s = str(val).replace(",", ".").strip()
        return float(s) / 100.0
    except Exception:
        return default


def _filter_tab12(forn: str, fab: str, cat: str) -> pd.DataFrame:
    if df_base.empty or not forn:
        return df_base.iloc[0:0].copy()

    df_temp = df_base[df_base[COLUNA_AGREGACAO_PRINCIPAL] == forn]

    if fab and fab != "[TODOS]":
        df_temp = df_temp[df_temp["Fabricante"] == fab]

    if cat and cat != "[TODAS]":
        df_temp = df_temp[df_temp["Area"] == cat]

    # Ordenação ABC como no desktop
    abc_map = {"A": 0, "B": 1, "C": 2}
    abc_order = df_temp["Curva_ABC"].map(abc_map).fillna(3)
    df_temp = df_temp.assign(ABC_Order=abc_order).sort_values(
        ["ABC_Order", "Fat_Total_Trimestre"], ascending=[True, False]
    )
    return df_temp


def _filter_tab3(cat_t3: str, forn_t3: str) -> pd.DataFrame:
    if df_base.empty or not cat_t3:
        return df_base.iloc[0:0].copy()

    df_temp = df_base[df_base["Area"] == cat_t3]
    if forn_t3 and forn_t3 != "[TODOS]":
        df_temp = df_temp[df_temp["Fornecedor"] == forn_t3]
    return df_temp


def _get_fab_cat_options_for_supplier(forn: str) -> Tuple[List[str], List[str]]:
    if df_base.empty or not forn:
        return ["[TODOS]"], ["[TODAS]"]
    df_forn = df_base[df_base[COLUNA_AGREGACAO_PRINCIPAL] == forn]
    lista_fab = sorted(df_forn["Fabricante"].unique().tolist())
    lista_fab.insert(0, "[TODOS]")
    lista_cat = sorted(df_forn["Area"].unique().tolist())
    lista_cat.insert(0, "[TODAS]")
    return lista_fab, lista_cat


def _get_supplier_options_for_category(cat_t3: str) -> List[str]:
    if df_base.empty or not cat_t3:
        return ["[TODOS]"]
    df_cat = df_base[df_base["Area"] == cat_t3]
    rank_forn_cat = df_cat.groupby("Fornecedor")["Fat_Total_Trimestre"].sum().sort_values(ascending=False)
    lista_forn_cat = rank_forn_cat.index.tolist()
    lista_forn_cat.insert(0, "[TODOS]")
    return lista_forn_cat


def _apply_row_class_rules() -> Dict[str, Any]:
    # Usa flags __is_neg e __is_yellow (equivalente às cores no desktop)
    return {
        "rowClassRules": {
            "row-neg": "params.data.__is_neg === true",
            "row-yellow": "params.data.__is_yellow === true",
        }
    }


def _kpi_line(prefix: str):
    return html.Div(
        [
            html.Span(prefix, className="kpi-label"),
        ],
        style={"display": "inline-block", "marginRight": "10px"},
    )


def _format_kpi(label: str, value: str, color: str | None = None):
    style = {"display": "inline-block", "marginRight": "18px"}
    vstyle = {"fontWeight": "700"}
    if color:
        vstyle["color"] = color
    return html.Span(
        [
            html.Span(label, className="kpi-label"),
            html.Span(value, className="kpi-value", style=vstyle),
        ],
        style=style,
    )


def _breakdown_component(breakdown: List[Dict[str, Any]]):
    # replica o texto multiline do desktop, mas como tabela HTML
    header = html.Thead(
        html.Tr(
            [
                html.Th("CATEGORIA"),
                html.Th("FAT(Nov)"),
                html.Th("MG(Nov)"),
                html.Th("GL(Year)"),
            ]
        )
    )
    body_rows = []
    for b in breakdown:
        body_rows.append(
            html.Tr(
                [
                    html.Td(b["categoria"][:15]),
                    html.Td(f"R$ {b['fat']:,.2f}"),
                    html.Td(f"{b['marg_perc']:.1%}"),
                    html.Td(f"{b['bench_ano']:.1%}"),
                ]
            )
        )
    body = html.Tbody(body_rows)

    return dbc.Table([header, body], bordered=True, size="sm", className="breakdown-table")


def _history_component(hist: Dict[str, Any], suffix: str):
    # Mantém exatamente os campos do desktop
    return html.Div(
        [
            html.Div("Detalhes (Inteligência Temporal):", style={"fontWeight": "700", "color": "navy"}),
            html.Div([html.Span("Produto: ", style={"width": "70px", "display": "inline-block"}), html.Span(hist.get("produto", "Selecione..."), style={"fontStyle": "italic"})]),
            html.Div(
                [
                    html.Span("Méd 6M: ", style={"width": "70px", "display": "inline-block"}),
                    html.Span(hist.get("hist_6m", "-"), style={"fontWeight": "700"}),
                    html.Span("  "),
                    html.Span("Méd 3M: ", style={"width": "70px", "display": "inline-block", "marginLeft": "10px"}),
                    html.Span(hist.get("hist_3m", "-"), style={"fontWeight": "700"}),
                ]
            ),
            html.Div(
                [
                    html.Span("Venda Nov: ", style={"width": "90px", "display": "inline-block"}),
                    html.Span(hist.get("hist_nov", "-"), style={"fontWeight": "700", "color": "blue"}),
                    html.Span("  "),
                    html.Span("Pico: ", style={"width": "50px", "display": "inline-block", "marginLeft": "10px"}),
                    html.Span(hist.get("hist_pico", "-"), style={"fontWeight": "700", "color": "green"}),
                ]
            ),
        ],
        className="history-box",
        id=f"hist-box-{suffix}",
    )


# ---------- Dash app ----------
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Simulador v76.8 - Web",
)
server = app.server

# Stores de sessão (equivalente a manter df_base “mutável” por usuário no desktop)
store_sim_default = {"manual": {}, "conc": {}}

# ColumnDefs (equivalentes às colunas dos sg.Table)
coldefs_t1 = [
    {"headerName": "SKU", "field": "SKU", "width": 95},
    {"headerName": "Produto", "field": "Produto", "width": 260},
    {"headerName": "ABC", "field": "ABC", "width": 70},
    {"headerName": "Categ", "field": "Categ", "width": 140},
    {"headerName": "Qtd Nov", "field": "Qtd Nov", "width": 95},
    {"headerName": "Preço Atual", "field": "Preço Atual", "width": 110},
    {"headerName": "Custo", "field": "Custo", "width": 110},
    {"headerName": "Marg R$", "field": "Marg R$", "width": 110},
    {"headerName": "Marg %", "field": "Marg %", "width": 95},
    {"headerName": NOME_CONC_1, "field": NOME_CONC_1, "width": 110},
    {"headerName": NOME_CONC_2, "field": NOME_CONC_2, "width": 110},
    {"headerName": "Dif % (Menor)", "field": "Dif % (Menor)", "width": 110},
    {"headerName": "Sim Preço", "field": "Sim Preço", "width": 110},
    {"headerName": "Sim Marg", "field": "Sim Marg", "width": 95},
    {"headerName": "Sim Custo Nec", "field": "Sim Custo Nec", "width": 120},
]

coldefs_t2 = [
    {"headerName": "SKU", "field": "SKU", "width": 95},
    {"headerName": "Produto", "field": "Produto", "width": 260},
    {"headerName": "ABC", "field": "ABC", "width": 70},
    {"headerName": "Categ", "field": "Categ", "width": 140},
    {"headerName": "Qtd Nov", "field": "Qtd Nov", "width": 95},
    {"headerName": "Preço Atual", "field": "Preço Atual", "width": 110},
    {"headerName": "Custo", "field": "Custo", "width": 110},
    {"headerName": "Marg Atual %", "field": "Marg Atual %", "width": 115},
    {"headerName": NOME_CONC_1, "field": NOME_CONC_1, "width": 110},
    {"headerName": NOME_CONC_2, "field": NOME_CONC_2, "width": 110},
    {"headerName": "Dif Atual (Menor)", "field": "Dif Atual (Menor)", "width": 130},
    {"headerName": "DELTA ALVO %", "field": "DELTA ALVO %", "width": 120},
    {"headerName": "Sim Preço (Conc)", "field": "Sim Preço (Conc)", "width": 130},
    {"headerName": "Sim Margem (Result)", "field": "Sim Margem (Result)", "width": 140},
]

coldefs_t3 = [
    {"headerName": "Fornecedor", "field": "Fornecedor", "width": 320},
    {"headerName": "Fat Nov", "field": "Fat Nov", "width": 160},
    {"headerName": "Margem Nov R$", "field": "Margem Nov R$", "width": 160},
    {"headerName": "Margem Nov %", "field": "Margem Nov %", "width": 140},
]


def make_grid(grid_id: str, column_defs: List[Dict[str, Any]]) -> dag.AgGrid:
    return dag.AgGrid(
        id=grid_id,
        columnDefs=column_defs,
        rowData=[],

        # MUITO IMPORTANTE: ajuda o grid a entender que o dataset mudou
        getRowId="params.data.id",

        defaultColDef={
            "resizable": True,
            "sortable": True,
            "filter": True,
        },

        dashGridOptions={
            # use string compatível (evita diferenças entre versões)
            "rowSelection": "single",
            "animateRows": True,

            # mantém suas regras de cor
            **_apply_row_class_rules(),
        },

        # REMOVER responsiveSizeToFit (causa warning/bug em tabs)
        # columnSize="responsiveSizeToFit",
        className="ag-theme-alpine",

        # garante largura mínima dentro do flex
        style={"height": "520px", "width": "100%", "minWidth": "0"},
    )


def make_summary_block(suffix: str):
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        _format_kpi("Fat. Total:", "-", None),
                        _format_kpi("Margem Média:", "-", "blue"),
                        html.Span("| ", style={"color": "#999"}),
                        _format_kpi("Total SKUs:", "-", None),
                        _format_kpi("A:", "-", "green"),
                        _format_kpi("B:", "-", "#bda404"),
                        _format_kpi("C:", "-", "red"),
                    ],
                    id=f"kpi-line-{suffix}",
                ),
                html.Hr(),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div("Top Categorias (Forn. vs Benchmarks):", style={"fontWeight": "700"}),
                                html.Div(id=f"breakdown-{suffix}", children=_breakdown_component([])),
                            ],
                            md=7,
                        ),
                        dbc.Col(
                            _history_component(
                                {"produto": "Selecione...", "hist_6m": "-", "hist_3m": "-", "hist_nov": "-", "hist_pico": "-"},
                                suffix,
                            ),
                            md=5,
                        ),
                    ],
                    className="g-2",
                ),
            ]
        ),
        className="mb-2",
    )


# ---------- Layout ----------
app.layout = dbc.Container(
    fluid=True,
    children=[
        dcc.Store(id="store-sim", storage_type="session", data=store_sim_default),
        dcc.Store(id="store-selected", storage_type="session", data={"produto_key": None}),
        dcc.Download(id="download-excel"),

        dbc.Row(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.Div(
                                [
                                    html.Span(f"{COLUNA_AGREGACAO_PRINCIPAL}: ", style={"fontWeight": "700"}),
                                    dcc.Dropdown(
                                        id="forn",
                                        options=[{"label": x, "value": x} for x in lista_fornecedores],
                                        value=(lista_fornecedores[0] if lista_fornecedores else None),
                                        placeholder="Selecione...",
                                        style={"width": "260px", "display": "inline-block", "verticalAlign": "middle"},
                                        clearable=True,
                                    ),
                                    html.Span("  Fabr: ", style={"fontWeight": "700", "marginLeft": "10px"}),
                                    dcc.Dropdown(
                                        id="fab",
                                        options=[{"label": "[TODOS]", "value": "[TODOS]"}],
                                        value="[TODOS]",
                                        style={"width": "220px", "display": "inline-block", "verticalAlign": "middle"},
                                        clearable=False,
                                    ),
                                    html.Span("  Categ: ", style={"fontWeight": "700", "marginLeft": "10px"}),
                                    dcc.Dropdown(
                                        id="cat",
                                        options=[{"label": "[TODAS]", "value": "[TODAS]"}],
                                        value="[TODAS]",
                                        style={"width": "220px", "display": "inline-block", "verticalAlign": "middle"},
                                        clearable=False,
                                    ),
                                    html.Span("  |  ", style={"color": "#999", "marginLeft": "10px"}),

                                    html.Span("Meta Fin. (%): ", style={"fontSize": "12px", "color": "navy"}),
                                    dcc.Input(
                                        id="meta_t1",
                                        value="30.0",
                                        type="text",
                                        style={"width": "70px", "textAlign": "right", "marginRight": "8px"},
                                    ),

                                    html.Span("Delta Padrão (%): ", style={"fontSize": "12px", "color": "#b75402"}),
                                    dcc.Input(
                                        id="meta_t2",
                                        value="0.0",
                                        type="text",
                                        style={"width": "70px", "textAlign": "right", "marginRight": "8px"},
                                    ),

                                    dbc.Button("Recalcular", id="btn-refresh", color="primary", className="me-2"),
                                    dbc.Button("Exportar", id="btn-export", color="success"),
                                ],
                                style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"},
                            ),
                            html.Div(
                                "Objetivo das Abas 1 e 2: replicar o simulador desktop com popups via duplo clique.",
                                className="small-muted",
                                style={"marginTop": "8px"},
                            ),
                        ]
                    )
                ),
                width=12,
            ),
            className="mb-2",
        ),

        dbc.Tabs(
            id="tabs",
            active_tab="tab-1",
            children=[
                dbc.Tab(
                    label="1. Visão de Custo",
                    tab_id="tab-1",
                    children=[
                        make_summary_block("t1"),
                        html.Div("Objetivo: Definir Preço/Margem para calcular Custo Alvo.", className="small-muted"),
                        make_grid("grid-t1", coldefs_t1),
                    ],
                ),
                dbc.Tab(
                    label="2. Visão de Precificação",
                    tab_id="tab-2",
                    children=[
                        make_summary_block("t2"),
                        html.Div("Objetivo: Definir Delta % vs Concorrente para ver a Margem Resultante.", className="small-muted"),
                        make_grid("grid-t2", coldefs_t2),
                    ],
                ),
                dbc.Tab(
                    label="3. Visão Categ (Fornecedores)",
                    tab_id="tab-3",
                    children=[
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Div(
                                        [
                                            html.Span("1. Selecione a Categoria (Principal): ", style={"fontWeight": "700", "color": "navy"}),
                                            dcc.Dropdown(
                                                id="cat_t3",
                                                options=[{"label": x, "value": x} for x in lista_categorias_global],
                                                value=None,
                                                placeholder="Selecione...",
                                                style={"width": "260px", "display": "inline-block"},
                                                clearable=True,
                                            ),
                                            html.Span("  -->  ", style={"fontWeight": "700"}),
                                            html.Span("2. Fornecedor (Opcional): ", style={"fontWeight": "700", "color": "navy"}),
                                            dcc.Dropdown(
                                                id="forn_t3",
                                                options=[{"label": "[TODOS]", "value": "[TODOS]"}],
                                                value="[TODOS]",
                                                style={"width": "260px", "display": "inline-block"},
                                                clearable=False,
                                            ),
                                        ],
                                        style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"},
                                    )
                                ]
                            ),
                            className="mb-2",
                            style={"backgroundColor": "#e1e1e1"},
                        ),
                        make_summary_block("t3"),
                        make_grid("grid-t3", coldefs_t3),
                    ],
                ),
            ],
        ),

        # ---------- Modal Financeiro (Tab 1) ----------
        dbc.Modal(
            id="modal-fin",
            is_open=False,
            size="lg",
            children=[
                dbc.ModalHeader(dbc.ModalTitle(id="modal-fin-title", children="Simulação Fin.")),
                dbc.ModalBody(
                    [
                        dbc.Tabs(
                            id="modal-fin-tabs",
                            active_tab="tab-marg",
                            children=[
                                dbc.Tab(
                                    label="Definir Margem",
                                    tab_id="tab-marg",
                                    children=[
                                        dbc.Row(
                                            [
                                                dbc.Col([dbc.Label("Preço R$:"), dcc.Input(id="fin-p", type="text", value="", style={"width": "120px", "textAlign": "right"})], md=6),
                                                dbc.Col([dbc.Label("Margem %:"), dcc.Input(id="fin-m", type="text", value="", style={"width": "120px", "textAlign": "right"})], md=6),
                                            ],
                                            className="g-2",
                                        )
                                    ],
                                ),
                                dbc.Tab(
                                    label="Definir Custo",
                                    tab_id="tab-cust",
                                    children=[
                                        dbc.Row(
                                            [
                                                dbc.Col([dbc.Label("Preço R$:"), dcc.Input(id="fin-p2", type="text", value="", style={"width": "120px", "textAlign": "right"})], md=6),
                                                dbc.Col([dbc.Label("Custo R$:"), dcc.Input(id="fin-c", type="text", value="", style={"width": "120px", "textAlign": "right"})], md=6),
                                            ],
                                            className="g-2",
                                        )
                                    ],
                                ),
                            ],
                        ),
                        html.Hr(),
                        html.Div("Salvar aplica simulação manual (equivalente ao desktop). Reset desativa a simulação manual.", className="small-muted"),
                    ]
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button("Salvar", id="fin-save", color="primary", className="me-2"),
                        dbc.Button("Reset", id="fin-reset", color="secondary"),
                        dbc.Button("Fechar", id="fin-close", color="light", className="ms-auto"),
                    ]
                ),
            ],
        ),

        # ---------- Modal Mercado (Tab 2) ----------
        dbc.Modal(
            id="modal-mkt",
            is_open=False,
            size="lg",
            children=[
                dbc.ModalHeader(dbc.ModalTitle(id="modal-mkt-title", children="Simulação Mercado")),
                dbc.ModalBody(
                    [
                        html.Div(id="mkt-menor-conc", style={"color": "blue", "fontWeight": "700"}),
                        html.Hr(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Label("Diferença Alvo (%):"),
                                        dcc.Input(id="mkt-delta", type="text", value="", style={"width": "140px", "textAlign": "right"}),
                                        html.Div("% (Ex: -5.0 para 5% abaixo)", className="small-muted"),
                                    ],
                                    md=6,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Preço Resultante Estimado:"),
                                        html.Div(id="mkt-preco-est", style={"fontStyle": "italic", "color": "gray"}),
                                    ],
                                    md=6,
                                ),
                            ],
                            className="g-2",
                        ),
                    ]
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button("Salvar Delta", id="mkt-save", color="primary", className="me-2"),
                        dbc.Button("Usar Padrão", id="mkt-reset", color="secondary"),
                        dbc.Button("Fechar", id="mkt-close", color="light", className="ms-auto"),
                    ]
                ),
            ],
        ),
    ],
)

# ---------- Callbacks de opções ----------
@app.callback(
    Output("fab", "options"),
    Output("fab", "value"),
    Output("cat", "options"),
    Output("cat", "value"),
    Input("forn", "value"),
)
def on_fornecedor_change(forn):
    fab_opts, cat_opts = _get_fab_cat_options_for_supplier(forn)
    return (
        [{"label": x, "value": x} for x in fab_opts],
        "[TODOS]",
        [{"label": x, "value": x} for x in cat_opts],
        "[TODAS]",
    )


@app.callback(
    Output("forn_t3", "options"),
    Output("forn_t3", "value"),
    Input("cat_t3", "value"),
)
def on_cat_t3_change(cat_t3):
    opts = _get_supplier_options_for_category(cat_t3)
    return [{"label": x, "value": x} for x in opts], "[TODOS]"


# ---------- Callback principal: atualizar tabelas + resumo ----------
@app.callback(
    Output("grid-t1", "rowData"),
    Output("grid-t2", "rowData"),
    Output("grid-t3", "rowData"),

    Output("kpi-line-t1", "children"),
    Output("breakdown-t1", "children"),

    Output("kpi-line-t2", "children"),
    Output("breakdown-t2", "children"),

    Output("kpi-line-t3", "children"),
    Output("breakdown-t3", "children"),

    Input("btn-refresh", "n_clicks"),
    Input("tabs", "active_tab"),
    Input("forn", "value"),
    Input("fab", "value"),
    Input("cat", "value"),
    Input("meta_t1", "value"),
    Input("meta_t2", "value"),
    Input("cat_t3", "value"),
    Input("forn_t3", "value"),
    Input("store-sim", "data"),
)
def refresh_all(_, active_tab, forn, fab, cat, meta_t1, meta_t2, cat_t3, forn_t3, sim_store):
    logger.info(
        "refresh_all: tab=%s forn=%s fab=%s cat=%s cat_t3=%s forn_t3=%s",
        active_tab, forn, fab, cat, cat_t3, forn_t3
    )

    meta_t1_atual = _safe_float_percent(meta_t1, 0.30)
    meta_t2_atual = _safe_float_percent(meta_t2, 0.00)

    df_view_12 = _filter_tab12(forn, fab, cat)
    rows_t1 = build_tab1_rows(df_view_12, sim_store, meta_t1_atual)
    sum_t1 = compute_summary(df_view_12, bench_ano)

    rows_t2 = build_tab2_rows(df_view_12, sim_store, meta_t2_atual)
    sum_t2 = compute_summary(df_view_12, bench_ano)

    df_view_3 = _filter_tab3(cat_t3, forn_t3)
    rows_t3 = build_tab3_rows(df_view_3)
    sum_t3 = compute_summary(df_view_3, bench_ano)

    def kpi_children(summary):
        return [
            _format_kpi("Fat. Total:", f"R$ {summary['fat_total']:,.2f}", None),
            _format_kpi("Margem Média:", f"{summary['marg_pond']:.1%}", "blue"),
            html.Span("| ", style={"color": "#999"}),
            _format_kpi("Total SKUs:", str(summary["qtd_sku"]), None),
            _format_kpi("A:", str(summary["sku_a"]), "green"),
            _format_kpi("B:", str(summary["sku_b"]), "#bda404"),
            _format_kpi("C:", str(summary["sku_c"]), "red"),
        ]

    return (
        rows_t1,
        rows_t2,
        rows_t3,

        kpi_children(sum_t1),
        _breakdown_component(sum_t1["breakdown"]),

        kpi_children(sum_t2),
        _breakdown_component(sum_t2["breakdown"]),

        kpi_children(sum_t3),
        _breakdown_component(sum_t3["breakdown"]),
    )


@app.callback(
    Output("grid-t1", "columnSize"),
    Output("grid-t2", "columnSize"),
    Output("grid-t3", "columnSize"),
    Input("tabs", "active_tab"),
)
def fit_columns_on_visible_tab(active_tab):
    # Só manda sizeToFit para o grid visível.
    # Nos outros, no_update (evita width=0).
    if active_tab == "tab-1":
        return "sizeToFit", no_update, no_update
    if active_tab == "tab-2":
        return no_update, "sizeToFit", no_update
    if active_tab == "tab-3":
        return no_update, no_update, "sizeToFit"
    return no_update, no_update, no_update


# ---------- Atualizar histórico ao clicar em célula (tabs 1 e 2) ----------
@app.callback(
    Output("hist-box-t1", "children"),
    Output("hist-box-t2", "children"),
    Input("grid-t1", "cellClicked"),
    Input("grid-t2", "cellClicked"),
    Input("forn", "value"),
    Input("fab", "value"),
    Input("cat", "value"),
    Input("tabs", "active_tab"),
)
def on_cell_click(cell1, cell2, forn, fab, cat, active_tab):
    hist_default = {"produto": "Selecione...", "hist_6m": "-", "hist_3m": "-", "hist_nov": "-", "hist_pico": "-"}

    trig = ctx.triggered_id
    if trig in ("forn", "fab", "cat", "tabs"):
        return _history_component(hist_default, "t1").children, _history_component(hist_default, "t2").children

    if df_base.empty:
        return _history_component(hist_default, "t1").children, _history_component(hist_default, "t2").children

    if trig == "grid-t1" and active_tab == "tab-1" and cell1 and isinstance(cell1, dict):
        data = cell1.get("data") or {}
        produto_key = data.get("_produto_key")
        if produto_key and produto_key in df_base.index:
            row = df_base.loc[produto_key]
            return _history_component(build_history_payload(row), "t1").children, no_update

    if trig == "grid-t2" and active_tab == "tab-2" and cell2 and isinstance(cell2, dict):
        data = cell2.get("data") or {}
        produto_key = data.get("_produto_key")
        if produto_key and produto_key in df_base.index:
            row = df_base.loc[produto_key]
            return no_update, _history_component(build_history_payload(row), "t2").children

    return no_update, no_update


# ---------- Duplo clique: abrir modais ----------
@app.callback(
    Output("modal-fin", "is_open"),
    Output("modal-fin-title", "children"),
    Output("fin-p", "value"),
    Output("fin-m", "value"),
    Output("fin-p2", "value"),
    Output("fin-c", "value"),

    Output("modal-mkt", "is_open"),
    Output("modal-mkt-title", "children"),
    Output("mkt-menor-conc", "children"),
    Output("mkt-delta", "value"),

    Output("store-selected", "data"),

    Input("grid-t1", "cellDoubleClicked"),
    Input("grid-t2", "cellDoubleClicked"),
    Input("fin-close", "n_clicks"),
    Input("mkt-close", "n_clicks"),

    State("tabs", "active_tab"),
    State("meta_t1", "value"),
    State("meta_t2", "value"),
    State("modal-fin", "is_open"),
    State("modal-mkt", "is_open"),
)
def modal_controller(cell1, cell2, fin_close, mkt_close, active_tab, meta_t1, meta_t2, fin_is_open, mkt_is_open):
    trig = ctx.triggered_id

    # Defaults: mantém estado atual se não for o trigger certo
    fin_open = bool(fin_is_open)
    mkt_open = bool(mkt_is_open)

    # ---- Fechar modais ----
    if trig == "fin-close":
        return (
            False, no_update, no_update, no_update, no_update, no_update,
            mkt_open, no_update, no_update, no_update,
            no_update
        )

    if trig == "mkt-close":
        return (
            fin_open, no_update, no_update, no_update, no_update, no_update,
            False, no_update, no_update, no_update,
            no_update
        )

    meta_t1_atual = _safe_float_percent(meta_t1, 0.30)
    meta_t2_atual = _safe_float_percent(meta_t2, 0.00)

    # ---- Abrir Financeiro (Tab 1) ----
    if trig == "grid-t1" and active_tab == "tab-1" and cell1 and isinstance(cell1, dict):
        data = cell1.get("data") or {}
        produto_key = data.get("_produto_key")
        if not produto_key:
            return (
                fin_open, no_update, no_update, no_update, no_update, no_update,
                mkt_open, no_update, no_update, no_update,
                {"produto_key": None}
            )

        menor_conc = float(data.get("_menor_conc", 0.0))
        p_atual = float(data.get("_p_atual", 0.0))

        sim_manual_ativa = bool(data.get("_sim_manual_ativa", False))
        sim_preco_man = float(data.get("_sim_preco_man", 0.0))
        sim_marg_man = float(data.get("_sim_marg_man", 0.0))

        if sim_manual_ativa:
            val_p = sim_preco_man
            val_m = sim_marg_man * 100.0
        else:
            val_p = (menor_conc if menor_conc > 0 else p_atual)
            val_m = meta_t1_atual * 100.0

        custo_calc = float(calcular_custo_necessario(val_p, val_m / 100.0))

        fin_title = f"Financeiro: {str(data.get('_produto_nome',''))[:30]}"

        return (
            True, fin_title, f"{val_p:.2f}", f"{val_m:.1f}", f"{val_p:.2f}", f"{custo_calc:.2f}",
            False, "Simulação Mercado", "", "",
            {"produto_key": produto_key}
        )

    # ---- Abrir Mercado (Tab 2) ----
    if trig == "grid-t2" and active_tab == "tab-2" and cell2 and isinstance(cell2, dict):
        data = cell2.get("data") or {}
        produto_key = data.get("_produto_key")
        if not produto_key:
            return (
                fin_open, no_update, no_update, no_update, no_update, no_update,
                mkt_open, no_update, no_update, no_update,
                {"produto_key": None}
            )

        menor_conc = float(data.get("_menor_conc", 0.0))

        sim_conc_ativa = bool(data.get("_sim_conc_ativa", False))
        sim_conc_delta = float(data.get("_sim_conc_delta", 0.0))

        delta_atual = sim_conc_delta if sim_conc_ativa else meta_t2_atual

        mkt_title = f"Mercado: {str(data.get('_produto_nome',''))[:30]}"
        mkt_menor = f"Menor Concorrente: R$ {menor_conc:,.2f}"
        mkt_delta = f"{delta_atual*100.0:.1f}"

        return (
            False, "Simulação Fin.", "", "", "", "",
            True, mkt_title, mkt_menor, mkt_delta,
            {"produto_key": produto_key}
        )

    # Nada a fazer
    return (
        fin_open, no_update, no_update, no_update, no_update, no_update,
        mkt_open, no_update, no_update, no_update,
        no_update
    )

# ---------- Atualização do preço estimado no modal mercado (quando delta muda) ----------
@app.callback(
    Output("mkt-preco-est", "children"),
    Input("mkt-delta", "value"),
    Input("store-selected", "data"),
    Input("grid-t2", "rowData"),
)
def update_mkt_estimate(delta, selected, rowData):
    produto_key = (selected or {}).get("produto_key")
    if not produto_key or not rowData:
        return "-"

    try:
        d = float(str(delta).replace(",", ".")) / 100.0
    except Exception:
        return "-"

    row = next((r for r in rowData if r.get("_produto_key") == produto_key), None)
    if not row:
        return "-"

    menor_conc = float(row.get("_menor_conc", 0.0))
    p_atual = float(row.get("_p_atual", 0.0))
    p_est = (menor_conc * (1 + d)) if menor_conc > 0 else p_atual
    return f"R$ {p_est:,.2f}"


# ---------- Exportar ----------
@app.callback(
    Output("download-excel", "data"),
    Input("btn-export", "n_clicks"),
    State("tabs", "active_tab"),
    State("forn", "value"),
    State("fab", "value"),
    State("cat", "value"),
    State("cat_t3", "value"),
    State("forn_t3", "value"),
    State("store-sim", "data"),
    prevent_initial_call=True,
)
def export_excel(_, active_tab, forn, fab, cat, cat_t3, forn_t3, sim_store):
    if df_base.empty:
        return no_update

    # O desktop exporta df_view_atual (RAW) e muda o nome por aba.
    if active_tab in ("tab-1", "tab-2"):
        df_view = _filter_tab12(forn, fab, cat)
        nome_tipo = "FINANCEIRO" if active_tab == "tab-1" else "MERCADO"
    else:
        df_view = _filter_tab3(cat_t3, forn_t3)
        nome_tipo = "CATEGORIA"

    if df_view is None or df_view.empty:
        return no_update

    # Aplica simulações ao DF exportado como o desktop faria (df_base tinha colunas Sim_*).
    df_out = df_view.copy()

    # garante colunas
    for c in ["Sim_Manual_Ativa", "Sim_Preco_Manual", "Sim_Margem_Manual", "Sim_Conc_Ativa", "Sim_Conc_Delta"]:
        if c not in df_out.columns:
            df_out[c] = False if c.endswith("_Ativa") else 0.0

    manual = (sim_store or {}).get("manual", {})
    conc = (sim_store or {}).get("conc", {})

    for produto_key, state in manual.items():
        if produto_key in df_out.index and isinstance(state, dict) and state.get("ativa"):
            df_out.at[produto_key, "Sim_Manual_Ativa"] = True
            df_out.at[produto_key, "Sim_Preco_Manual"] = float(state.get("preco", 0.0))
            df_out.at[produto_key, "Sim_Margem_Manual"] = float(state.get("margem", 0.0))

    for produto_key, state in conc.items():
        if produto_key in df_out.index and isinstance(state, dict) and state.get("ativa"):
            df_out.at[produto_key, "Sim_Conc_Ativa"] = True
            df_out.at[produto_key, "Sim_Conc_Delta"] = float(state.get("delta", 0.0))

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=True)
    output.seek(0)

    filename = f"Simulacao_{nome_tipo}.xlsx"
    return dcc.send_bytes(output.getvalue(), filename)


if __name__ == "__main__":
    # Local:
    app.run(debug=True, host="127.0.0.1", port=8050)

    # Para publicar na rede (LAN/servidor):
    # app.run(debug=False, host="0.0.0.0", port=8050)
