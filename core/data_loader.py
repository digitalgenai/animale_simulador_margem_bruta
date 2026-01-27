from __future__ import annotations

import logging
import os
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from core.db import get_engine
from core.config import (
    TEXT_COLS,
    BASE_NUM_COLS,
    AUX_NUM_COLS,
    LISTA_MESES_ANO,
    MESES_6M,
    MESES_3M,
    SIM_COLS_DEFAULTS,
    col_conc_1,
    col_conc_2,
    COLUNA_AGREGACAO_PRINCIPAL,
)

logger = logging.getLogger(__name__)


def _ensure_columns(df: pd.DataFrame, cols: List[str], default):
    for col in cols:
        if col not in df.columns:
            df[col] = default


def _safe_div(n: pd.Series, d: pd.Series) -> pd.Series:
    d2 = d.replace({0: np.nan})
    out = n / d2
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def load_base_data() -> Tuple[pd.DataFrame, Dict[str, float], Dict[str, float], Dict[str, float], List[str], List[str]]:
    """
    Carrega e prepara df_base + benchmarks a partir do Postgres (stage.obt_faturamento),
    mantendo o MESMO "contrato" do loader do Excel.

    Retorna:
      df_base (index Produto_Key)
      bench_ano, bench_6m, bench_3m (dict por Area)
      lista_fornecedores (ranking)
      lista_categorias_global (sorted unique de Area)
    """
    load_dotenv()

    schema = os.getenv("PGSCHEMA", "stage")
    table = os.getenv("PGTABLE", "obt_faturamento")

    logger.info("Carregando base do Postgres: %s.%s", schema, table)

    engine = get_engine()

    # Lê os dados brutos (vendas) necessários para derivar as colunas do "modelo Excel"
    sql = text(f"""
        SELECT
            cod_produto,
            produto,
            fornecedor,
            fabricante,
            area,
            data_venda,
            qtd_venda,
            total_item,
            lucro_total
        FROM {schema}.{table}
        WHERE data_venda IS NOT NULL
    """)
    df_raw = pd.read_sql(sql, engine)

    if df_raw.empty:
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # Tipos
    df_raw["data_venda"] = pd.to_datetime(df_raw["data_venda"], errors="coerce")
    df_raw = df_raw.dropna(subset=["data_venda"]).copy()

    df_raw["qtd_venda"] = pd.to_numeric(df_raw["qtd_venda"], errors="coerce").fillna(0).astype(int)
    df_raw["total_item"] = pd.to_numeric(df_raw["total_item"], errors="coerce").fillna(0.0)
    df_raw["lucro_total"] = pd.to_numeric(df_raw["lucro_total"], errors="coerce").fillna(0.0)

    # Normaliza strings
    for c in ["cod_produto", "produto", "fornecedor", "fabricante", "area"]:
        df_raw[c] = df_raw[c].astype(str).fillna("").str.strip()

    # Mês de referência: usa o mês mais recente do dataset
    max_dt = df_raw["data_venda"].max()
    ref_month_start = pd.Timestamp(max_dt.year, max_dt.month, 1)

    n = len(LISTA_MESES_ANO)
    if n <= 0:
        raise RuntimeError("LISTA_MESES_ANO está vazio no core.config.")

    logger.info("LISTA_MESES_ANO possui %d itens (ano-móvel).", n)

    # Assumimos que LISTA_MESES_ANO representa os 'n' meses terminando no mês ref.
    # i=0 => (n-1) meses atrás ... i=(n-1) => mês ref
    month_ts_by_label: Dict[str, pd.Timestamp] = {}
    for i, label in enumerate(LISTA_MESES_ANO):
        offset_months = i - (n - 1)
        month_ts_by_label[label] = (ref_month_start + pd.DateOffset(months=offset_months)).normalize()

    # Cria coluna "mes" (início do mês) para pivotar
    df_raw["mes"] = df_raw["data_venda"].dt.to_period("M").dt.to_timestamp()

    # Agrega por SKU x Mês (fat e marg_val)
    grp = (
        df_raw.groupby(["cod_produto", "mes"], as_index=False)
        .agg(
            Produto=("produto", "first"),
            Fornecedor=("fornecedor", "first"),
            Fabricante=("fabricante", "first"),
            Area=("area", "first"),
            Qtd=("qtd_venda", "sum"),
            Fat=("total_item", "sum"),
            Marg_Val=("lucro_total", "sum"),
        )
    )

    # Base por SKU (dimensões)
    base_dim = (
        grp.groupby("cod_produto", as_index=False)
        .agg(
            Produto=("Produto", "first"),
            Fornecedor=("Fornecedor", "first"),
            Fabricante=("Fabricante", "first"),
            Area=("Area", "first"),
        )
    )
    base_dim = base_dim.rename(columns={"cod_produto": "SKU"})

    # Pivot Fat_{m} e Marg_Val_{m} seguindo LISTA_MESES_ANO
    # Primeiro mapeia cada linha para o "label" do mês (se estiver dentro do range dos 12 meses)
    ts_to_label = {v: k for k, v in month_ts_by_label.items()}
    grp["mes_label"] = grp["mes"].map(ts_to_label)

    grp_12m = grp[grp["mes_label"].notna()].copy()

    fat_pvt = grp_12m.pivot_table(index="cod_produto", columns="mes_label", values="Fat", aggfunc="sum").fillna(0.0)
    marg_pvt = grp_12m.pivot_table(index="cod_produto", columns="mes_label", values="Marg_Val", aggfunc="sum").fillna(0.0)

    # Garante todas as colunas do ano na ordem do config
    for m in LISTA_MESES_ANO:
        if m not in fat_pvt.columns:
            fat_pvt[m] = 0.0
        if m not in marg_pvt.columns:
            marg_pvt[m] = 0.0

    fat_pvt = fat_pvt[LISTA_MESES_ANO]
    marg_pvt = marg_pvt[LISTA_MESES_ANO]

    fat_pvt.columns = [f"Fat_{c}" for c in fat_pvt.columns]
    marg_pvt.columns = [f"Marg_Val_{c}" for c in marg_pvt.columns]

    # Junta dimensões + pivots
    df_base = base_dim.merge(
        fat_pvt.reset_index().rename(columns={"cod_produto": "SKU"}),
        on="SKU",
        how="left",
    ).merge(
        marg_pvt.reset_index().rename(columns={"cod_produto": "SKU"}),
        on="SKU",
        how="left",
    )

    df_base.fillna(0.0, inplace=True)

    # --- A partir daqui, replica o seu pipeline atual do Excel ---

    # Tratamento Textos (usa TEXT_COLS do config)
    for col in TEXT_COLS:
        if col in df_base.columns:
            df_base[col] = (
                df_base[col]
                .astype(str)
                .str.strip()
                .replace(["nan", "NaN", ""], "SEM_INFO")
            )
            if col == "Cod_Barras":
                df_base[col] = df_base[col].str.replace(r"\.0$", "", regex=True)
        else:
            df_base[col] = "-"

    # Tratamento Numéricos: BASE_NUM_COLS + Fat_{m} + Marg_Val_{m}
    cols_num = list(BASE_NUM_COLS)
    for m in LISTA_MESES_ANO:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}"])

    _ensure_columns(df_base, cols_num, 0.0)
    for col in cols_num:
        df_base[col] = pd.to_numeric(df_base[col], errors="coerce").fillna(0)

    # Aux e concorrentes
    _ensure_columns(df_base, AUX_NUM_COLS, 0.0)

    if col_conc_1 not in df_base.columns:
        df_base[col_conc_1] = 0.0
    if col_conc_2 not in df_base.columns:
        df_base[col_conc_2] = 0.0

    # ==== Recria os campos totais usados no simulador (se não vierem prontos) ====
    # Trimestre = MESES_3M (normalmente últimos 3 do ano móvel)
    cols_fat_3m = [f"Fat_{m}" for m in MESES_3M]
    cols_marg_3m = [f"Marg_Val_{m}" for m in MESES_3M]

    _ensure_columns(df_base, cols_fat_3m, 0.0)
    _ensure_columns(df_base, cols_marg_3m, 0.0)

    # Se seu Excel já tinha esses campos, aqui garantimos consistência.
    df_base["Fat_Total_Trimestre"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Valor_Margem_Total_Trimestre"] = df_base[cols_marg_3m].sum(axis=1)

    # Margem média trimestre (igual ao original)
    df_base["Margem_Media_Trimestre"] = (
        (df_base["Valor_Margem_Total_Trimestre"] / df_base["Fat_Total_Trimestre"])
        .fillna(0)
        .replace([pd.NA, float("inf"), float("-inf")], 0)
    )

    # --- CÁLCULO DOS BENCHMARKS GLOBAIS (igual ao original) ---
    logger.info("Calculando Benchmarks Globais...")

    cols_fat_ano = [f"Fat_{m}" for m in LISTA_MESES_ANO]
    cols_marg_ano = [f"Marg_Val_{m}" for m in LISTA_MESES_ANO]
    cols_fat_6m = [f"Fat_{m}" for m in MESES_6M]
    cols_marg_6m = [f"Marg_Val_{m}" for m in MESES_6M]

    df_base["Temp_Fat_Ano"] = df_base[cols_fat_ano].sum(axis=1)
    df_base["Temp_Marg_Ano"] = df_base[cols_marg_ano].sum(axis=1)
    df_base["Temp_Fat_6M"] = df_base[cols_fat_6m].sum(axis=1)
    df_base["Temp_Marg_6M"] = df_base[cols_marg_6m].sum(axis=1)
    df_base["Temp_Fat_3M"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Temp_Marg_3M"] = df_base[cols_marg_3m].sum(axis=1)

    df_bench = df_base.groupby("Area")[[
        "Temp_Fat_Ano", "Temp_Marg_Ano",
        "Temp_Fat_6M", "Temp_Marg_6M",
        "Temp_Fat_3M", "Temp_Marg_3M"
    ]].sum()

    df_bench["Bench_Ano"] = _safe_div(df_bench["Temp_Marg_Ano"], df_bench["Temp_Fat_Ano"])
    df_bench["Bench_6M"] = _safe_div(df_bench["Temp_Marg_6M"], df_bench["Temp_Fat_6M"])
    df_bench["Bench_3M"] = _safe_div(df_bench["Temp_Marg_3M"], df_bench["Temp_Fat_3M"])

    bench_ano = df_bench["Bench_Ano"].fillna(0).replace([float("inf"), float("-inf")], 0).to_dict()
    bench_6m = df_bench["Bench_6M"].fillna(0).replace([float("inf"), float("-inf")], 0).to_dict()
    bench_3m = df_bench["Bench_3M"].fillna(0).replace([float("inf"), float("-inf")], 0).to_dict()

    # Drop temps
    df_base.drop(
        columns=[
            "Temp_Fat_Ano", "Temp_Marg_Ano",
            "Temp_Fat_6M", "Temp_Marg_6M",
            "Temp_Fat_3M", "Temp_Marg_3M"
        ],
        inplace=True,
        errors="ignore",
    )

    # --- Índice por Produto (mantém seu comportamento) ---
    df_base["Produto"] = df_base["Produto"].astype(str).str.strip()
    dupe_mask = df_base["Produto"].duplicated(keep=False)

    df_base["Produto_Key"] = df_base["Produto"]
    if dupe_mask.any():
        df_base.loc[dupe_mask, "Produto_Key"] = df_base.loc[dupe_mask].apply(
            lambda r: f"{r['Produto']} [{r.get('SKU','SEM_INFO')}]",
            axis=1
        )

    # Garante colunas de simulação existirem
    for col, default in SIM_COLS_DEFAULTS.items():
        if col not in df_base.columns:
            df_base[col] = default

    df_base = df_base.set_index("Produto_Key", drop=False)

    # Listas globais
    forn_ranking = (
        df_base.groupby(COLUNA_AGREGACAO_PRINCIPAL)["Fat_Total_Trimestre"]
        .sum()
        .sort_values(ascending=False)
    )
    lista_fornecedores = forn_ranking.index.tolist()
    if not lista_fornecedores:
        lista_fornecedores = ["SEM DADOS"]

    lista_categorias_global = sorted(df_base["Area"].unique().tolist())

    logger.info("Base carregada do Postgres: %d linhas", len(df_base))
    return df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global
