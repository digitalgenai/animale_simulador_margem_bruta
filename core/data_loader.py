from __future__ import annotations

import logging
from typing import Dict, Tuple, List

import pandas as pd

from core.config import (
    BASE_SIMULADOR_PATH,
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


def load_base_data() -> Tuple[pd.DataFrame, Dict[str, float], Dict[str, float], Dict[str, float], List[str], List[str]]:
    """
    Carrega e prepara df_base + benchmarks.
    Retorna:
      df_base (index Produto_Key)
      bench_ano, bench_6m, bench_3m (dict por Area)
      lista_fornecedores (ranking)
      lista_categorias_global (sorted unique de Area)
    """
    logger.info("Carregando base: %s", BASE_SIMULADOR_PATH)

    df_base = pd.read_excel(BASE_SIMULADOR_PATH)
    df_base.columns = df_base.columns.str.strip()

    if "Cod. Produto" in df_base.columns:
        df_base.rename(columns={"Cod. Produto": "SKU"}, inplace=True)

    df_base.dropna(subset=["Produto"], inplace=True)

    # Tratamento Textos
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

    # Tratamento Numéricos
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

    # Margem média trimestre (igual ao original)
    df_base["Margem_Media_Trimestre"] = (
        (df_base["Valor_Margem_Total_Trimestre"] / df_base["Fat_Total_Trimestre"])
        .fillna(0)
        .replace([pd.NA, float("inf"), float("-inf")], 0)
    )

    # --- CÁLCULO DOS BENCHMARKS GLOBAIS ---
    logger.info("Calculando Benchmarks Globais...")

    cols_fat_ano = [f"Fat_{m}" for m in LISTA_MESES_ANO]
    cols_marg_ano = [f"Marg_Val_{m}" for m in LISTA_MESES_ANO]
    cols_fat_6m = [f"Fat_{m}" for m in MESES_6M]
    cols_marg_6m = [f"Marg_Val_{m}" for m in MESES_6M]
    cols_fat_3m = [f"Fat_{m}" for m in MESES_3M]
    cols_marg_3m = [f"Marg_Val_{m}" for m in MESES_3M]

    # Temporárias
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

    df_bench["Bench_Ano"] = df_bench["Temp_Marg_Ano"] / df_bench["Temp_Fat_Ano"]
    df_bench["Bench_6M"] = df_bench["Temp_Marg_6M"] / df_bench["Temp_Fat_6M"]
    df_bench["Bench_3M"] = df_bench["Temp_Marg_3M"] / df_bench["Temp_Fat_3M"]

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

    # --- Índice por Produto (web precisa de key estável; preserva comportamento)
    # Se houver duplicidade de Produto, criamos Produto_Key = "Produto [SKU]" para manter selecionável e único.
    df_base["Produto"] = df_base["Produto"].astype(str).str.strip()
    dupe_mask = df_base["Produto"].duplicated(keep=False)

    df_base["Produto_Key"] = df_base["Produto"]
    if dupe_mask.any():
        df_base.loc[dupe_mask, "Produto_Key"] = df_base.loc[dupe_mask].apply(
            lambda r: f"{r['Produto']} [{r.get('SKU','SEM_INFO')}]", axis=1
        )

    # Garante colunas de simulação existirem (igual ao original)
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

    logger.info("Base carregada: %d linhas", len(df_base))
    return df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global
