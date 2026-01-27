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


def _calc_curva_abc(df_prod: pd.DataFrame, fat_col: str) -> pd.Series:
    if fat_col not in df_prod.columns:
        return pd.Series(index=df_prod.index, data="C")

    s = df_prod[fat_col].fillna(0.0).astype(float)
    total = float(s.sum())
    if total <= 0:
        return pd.Series(index=df_prod.index, data="C")

    order = s.sort_values(ascending=False)
    acum = order.cumsum() / total

    abc = pd.Series(index=order.index, dtype="object")
    abc.loc[acum <= 0.80] = "A"
    abc.loc[(acum > 0.80) & (acum <= 0.95)] = "B"
    abc.loc[acum > 0.95] = "C"
    return abc.reindex(df_prod.index).fillna("C")


def _month_index(ts: pd.Series, start_month: pd.Timestamp) -> pd.Series:
    """
    Retorna o índice de mês (inteiro) relativo a start_month:
      start_month => 0
      start_month + 1 mês => 1
      ...
    """
    return (ts.dt.year - start_month.year) * 12 + (ts.dt.month - start_month.month)


def _build_month_labels(start_month: pd.Timestamp, n: int) -> List[str]:
    """
    Gera labels estáveis e ordenáveis para os meses da janela, por exemplo: "2025-02", ... "2026-01".
    Isso elimina bug de Janeiro (e qualquer mês) por desalinhamento de lista fixa.
    """
    labels: List[str] = []
    for i in range(n):
        dt = (start_month + pd.DateOffset(months=i)).normalize()
        labels.append(dt.strftime("%Y-%m"))
    return labels


def load_base_data() -> Tuple[
    pd.DataFrame,
    Dict[str, float],
    Dict[str, float],
    Dict[str, float],
    List[str],
    List[str],
]:
    load_dotenv()

    schema = os.getenv("PGSCHEMA", "stage")
    table = os.getenv("PGTABLE", "obt_faturamento")
    full = f"{schema}.{table}"

    logger.info("Carregando base do Postgres: %s", full)

    engine = get_engine()

    sql = text(
        f"""
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
        FROM {full}
        WHERE data_venda IS NOT NULL
        """
    )
    df_raw = pd.read_sql(sql, engine)

    if df_raw.empty:
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # Tipos / normalização
    df_raw["data_venda"] = pd.to_datetime(df_raw["data_venda"], errors="coerce")
    df_raw = df_raw.dropna(subset=["data_venda"]).copy()

    df_raw["qtd_venda"] = pd.to_numeric(df_raw["qtd_venda"], errors="coerce").fillna(0).astype(int)
    df_raw["total_item"] = pd.to_numeric(df_raw["total_item"], errors="coerce").fillna(0.0)
    df_raw["lucro_total"] = pd.to_numeric(df_raw["lucro_total"], errors="coerce").fillna(0.0)

    for c in ["cod_produto", "produto", "fornecedor", "fabricante", "area"]:
        df_raw[c] = df_raw[c].astype(str).fillna("").str.strip()

    # Mês de referência: mês mais recente do dataset
    max_dt = df_raw["data_venda"].max()
    if pd.isna(max_dt):
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    ref_month_start = pd.Timestamp(max_dt.year, max_dt.month, 1).normalize()

    # Tamanho da janela (ano-móvel): se LISTA_MESES_ANO existir, usa o tamanho dela.
    # Se quiser sempre 12, coloque n=12.
    n = len(LISTA_MESES_ANO) if isinstance(LISTA_MESES_ANO, (list, tuple)) and len(LISTA_MESES_ANO) > 0 else 12
    if n <= 0:
        raise RuntimeError("LISTA_MESES_ANO está vazio e n não pôde ser inferido.")

    start_month = (ref_month_start - pd.DateOffset(months=n - 1)).normalize()

    # Labels REAIS da janela (estáveis)
    LISTA_MESES_ANO_USO = _build_month_labels(start_month, n)
    MESES_3M_USO = LISTA_MESES_ANO_USO[-3:]
    MESES_6M_USO = LISTA_MESES_ANO_USO[-6:]

    logger.info(
        "Janela ano-móvel: start=%s ref=%s (n=%d). Labels(USO)=%s",
        start_month.date(),
        ref_month_start.date(),
        n,
        LISTA_MESES_ANO_USO,
    )

    # Coluna "mes" (início do mês)
    df_raw["mes"] = df_raw["data_venda"].dt.to_period("M").dt.to_timestamp().dt.normalize()

    # índice do mês dentro da janela [0..n-1]
    df_raw["mes_idx"] = _month_index(df_raw["mes"], start_month)

    # filtra somente meses dentro da janela
    df_win = df_raw[(df_raw["mes_idx"] >= 0) & (df_raw["mes_idx"] < n)].copy()

    if df_win.empty:
        min_mes = df_raw["mes"].min()
        max_mes = df_raw["mes"].max()
        logger.warning(
            "Nenhuma linha caiu na janela [start=%s..ref=%s]. Range real mes=[%s..%s].",
            start_month.date(),
            ref_month_start.date(),
            min_mes.date() if pd.notna(min_mes) else None,
            max_mes.date() if pd.notna(max_mes) else None,
        )
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # cria label via posição, mas usando labels da janela REAL (não config fixo)
    labels_arr = np.array(LISTA_MESES_ANO_USO, dtype=object)
    df_win["mes_label"] = labels_arr[df_win["mes_idx"].astype(int).to_numpy()]

    # Agrega por SKU x mês
    grp = (
        df_win.groupby(["cod_produto", "mes_label"], as_index=False)
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

    # Dimensões por SKU
    base_dim = (
        grp.groupby("cod_produto", as_index=False)
        .agg(
            Produto=("Produto", "first"),
            Fornecedor=("Fornecedor", "first"),
            Fabricante=("Fabricante", "first"),
            Area=("Area", "first"),
        )
        .rename(columns={"cod_produto": "SKU"})
    )

    # Pivots
    fat_pvt = grp.pivot_table(index="cod_produto", columns="mes_label", values="Fat", aggfunc="sum").fillna(0.0)
    marg_pvt = grp.pivot_table(index="cod_produto", columns="mes_label", values="Marg_Val", aggfunc="sum").fillna(0.0)
    qtd_pvt = grp.pivot_table(index="cod_produto", columns="mes_label", values="Qtd", aggfunc="sum").fillna(0.0)

    # garante todas as colunas na ordem da janela REAL
    for m in LISTA_MESES_ANO_USO:
        if m not in fat_pvt.columns:
            fat_pvt[m] = 0.0
        if m not in marg_pvt.columns:
            marg_pvt[m] = 0.0
        if m not in qtd_pvt.columns:
            qtd_pvt[m] = 0.0

    fat_pvt = fat_pvt[LISTA_MESES_ANO_USO]
    marg_pvt = marg_pvt[LISTA_MESES_ANO_USO]
    qtd_pvt = qtd_pvt[LISTA_MESES_ANO_USO]

    fat_pvt.columns = [f"Fat_{c}" for c in fat_pvt.columns]
    marg_pvt.columns = [f"Marg_Val_{c}" for c in marg_pvt.columns]
    qtd_pvt.columns = [f"Qtd_{c}" for c in qtd_pvt.columns]

    # Junta dimensões + pivots
    df_base = (
        base_dim.merge(fat_pvt.reset_index().rename(columns={"cod_produto": "SKU"}), on="SKU", how="left")
        .merge(marg_pvt.reset_index().rename(columns={"cod_produto": "SKU"}), on="SKU", how="left")
        .merge(qtd_pvt.reset_index().rename(columns={"cod_produto": "SKU"}), on="SKU", how="left")
    )

    # -------------------------
    # Pipeline compatível Excel
    # -------------------------

    # Textos
    for col in TEXT_COLS:
        if col in df_base.columns:
            df_base[col] = (
                df_base[col].astype(str).str.strip().replace(["nan", "NaN", ""], "SEM_INFO")
            )
            if col == "Cod_Barras":
                df_base[col] = df_base[col].str.replace(r"\.0$", "", regex=True)
        else:
            df_base[col] = "-"

    # Numéricos: BASE_NUM_COLS + Fat_ + Marg_Val_ + Qtd_ (da janela REAL)
    cols_num = list(BASE_NUM_COLS)
    for m in LISTA_MESES_ANO_USO:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}", f"Qtd_{m}"])

    _ensure_columns(df_base, cols_num, 0.0)
    for col in cols_num:
        df_base[col] = pd.to_numeric(df_base[col], errors="coerce").fillna(0.0)

    # Aux e concorrentes
    _ensure_columns(df_base, AUX_NUM_COLS, 0.0)

    if col_conc_1 not in df_base.columns:
        df_base[col_conc_1] = 0.0
    if col_conc_2 not in df_base.columns:
        df_base[col_conc_2] = 0.0

    # ==== Totais trimestre (3M REAL) ====
    cols_fat_3m = [f"Fat_{m}" for m in MESES_3M_USO]
    cols_marg_3m = [f"Marg_Val_{m}" for m in MESES_3M_USO]
    _ensure_columns(df_base, cols_fat_3m, 0.0)
    _ensure_columns(df_base, cols_marg_3m, 0.0)

    df_base["Fat_Total_Trimestre"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Valor_Margem_Total_Trimestre"] = df_base[cols_marg_3m].sum(axis=1)

    df_base["Margem_Media_Trimestre"] = (
        _safe_div(df_base["Valor_Margem_Total_Trimestre"], df_base["Fat_Total_Trimestre"])
    )

    # Curva ABC
    df_base["Curva_ABC"] = _calc_curva_abc(df_base, "Fat_Total_Trimestre")

    # escolhe o mês-ref como o último mês (label) que realmente tem venda (na janela REAL)
    mes_ref_label = None
    for m in reversed(LISTA_MESES_ANO_USO):
        fat_m = f"Fat_{m}"
        qtd_m = f"Qtd_{m}"
        if fat_m in df_base.columns and float(df_base[fat_m].sum()) > 0:
            mes_ref_label = m
            break
        if qtd_m in df_base.columns and float(df_base[qtd_m].sum()) > 0:
            mes_ref_label = m
            break

    if mes_ref_label is None:
        mes_ref_label = LISTA_MESES_ANO_USO[-1]

    # ==== Deriva colunas do mês ref ====
    fat_ref = f"Fat_{mes_ref_label}"
    marg_ref = f"Marg_Val_{mes_ref_label}"
    qtd_ref = f"Qtd_{mes_ref_label}"

    logger.info("mes_ref_label=%s | fat_ref=%s | qtd_ref=%s", mes_ref_label, fat_ref, qtd_ref)

    _ensure_columns(df_base, [fat_ref, marg_ref, qtd_ref], 0.0)

    logger.info(
        "sum(%s)=%.2f | sum(%s)=%.2f | raw_total=%.2f raw_qtd=%d",
        fat_ref,
        float(df_base[fat_ref].sum()),
        qtd_ref,
        float(df_base[qtd_ref].sum()),
        float(df_raw["total_item"].sum()),
        int(df_raw["qtd_venda"].sum()),
    )

    df_base["Qtd_Nov"] = df_base[qtd_ref].fillna(0.0).astype(int)
    df_base["Preco_Atual"] = _safe_div(df_base[fat_ref], df_base[qtd_ref])
    df_base["Custo"] = _safe_div(df_base[fat_ref] - df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Unit"] = _safe_div(df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Perc"] = _safe_div(df_base[marg_ref], df_base[fat_ref])

    # Aliases “humanos”
    df_base["Qtd Nov"] = df_base["Qtd_Nov"]
    df_base["Preço Atual"] = df_base["Preco_Atual"]
    df_base["Marg R$"] = df_base["Marg_Unit"]
    df_base["Marg %"] = df_base["Marg_Perc"]
    df_base["Marg Atual %"] = df_base["Marg_Perc"]

    # --- Benchmarks globais ---
    logger.info("Calculando Benchmarks Globais...")

    cols_fat_ano = [f"Fat_{m}" for m in LISTA_MESES_ANO_USO]
    cols_marg_ano = [f"Marg_Val_{m}" for m in LISTA_MESES_ANO_USO]
    cols_fat_6m = [f"Fat_{m}" for m in MESES_6M_USO]
    cols_marg_6m = [f"Marg_Val_{m}" for m in MESES_6M_USO]

    _ensure_columns(df_base, cols_fat_ano, 0.0)
    _ensure_columns(df_base, cols_marg_ano, 0.0)
    _ensure_columns(df_base, cols_fat_6m, 0.0)
    _ensure_columns(df_base, cols_marg_6m, 0.0)

    df_base["Temp_Fat_Ano"] = df_base[cols_fat_ano].sum(axis=1)
    df_base["Temp_Marg_Ano"] = df_base[cols_marg_ano].sum(axis=1)
    df_base["Temp_Fat_6M"] = df_base[cols_fat_6m].sum(axis=1)
    df_base["Temp_Marg_6M"] = df_base[cols_marg_6m].sum(axis=1)
    df_base["Temp_Fat_3M"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Temp_Marg_3M"] = df_base[cols_marg_3m].sum(axis=1)

    df_bench = df_base.groupby("Area")[
        [
            "Temp_Fat_Ano",
            "Temp_Marg_Ano",
            "Temp_Fat_6M",
            "Temp_Marg_6M",
            "Temp_Fat_3M",
            "Temp_Marg_3M",
        ]
    ].sum()

    df_bench["Bench_Ano"] = _safe_div(df_bench["Temp_Marg_Ano"], df_bench["Temp_Fat_Ano"])
    df_bench["Bench_6M"] = _safe_div(df_bench["Temp_Marg_6M"], df_bench["Temp_Fat_6M"])
    df_bench["Bench_3M"] = _safe_div(df_bench["Temp_Marg_3M"], df_bench["Temp_Fat_3M"])

    bench_ano = df_bench["Bench_Ano"].fillna(0.0).replace([float("inf"), float("-inf")], 0.0).to_dict()
    bench_6m = df_bench["Bench_6M"].fillna(0.0).replace([float("inf"), float("-inf")], 0.0).to_dict()
    bench_3m = df_bench["Bench_3M"].fillna(0.0).replace([float("inf"), float("-inf")], 0.0).to_dict()

    df_base.drop(
        columns=[
            "Temp_Fat_Ano",
            "Temp_Marg_Ano",
            "Temp_Fat_6M",
            "Temp_Marg_6M",
            "Temp_Fat_3M",
            "Temp_Marg_3M",
        ],
        inplace=True,
        errors="ignore",
    )

    # Índice por Produto
    df_base["Produto"] = df_base["Produto"].astype(str).str.strip()
    dupe_mask = df_base["Produto"].duplicated(keep=False)

    df_base["Produto_Key"] = df_base["Produto"]
    if dupe_mask.any():
        df_base.loc[dupe_mask, "Produto_Key"] = df_base.loc[dupe_mask].apply(
            lambda r: f"{r['Produto']} [{r.get('SKU', 'SEM_INFO')}]",
            axis=1,
        )

    # Garante colunas de simulação
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
    lista_fornecedores = forn_ranking.index.tolist() or ["SEM DADOS"]
    lista_categorias_global = sorted(df_base["Area"].astype(str).unique().tolist())

    # Log “prova de vida”
    try:
        sku_teste = "68056903"
        ex = df_base[df_base["SKU"].astype(str) == sku_teste].head(1)
        if not ex.empty:
            logger.info(
                "PROVA SKU=%s => Qtd_Nov=%s Preco_Atual=%.2f Custo=%.2f Marg%%=%.2f",
                sku_teste,
                int(ex["Qtd_Nov"].iloc[0]),
                float(ex["Preco_Atual"].iloc[0]),
                float(ex["Custo"].iloc[0]),
                float(ex["Marg_Perc"].iloc[0]) * 100.0,
            )
    except Exception:
        pass

    logger.info("Base carregada do Postgres: %d linhas", len(df_base))
    return df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global
