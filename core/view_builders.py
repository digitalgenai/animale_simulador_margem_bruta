from __future__ import annotations

from typing import Dict, Any, List, Tuple

import pandas as pd

from core.config import (
    col_conc_1,
    col_conc_2,
    NOME_CONC_1,
    NOME_CONC_2,
    MAX_ROWS_T1_T2,
)
from core.formatters import fmt_real, fmt_perc, fmt_qtd, fmt_str, fmt_media
from core.calculations import (
    get_menor_concorrente,
    dif_concorrente_custom,
    calcular_custo_necessario,
    calcular_margem_real_percentual,
    calcular_margem_real_valor,
)

from core.data_loader import get_month_context


def _get_sim_state(sim_store: Dict[str, Any], produto_key: str) -> Tuple[bool, float, float, bool, float]:
    """
    Retorna:
      sim_manual_ativa, sim_preco_man, sim_marg_man, sim_conc_ativa, sim_conc_delta
    (equivalente ao acesso em df_base no desktop)
    """
    manual = (sim_store or {}).get("manual", {}).get(produto_key)
    conc = (sim_store or {}).get("conc", {}).get(produto_key)

    sim_manual_ativa = bool(manual.get("ativa")) if isinstance(manual, dict) else False
    sim_preco_man = float(manual.get("preco", 0.0)) if isinstance(manual, dict) else 0.0
    sim_marg_man = float(manual.get("margem", 0.0)) if isinstance(manual, dict) else 0.0

    sim_conc_ativa = bool(conc.get("ativa")) if isinstance(conc, dict) else False
    sim_conc_delta = float(conc.get("delta", 0.0)) if isinstance(conc, dict) else 0.0

    return sim_manual_ativa, sim_preco_man, sim_marg_man, sim_conc_ativa, sim_conc_delta


def compute_summary(df_view_atual: pd.DataFrame, bench_ano: Dict[str, float], month_ctx: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if df_view_atual is None or df_view_atual.empty:
        return {
            "fat_total": 0.0,
            "marg_pond": 0.0,
            "qtd_sku": 0,
            "sku_a": 0,
            "sku_b": 0,
            "sku_c": 0,
            "breakdown": [],
            "hist_placeholder": True,
            "margem_5m": 0.0,
        }

    fat_total = float(df_view_atual["Fat_Total_Trimestre"].sum())
    marg_pond = (
        float((df_view_atual["Fat_Total_Trimestre"] * df_view_atual["Margem_Media_Trimestre"]).sum()) / fat_total
        if fat_total > 0
        else 0.0
    )

    ctx = month_ctx or get_month_context()
    labels_legacy = ctx.get("labels_legacy") or []
    labels_5m = labels_legacy[-5:] if len(labels_legacy) >= 5 else []

    cols_fat_5m = [f"Fat_{m}" for m in labels_5m]
    cols_marg_val_5m = [f"Marg_Val_{m}" for m in labels_5m]

    if labels_5m and all(c in df_view_atual.columns for c in cols_fat_5m + cols_marg_val_5m):
        fat_5m = float(df_view_atual[cols_fat_5m].sum().sum())
        marg_5m = float(df_view_atual[cols_marg_val_5m].sum().sum())
        margem_5m = (marg_5m / fat_5m) if fat_5m > 0 else 0.0
    else:
        margem_5m = 0.0

    counts_abc = df_view_atual["Curva_ABC"].value_counts()
    sku_a = int(counts_abc.get("A", 0))
    sku_b = int(counts_abc.get("B", 0))
    sku_c = int(counts_abc.get("C", 0))

    # Breakdown (Top 5 categorias)
    df_bd = (
        df_view_atual.groupby("Area")
        .agg(Fat=("Fat_Total_Trimestre", "sum"), Marg=("Valor_Margem_Total_Trimestre", "sum"))
        .sort_values("Fat", ascending=False)
        .head(5)
    )

    breakdown = []
    for cat, row in df_bd.iterrows():
        f = float(row["Fat"])
        m = float(row["Marg"])
        m_perc = (m / f) if f > 0 else 0.0
        breakdown.append(
            {
                "categoria": str(cat),
                "fat": f,
                "marg_perc": m_perc,
                "bench_ano": float(bench_ano.get(cat, 0.0)),
            }
        )

    return {
        "fat_total": fat_total,
        "marg_pond": marg_pond,
        "qtd_sku": int(len(df_view_atual)),
        "sku_a": sku_a,
        "sku_b": sku_b,
        "sku_c": sku_c,
        "breakdown": breakdown,
        "hist_placeholder": True,
        "margem_5m": margem_5m,
    }


def build_tab1_rows(df_view_atual: pd.DataFrame, sim_store: Dict[str, Any], meta_t1_atual: float) -> List[Dict[str, Any]]:
    rows = []
    if df_view_atual is None or df_view_atual.empty:
        return rows

    df_limit = df_view_atual.head(MAX_ROWS_T1_T2)

    for _, row in df_limit.iterrows():
        produto_key = row["Produto_Key"]
        produto_nome = row.get("Produto", produto_key)
        area = str(row.get("Area", ""))

        p_atual = float(row.get("Preco_Mais_Recente", 0.0))
        c_atual = float(row.get("Custo_Mais_Recente", 0.0))

        sim_manual_ativa, sim_preco_man, sim_marg_man, _, _ = _get_sim_state(sim_store, produto_key)

        menor_conc = float(get_menor_concorrente(row))
        val_conc1 = float(row.get(col_conc_1, 0.0))
        val_conc2 = float(row.get(col_conc_2, 0.0))

        marg_real = float(calcular_margem_real_percentual(c_atual, p_atual, area=area))
        dif_conc = float(dif_concorrente_custom(p_atual, menor_conc))

        is_neg = marg_real < 0
        is_yellow = (menor_conc > 0 and p_atual < menor_conc and (menor_conc - p_atual) > 1)

        # Simulação
        if sim_manual_ativa:
            sim_p = float(sim_preco_man)
            sim_m = float(sim_marg_man)
            sim_c = float(calcular_custo_necessario(sim_p, sim_m, area=area))
        else:
            sim_p = float(menor_conc if menor_conc > 0 else p_atual)
            sim_m = float(meta_t1_atual)
            sim_c = float(calcular_custo_necessario(sim_p, sim_m, area=area))

        rows.append(
            {
                "id": produto_key,

                "SKU": fmt_str(row.get("SKU")),
                "Produto": str(produto_nome),
                "ABC": fmt_str(row.get("Curva_ABC")),
                "Categ": fmt_str(row.get("Area")),
                "Qtd Ref": fmt_qtd(row.get("Qtd_Media_Mensal", 0.0)),

                "Preço Atual": fmt_real(p_atual),
                "Custo": fmt_real(c_atual),
                "Marg R$": fmt_real(calcular_margem_real_valor(c_atual, p_atual, area=area)),
                "Marg %": fmt_perc(marg_real),

                NOME_CONC_1: fmt_real(val_conc1),
                NOME_CONC_2: fmt_real(val_conc2),
                "Dif % (Menor)": fmt_perc(dif_conc),

                "Sim Preço": fmt_real(sim_p),
                "Sim Marg": fmt_perc(sim_m),
                "Sim Custo Nec": fmt_real(sim_c),

                "_produto_key": produto_key,
                "_produto_nome": str(produto_nome),
                "_p_atual": p_atual,
                "_c_atual": c_atual,
                "_menor_conc": menor_conc,
                "_meta_t1": meta_t1_atual,
                "_sim_manual_ativa": sim_manual_ativa,
                "_sim_preco_man": sim_preco_man,
                "_sim_marg_man": sim_marg_man,
                "_area": area,

                "__is_neg": is_neg,
                "__is_yellow": (not is_neg) and is_yellow,
            }
        )

    return rows


def build_tab2_rows(df_view_atual: pd.DataFrame, sim_store: Dict[str, Any], meta_t2_atual: float) -> List[Dict[str, Any]]:
    rows = []
    if df_view_atual is None or df_view_atual.empty:
        return rows

    df_limit = df_view_atual.head(MAX_ROWS_T1_T2)

    for _, row in df_limit.iterrows():
        produto_key = row["Produto_Key"]
        produto_nome = row.get("Produto", produto_key)
        area = str(row.get("Area", ""))

        p_atual = float(row.get("Preco_Mais_Recente", 0.0))
        c_atual = float(row.get("Custo_Mais_Recente", 0.0))

        _, _, _, sim_conc_ativa, sim_conc_delta = _get_sim_state(sim_store, produto_key)

        menor_conc = float(get_menor_concorrente(row))
        val_conc1 = float(row.get(col_conc_1, 0.0))
        val_conc2 = float(row.get(col_conc_2, 0.0))

        marg_real = float(calcular_margem_real_percentual(c_atual, p_atual, area=area))
        dif_atual = float(dif_concorrente_custom(p_atual, menor_conc))

        is_neg = marg_real < 0
        is_yellow = (menor_conc > 0 and p_atual < menor_conc and (menor_conc - p_atual) > 1)

        delta_target = float(sim_conc_delta if sim_conc_ativa else meta_t2_atual)

        if menor_conc > 0:
            sim_p_conc = float(menor_conc * (1 + delta_target))
        else:
            sim_p_conc = float(p_atual)

        sim_marg_result = float(calcular_margem_real_percentual(c_atual, sim_p_conc, area=area))

        delta_str = fmt_perc(delta_target)
        if sim_conc_ativa:
            delta_str += " (M)"

        rows.append(
            {
                "id": produto_key,

                "SKU": fmt_str(row.get("SKU")),
                "Produto": str(produto_nome),
                "ABC": fmt_str(row.get("Curva_ABC")),
                "Categ": fmt_str(row.get("Area")),
                "Qtd Ref": fmt_qtd(row.get("Qtd_Media_Mensal", 0.0)),

                "Preço Atual": fmt_real(p_atual),
                "Custo": fmt_real(c_atual),
                "Marg Atual %": fmt_perc(marg_real),

                NOME_CONC_1: fmt_real(val_conc1),
                NOME_CONC_2: fmt_real(val_conc2),
                "Dif Atual (Menor)": fmt_perc(dif_atual),

                "DELTA ALVO %": delta_str,
                "Sim Preço (Conc)": fmt_real(sim_p_conc),
                "Sim Margem (Result)": fmt_perc(sim_marg_result),

                "_produto_key": produto_key,
                "_produto_nome": str(produto_nome),
                "_p_atual": p_atual,
                "_c_atual": c_atual,
                "_menor_conc": menor_conc,
                "_meta_t2": meta_t2_atual,
                "_sim_conc_ativa": sim_conc_ativa,
                "_sim_conc_delta": sim_conc_delta,
                "_area": area,

                "__is_neg": is_neg,
                "__is_yellow": (not is_neg) and is_yellow,
            }
        )

    return rows


def build_tab3_rows(df_view_atual: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = []
    if df_view_atual is None or df_view_atual.empty:
        return rows

    if "Fat_Ref" not in df_view_atual.columns:
        df_view_atual = df_view_atual.assign(Fat_Ref=0.0)
    if "Marg_Val_Ref" not in df_view_atual.columns:
        df_view_atual = df_view_atual.assign(Marg_Val_Ref=0.0)

    df_agg = df_view_atual.groupby("Fornecedor")[["Fat_Ref", "Marg_Val_Ref"]].sum()
    df_agg = df_agg.sort_values("Fat_Ref", ascending=False)

    for forn_nome, row in df_agg.iterrows():
        f_ref = float(row["Fat_Ref"])
        m_ref_val = float(row["Marg_Val_Ref"])
        m_ref_perc = (m_ref_val / f_ref) if f_ref > 0 else 0.0

        rows.append(
            {
                "id": str(forn_nome),
                "Fornecedor": str(forn_nome),
                "Fat Ref": fmt_real(f_ref),
                "Margem Ref R$": fmt_real(m_ref_val),
                "Margem Ref %": fmt_perc(m_ref_perc),
            }
        )

    return rows


def build_history_payload(row: pd.Series) -> Dict[str, Any]:
    """
    Replica a atualização do painel 'Detalhes (Inteligência Temporal)' das abas 1 e 2.
    """
    produto_nome = str(row.get("Produto", ""))
    return {
        "produto": (produto_nome[:30] + "...") if len(produto_nome) > 30 else produto_nome,
        "hist_6m": fmt_media(row.get("Hist_Qtd_Media_6M", 0.0)),
        "hist_3m": fmt_media(row.get("Hist_Qtd_Media_3M", 0.0)),
        "hist_ref": fmt_media(row.get("Qtd_Media_Mensal", 0.0)),
        "hist_pico": f"{row.get('Hist_Mes_Pico','SEM_INFO')} ({fmt_qtd(row.get('Hist_Qtd_Pico', 0.0))})",
    }
