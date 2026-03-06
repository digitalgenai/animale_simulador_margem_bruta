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
    calcular_margem_pond_percentual,
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


def compute_summary(df_view: pd.DataFrame, bench_ano: dict, month_ctx=None):
    """
    Correção crítica:
    - MG(mês) deve ser margem real PONDERADA/AGREGADA, não média de %.
    - GL(Year) deve vir do bench_ano calculado como margem real anual agregada / fat anual agregada.
    """
    if df_view is None or df_view.empty:
        return {
            "fat_total": 0.0,
            "marg_pond": 0.0,
            "qtd_sku": 0,
            "sku_a": 0,
            "sku_b": 0,
            "sku_c": 0,
            "breakdown": [],
        }

    # FAT do recorte
    fat_total = float(pd.to_numeric(df_view.get("Fat_Ref"), errors="coerce").fillna(0.0).sum())

    # Margem % ponderada do recorte (margem real agregada / fat agregado)
    marg_pond = float(calcular_margem_pond_percentual(df_view, col_fat="Fat_Ref", col_marg_val="Marg_Val_Ref"))

    # breakdown Top Categorias (Forn. vs Benchmarks)
    breakdown = []
    if "Area" in df_view.columns:
        gb = df_view.groupby("Area", dropna=False)
        for area, g in gb:
            fat = float(pd.to_numeric(g.get("Fat_Ref"), errors="coerce").fillna(0.0).sum())
            if fat <= 0:
                continue

            # MG(mês) correta: ponderada/real
            mg = float(calcular_margem_pond_percentual(g, col_fat="Fat_Ref", col_marg_val="Marg_Val_Ref"))

            # GL(Year): pega do bench_ano (pode ser float ou dict)
            b = bench_ano.get(area, 0.0)
            if isinstance(b, dict):
                gl = float(b.get("marg_perc", 0.0))
            else:
                gl = float(b or 0.0)

            breakdown.append(
                {
                    "categoria": str(area),
                    "fat": fat,
                    "marg_perc": mg,
                    "bench_ano": gl,
                }
            )

    # ordena top por FAT
    breakdown.sort(key=lambda x: x["fat"], reverse=True)
    breakdown = breakdown[:10]

    # SKUs e ABC
    qtd_sku = int(len(df_view))
    sku_a = int((df_view.get("Curva_ABC") == "A").sum()) if "Curva_ABC" in df_view.columns else 0
    sku_b = int((df_view.get("Curva_ABC") == "B").sum()) if "Curva_ABC" in df_view.columns else 0
    sku_c = int((df_view.get("Curva_ABC") == "C").sum()) if "Curva_ABC" in df_view.columns else 0

    return {
        "fat_total": fat_total,
        "marg_pond": marg_pond,
        "qtd_sku": qtd_sku,
        "sku_a": sku_a,
        "sku_b": sku_b,
        "sku_c": sku_c,
        "breakdown": breakdown,
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
