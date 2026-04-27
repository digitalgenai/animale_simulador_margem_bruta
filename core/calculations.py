from __future__ import annotations

import unicodedata
from typing import Optional

import numpy as np
import pandas as pd

from core.config import TAXA_DEDUCAO_FATURAMENTO, TAXA_ESTETICA_SAUDE, col_conc_1, col_conc_2


def _norm_txt(s: str) -> str:
    """
    Normaliza texto pra comparação (lower + remove acentos + trim).
    """
    if s is None:
        return ""
    s2 = str(s).strip().lower()
    s2 = unicodedata.normalize("NFKD", s2)
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    return s2


def is_estetica_saude(area: Optional[str]) -> bool:
    """
    Regra: aplica taxa extra (2.38%) para itens cuja Area remeta a Estética e/ou Saúde.

    Implementação robusta:
    - aceita variações com/sem acento
    - aceita compostos (ex: "Estética e Saúde", "Saude", "Estetica", etc.)
    """
    a = _norm_txt(area or "")
    if not a:
        return False
    return ("estetic" in a) or ("saude" in a) or ("saud" in a)


def _taxa_deducao(area: Optional[str] = None, taxa_extra: float = 0.0) -> float:
    """
    Retorna a taxa de dedução aplicável.

    Regra: TAXA_DEDUCAO_FATURAMENTO (22,03%) para todas as categorias,
    incluindo Saúde/Estética, alinhando com a lógica do Power BI.
    taxa_extra (opcional) é somada ao resultado final.
    """
    return TAXA_DEDUCAO_FATURAMENTO + float(taxa_extra or 0.0)


def get_menor_concorrente(row: dict) -> float:
    c1 = row.get(col_conc_1, 0)
    c2 = row.get(col_conc_2, 0)
    c1 = c1 if isinstance(c1, (int, float)) and not np.isnan(c1) else 0
    c2 = c2 if isinstance(c2, (int, float)) and not np.isnan(c2) else 0
    if c1 > 0 and c2 > 0:
        return min(c1, c2)
    elif c1 > 0:
        return c1
    elif c2 > 0:
        return c2
    return 0.0


def dif_concorrente_custom(preco_atual: float, conc_referencia: float) -> float:
    if conc_referencia == 0:
        return 0.0
    try:
        return (preco_atual - conc_referencia) / conc_referencia
    except Exception:
        return 0.0


def calcular_custo_necessario(preco_alvo: float, margem_alvo: float, area: Optional[str] = None) -> float:
    """
    Custo necessário para atingir margem alvo (percentual) dado um preço.
    Considera:
      - TAXA_DEDUCAO_FATURAMENTO (padrão)
      - + TAXA_ESTETICA_SAUDE quando Area for Estética/Saúde
    """
    if preco_alvo <= 0:
        return 0.0
    try:
        td = _taxa_deducao(area)
        return (preco_alvo * (1 - td)) - (preco_alvo * margem_alvo)
    except Exception:
        return 0.0


def calcular_margem_real_percentual(custo: float, preco: float, area: Optional[str] = None) -> float:
    """
    Margem real percentual (sobre o preço) considerando deduções.
    """
    if preco <= 0:
        return 0.0
    try:
        td = _taxa_deducao(area)
        return ((preco * (1 - td)) - custo) / preco
    except Exception:
        return 0.0


def calcular_margem_real_valor(custo: float, preco: float, area: Optional[str] = None) -> float:
    """
    Margem real em R$ considerando deduções.
    """
    if preco <= 0:
        return 0.0
    try:
        td = _taxa_deducao(area)
        return (preco * (1 - td)) - custo
    except Exception:
        return 0.0


def calcular_margem_real_valor_total(
    df: "pd.DataFrame",
    *,
    col_fat: str = "Fat_Ref",
    col_marg_val: str = "Marg_Val_Ref",
    col_area: str = "Area",
    # fallback (caso não exista Marg_Val/Fat)
    col_preco: str = "Preco_Ref",
    col_custo: str = "Custo_Ref",
    col_qtd: str = "Qtd_Ref",
) -> float:
    """
    Soma da margem real em R$ (com dedução), agregada no dataframe.

    Preferência (mais consistente com o pipeline atual):
      - Se existirem colunas de FAT e margem bruta em R$ (ex: Fat_Ref / Marg_Val_Ref),
        calcula:
            margem_real_val = marg_bruta_val - (taxa_deducao(area) * fat)

        E agrega somando tudo.

    Fallback:
      - Se não tiver Marg_Val/Fat, tenta:
            sum( ((preco*(1-td)) - custo) * qtd )

    Obs: se qtd não existir, assume 1.
    """
    if df is None or df.empty:
        return 0.0

    def _pick(*cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    c_area = _pick(col_area, "Area", "Categ", "Categoria")

    # 1) caminho preferido: marg_val + fat
    c_fat = _pick(col_fat, "Fat_Ref", "Fat Ref", "fat", "FAT", "Faturamento")
    c_marg_val = _pick(col_marg_val, "Marg_Val_Ref", "Marg_Val Ref", "marg_val", "Margem_Val", "Margem R$")

    if c_fat and c_marg_val:
        fat = pd.to_numeric(df[c_fat], errors="coerce").fillna(0.0)
        marg_bruta = pd.to_numeric(df[c_marg_val], errors="coerce").fillna(0.0)

        if c_area:
            areas = df[c_area].astype(str).fillna("")
            td = areas.map(lambda a: _taxa_deducao(a))
        else:
            td = 0.0

        marg_real = marg_bruta - (fat * td)
        return float(marg_real.sum())

    # 2) fallback: preco/custo/qtd
    c_preco = _pick(col_preco, "Preco_Mais_Recente", "Preço Atual", "Preco", "Preco_Atual")
    c_custo = _pick(col_custo, "Custo_Mais_Recente", "Custo", "Custo_Atual")
    c_qtd = _pick(col_qtd, "Qtd_Ref", "Qtd Ref", "Qtd", "Quantidade", "Qtd_Media_Mensal")

    if not c_preco or not c_custo:
        return 0.0

    preco = pd.to_numeric(df[c_preco], errors="coerce").fillna(0.0)
    custo = pd.to_numeric(df[c_custo], errors="coerce").fillna(0.0)
    qtd = pd.to_numeric(df[c_qtd], errors="coerce").fillna(1.0) if c_qtd else 1.0

    if c_area:
        areas = df[c_area].astype(str).fillna("")
        td = areas.map(lambda a: _taxa_deducao(a))
    else:
        td = 0.0

    marg_unit = (preco * (1.0 - td)) - custo
    marg_total = (marg_unit * qtd).sum()
    return float(marg_total)


def calcular_margem_pond_percentual(
    df: "pd.DataFrame",
    *,
    col_fat: str = "Fat_Ref",
    **kwargs,
) -> float:
    """
    Margem % ponderada:
      (margem_total_R$ / faturamento_total_R$)

    kwargs vai para calcular_margem_real_valor_total.
    """
    if df is None or df.empty:
        return 0.0

    col_fat2 = col_fat if col_fat in df.columns else ("Fat Ref" if "Fat Ref" in df.columns else None)
    if not col_fat2:
        return 0.0

    fat_total = pd.to_numeric(df[col_fat2], errors="coerce").fillna(0.0).sum()
    if fat_total <= 0:
        return 0.0

    marg_total = calcular_margem_real_valor_total(df, col_fat=col_fat2, **kwargs)
    return float(marg_total / fat_total)