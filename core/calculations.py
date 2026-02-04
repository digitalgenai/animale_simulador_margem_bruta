from __future__ import annotations

import unicodedata
from typing import Optional

import numpy as np

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
    Regra: aplica taxa extra (2.08%) para itens cuja Area remeta a Estética e/ou Saúde.

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

    Regra:
    - Padrão: TAXA_DEDUCAO_FATURAMENTO (ex: 22,03%)
    - Se Area for Estética e/ou Saúde: TAXA_ESTETICA_SAUDE (2,08%) -> substitui a padrão
    - taxa_extra (opcional) é somada ao resultado final
    """
    extra = float(taxa_extra or 0.0)

    if area is not None and is_estetica_saude(area):
        base = TAXA_ESTETICA_SAUDE
    else:
        base = TAXA_DEDUCAO_FATURAMENTO

    return base + extra


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
