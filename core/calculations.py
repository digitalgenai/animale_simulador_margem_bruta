from __future__ import annotations

import numpy as np

from core.config import TAXA_DEDUCAO_FATURAMENTO, col_conc_1, col_conc_2


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


def calcular_custo_necessario(preco_alvo: float, margem_alvo: float) -> float:
    if preco_alvo <= 0:
        return 0.0
    try:
        return (preco_alvo * (1 - TAXA_DEDUCAO_FATURAMENTO)) - (preco_alvo * margem_alvo)
    except Exception:
        return 0.0


def calcular_margem_real_percentual(custo: float, preco: float) -> float:
    if preco <= 0:
        return 0.0
    try:
        return ((preco * (1 - TAXA_DEDUCAO_FATURAMENTO)) - custo) / preco
    except Exception:
        return 0.0


def calcular_margem_real_valor(custo: float, preco: float) -> float:
    if preco <= 0:
        return 0.0
    try:
        return (preco * (1 - TAXA_DEDUCAO_FATURAMENTO)) - custo
    except Exception:
        return 0.0
