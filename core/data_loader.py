from __future__ import annotations

import logging
import re
from typing import Dict, Tuple, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.db import get_engine
from core.config import (
    TEXT_COLS,
    BASE_NUM_COLS,
    AUX_NUM_COLS,
    LISTA_MESES_ANO,
    MESES_6M,  # mantido por compat (mesmo não usando diretamente aqui)
    MESES_3M,  # mantido por compat (mesmo não usando diretamente aqui)
    SIM_COLS_DEFAULTS,
    col_conc_1,
    col_conc_2,
    COLUNA_AGREGACAO_PRINCIPAL,
    PGSCHEMA_DEFAULT,
    PGTABLE_DEFAULT,
    N_MESES_JANELA,
)

logger = logging.getLogger(__name__)

# ======================================================================================
# Month context (exportável pra plots/callbacks)
# ======================================================================================

_MONTH_CTX: Dict[str, object] = {
    "start_month": None,
    "ref_month": None,                 # mês/ano selecionado no UI (primeiro dia)
    "closed_month": None,              # mês anterior (fechado) = ref_month - 1 mês
    "ref_month_safe": None,            # "YYYY_MM" do ref_month
    "closed_month_safe": None,         # "YYYY_MM" do closed_month
    "n": None,
    "months_ts": None,                 # List[pd.Timestamp]
    "labels_safe": None,               # List[str] ex: "2026_01" (sem hífen)
    "labels_legacy": None,             # List[str] ex: "Mar", "Abr", ... (alinhado)
    "labels_in_use": None,             # List[str]
    "legacy_to_safe": None,            # Dict[str,str]
    "safe_to_legacy": None,            # Dict[str,str]

    # Para o seletor Mês/Ano (range real do dataset)
    "available_months_ts": None,       # List[pd.Timestamp] (meses existentes no dataset)
    "available_labels_safe": None,     # List[str] "YYYY_MM"
    "available_labels_human": None,    # List[str] "Jan/2026"
}

_PT_ABBR = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
_PT_ABBR_SET = {m.lower() for m in _PT_ABBR}


def get_month_context() -> Dict[str, object]:
    """
    Retorna o contexto de meses calculado no último load_base_data().

    Use isso em plots/callbacks no lugar de depender cegamente de LISTA_MESES_ANO/MESES_3M/MESES_6M do config.
    """
    return dict(_MONTH_CTX)


# ======================================================================================
# Helpers
# ======================================================================================

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


def _build_months_ts(start_month: pd.Timestamp, n: int) -> List[pd.Timestamp]:
    out: List[pd.Timestamp] = []
    for i in range(n):
        out.append((start_month + pd.DateOffset(months=i)).normalize())
    return out


def _labels_safe_from_ts(months_ts: List[pd.Timestamp]) -> List[str]:
    # SAFE pra JS/AGGrid: só underscore
    return [m.strftime("%Y_%m") for m in months_ts]


def _looks_like_iso_label(s: str) -> bool:
    # "2026-01" ou "2026_01"
    return bool(re.match(r"^\d{4}[-_]\d{2}$", str(s).strip()))


def _normalize_case_like(src: str, target: str) -> str:
    # tenta manter "MAR" vs "Mar" etc.
    if src.isupper():
        return target.upper()
    if src.islower():
        return target.lower()
    return target  # Title-case default


def _rotate_legacy_labels_to_start(
    legacy_labels: List[str],
    start_month: pd.Timestamp,
) -> Optional[List[str]]:
    """
    Se LISTA_MESES_ANO for algo tipo ["Jan","Fev",...], rotaciona pra começar no mês do start_month.
    Se não der pra inferir, retorna None.
    """
    if not legacy_labels:
        return None

    start_abbr = _PT_ABBR[start_month.month - 1]

    idx = None
    for i, lab in enumerate(legacy_labels):
        if str(lab).strip().lower() == start_abbr.lower():
            idx = i
            break

    if idx is None:
        return None

    rot = legacy_labels[idx:] + legacy_labels[:idx]

    # garante casing coerente para labels que parecem meses PT
    rot0 = str(rot[0])
    out: List[str] = []
    for j, x in enumerate(rot):
        sx = str(x).strip()
        if sx.lower() in _PT_ABBR_SET:
            out.append(_normalize_case_like(rot0, _PT_ABBR[(start_month.month - 1 + j) % 12]))
        else:
            out.append(sx)
    return out


def _build_labels_legacy(start_month: pd.Timestamp, n: int) -> List[str]:
    """
    Labels "legado" compatíveis com o resto do app:
    - Se LISTA_MESES_ANO existir e for usável: alinha/rotaciona pra janela real.
    - Caso contrário: gera algo estável (ex: "Jan_2026" ...) sem hífen.
    """
    # 1) tentar usar LISTA_MESES_ANO do config (sem quebrar janeiro)
    if isinstance(LISTA_MESES_ANO, (list, tuple)) and len(LISTA_MESES_ANO) >= n:
        base = [str(x).strip() for x in LISTA_MESES_ANO[:n]]

        # se for ISO, não compensa "rotacionar", deixa como está
        if all(_looks_like_iso_label(x) for x in base):
            # mas converte "-" -> "_" pra não matar JS
            return [x.replace("-", "_") for x in base]

        rot = _rotate_legacy_labels_to_start(base, start_month)
        if rot is not None and len(rot) >= n:
            return rot[:n]

    # 2) fallback: "Jan_2026", "Fev_2026", ... (sem hífen)
    months_ts = _build_months_ts(start_month, n)
    labs: List[str] = []
    for m in months_ts:
        abbr = _PT_ABBR[m.month - 1]
        labs.append(f"{abbr}_{m.year}")
    return labs


def _dedupe_labels(labels: List[str]) -> List[str]:
    """
    Evita colisão de labels (se acontecer), adicionando sufixo incremental.
    """
    seen: Dict[str, int] = {}
    out: List[str] = []
    for x in labels:
        k = str(x)
        if k not in seen:
            seen[k] = 0
            out.append(k)
        else:
            seen[k] += 1
            out.append(f"{k}_{seen[k]}")
    return out


def _reset_pivot_index_to_sku(df_pvt: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o pivot resetado tenha coluna SKU (robusto contra index sem nome).
    """
    out = df_pvt.reset_index()
    if "cod_produto" in out.columns:
        return out.rename(columns={"cod_produto": "SKU"})
    if "index" in out.columns:
        return out.rename(columns={"index": "SKU"})
    # fallback: assume primeira coluna é o índice
    return out.rename(columns={out.columns[0]: "SKU"})


def _human_month_label(ts: pd.Timestamp) -> str:
    return f"{_PT_ABBR[ts.month - 1]}/{ts.year}"


def _clamp_month(target: pd.Timestamp, available: List[pd.Timestamp]) -> pd.Timestamp:
    """
    Garante que o mês selecionado exista dentro do range disponível no dataset.
    Se estiver fora, "clampa" pro min/max existente.
    """
    if not available:
        return target
    mn = min(available)
    mx = max(available)
    if target < mn:
        return mn
    if target > mx:
        return mx
    return target


# ======================================================================================
# Main
# ======================================================================================

def load_base_data(
    engine: Optional[Engine] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    n_months: Optional[int] = None,
    ref_year: Optional[int] = None,
    ref_month: Optional[int] = None,
) -> Tuple[
    pd.DataFrame,
    Dict[str, float],
    Dict[str, float],
    Dict[str, float],
    List[str],
    List[str],
]:
    """
    ref_year/ref_month:
      - representa o Mês/Ano selecionado no UI (ex: 2026/01)
      - "closed_month" = mês anterior (ex: 2025/12) será usado como base para:
          Qtd_Nov, Fat_Nov, Marg_Val_Nov e derivados (Preço Atual, Custo, Margens...)
    """
    schema = schema or PGSCHEMA_DEFAULT
    table = table or PGTABLE_DEFAULT
    n = int(n_months or N_MESES_JANELA)
    if n < 2:
        n = 2  # precisamos de pelo menos 2 meses para "mês anterior"

    full = f"{schema}.{table}"
    logger.info("Carregando base do Postgres: %s", full)

    engine = engine or get_engine()

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

    df_raw["qtd_venda"] = pd.to_numeric(df_raw["qtd_venda"], errors="coerce").fillna(0.0)
    df_raw["total_item"] = pd.to_numeric(df_raw["total_item"], errors="coerce").fillna(0.0)
    df_raw["lucro_total"] = pd.to_numeric(df_raw["lucro_total"], errors="coerce").fillna(0.0)

    for c in ["cod_produto", "produto", "fornecedor", "fabricante", "area"]:
        df_raw[c] = df_raw[c].astype(str).fillna("").str.strip()

    # Coluna mes (início do mês) (para range real e também janela)
    df_raw["mes"] = df_raw["data_venda"].dt.to_period("M").dt.to_timestamp().dt.normalize()

    available_months_ts = sorted(df_raw["mes"].dropna().unique().tolist())
    available_labels_safe = [pd.Timestamp(m).strftime("%Y_%m") for m in available_months_ts]
    available_labels_human = [_human_month_label(pd.Timestamp(m)) for m in available_months_ts]

    if not available_months_ts:
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # ref_month_start: mês selecionado no UI (ou default = mês mais recente do dataset)
    if ref_year is not None and ref_month is not None:
        try:
            ref_month_start = pd.Timestamp(int(ref_year), int(ref_month), 1).normalize()
        except Exception:
            ref_month_start = max(available_months_ts)
    else:
        ref_month_start = max(available_months_ts)

    # garante dentro do range real do dataset
    ref_month_start = _clamp_month(ref_month_start, available_months_ts)

    closed_month_start = (ref_month_start - pd.DateOffset(months=1)).normalize()

    # Janela: termina no ref_month_start (UI), mas as métricas "Nov" usam closed_month_start
    start_month = (ref_month_start - pd.DateOffset(months=n - 1)).normalize()

    # meses reais (janela)
    months_ts = _build_months_ts(start_month, n)

    # labels SAFE (sem hífen) -> não quebra JS/AGGrid expression
    labels_safe = _labels_safe_from_ts(months_ts)

    # labels LEGACY alinhados (compatibilidade com plots/callbacks antigos)
    labels_legacy = _build_labels_legacy(start_month, n)
    labels_legacy = _dedupe_labels(labels_legacy)

    # Mapas
    safe_to_legacy = dict(zip(labels_safe, labels_legacy))
    legacy_to_safe = dict(zip(labels_legacy, labels_safe))

    # Atualiza contexto global (pra seletor/plots/callbacks)
    _MONTH_CTX.update(
        {
            "start_month": start_month,
            "ref_month": ref_month_start,
            "closed_month": closed_month_start,
            "ref_month_safe": ref_month_start.strftime("%Y_%m"),
            "closed_month_safe": closed_month_start.strftime("%Y_%m"),
            "n": n,
            "months_ts": months_ts,
            "labels_safe": labels_safe,
            "labels_legacy": labels_legacy,
            "labels_in_use": labels_legacy,
            "legacy_to_safe": legacy_to_safe,
            "safe_to_legacy": safe_to_legacy,
            "available_months_ts": available_months_ts,
            "available_labels_safe": available_labels_safe,
            "available_labels_human": available_labels_human,
        }
    )

    logger.info(
        "Janela: start=%s ref(UI)=%s closed=%s (n=%d)",
        start_month.date(),
        ref_month_start.date(),
        closed_month_start.date(),
        n,
    )

    # Índice de mês relativo ao start da janela
    df_raw["mes_idx"] = _month_index(df_raw["mes"], start_month)

    # Filtra janela
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

    # Label SAFE por posição
    labels_safe_arr = np.array(labels_safe, dtype=object)
    df_win["mes_label_safe"] = labels_safe_arr[df_win["mes_idx"].astype(int).to_numpy()]

    # Agrega por SKU x mês (SAFE)
    grp = (
        df_win.groupby(["cod_produto", "mes_label_safe"], as_index=False)
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

    # Pivots (SAFE)
    fat_pvt = grp.pivot_table(index="cod_produto", columns="mes_label_safe", values="Fat", aggfunc="sum").fillna(0.0)
    marg_pvt = grp.pivot_table(index="cod_produto", columns="mes_label_safe", values="Marg_Val", aggfunc="sum").fillna(0.0)
    qtd_pvt = grp.pivot_table(index="cod_produto", columns="mes_label_safe", values="Qtd", aggfunc="sum").fillna(0.0)

    # garante todas as colunas SAFE na ordem da janela real
    for m in labels_safe:
        if m not in fat_pvt.columns:
            fat_pvt[m] = 0.0
        if m not in marg_pvt.columns:
            marg_pvt[m] = 0.0
        if m not in qtd_pvt.columns:
            qtd_pvt[m] = 0.0

    fat_pvt = fat_pvt[labels_safe]
    marg_pvt = marg_pvt[labels_safe]
    qtd_pvt = qtd_pvt[labels_safe]

    # Renomeia colunas SAFE -> Fat_YYYY_MM etc
    fat_pvt.columns = [f"Fat_{c}" for c in fat_pvt.columns]
    marg_pvt.columns = [f"Marg_Val_{c}" for c in marg_pvt.columns]
    qtd_pvt.columns = [f"Qtd_{c}" for c in qtd_pvt.columns]

    # Junta dimensões + pivots (robusto com index name)
    df_base = (
        base_dim
        .merge(_reset_pivot_index_to_sku(fat_pvt), on="SKU", how="left")
        .merge(_reset_pivot_index_to_sku(marg_pvt), on="SKU", how="left")
        .merge(_reset_pivot_index_to_sku(qtd_pvt), on="SKU", how="left")
    )

    # ----------------------------------------------------------------------------------
    # DUPLICA COLUNAS PRA LEGACY (compat com plots/callbacks antigos)
    # Ex: Fat_2026_01 -> Fat_Jan (ou o que LISTA_MESES_ANO alinhado gerou)
    # ----------------------------------------------------------------------------------
    for safe, legacy in safe_to_legacy.items():
        fat_s = f"Fat_{safe}"
        marg_s = f"Marg_Val_{safe}"
        qtd_s = f"Qtd_{safe}"

        fat_l = f"Fat_{legacy}"
        marg_l = f"Marg_Val_{legacy}"
        qtd_l = f"Qtd_{legacy}"

        if fat_s in df_base.columns and fat_l not in df_base.columns:
            df_base[fat_l] = df_base[fat_s]
        if marg_s in df_base.columns and marg_l not in df_base.columns:
            df_base[marg_l] = df_base[marg_s]
        if qtd_s in df_base.columns and qtd_l not in df_base.columns:
            df_base[qtd_l] = df_base[qtd_s]

    # -------------------------
    # Pipeline compatível Excel
    # -------------------------

    # Normaliza textos (e aplica defaults úteis pro teu filtro)
    for col in TEXT_COLS:
        if col in df_base.columns:
            df_base[col] = df_base[col].astype(str).str.strip().replace(["nan", "NaN", ""], "SEM_INFO")
            if col == "Cod_Barras":
                df_base[col] = df_base[col].str.replace(r"\.0$", "", regex=True)
        else:
            df_base[col] = "-"

    # Alguns campos que teu app tipicamente filtra:
    for txt in ["Fornecedor", "Fabricante", "Area", "Produto"]:
        if txt in df_base.columns:
            df_base[txt] = df_base[txt].astype(str).str.strip().replace(["", "nan", "NaN"], "SEM_INFO")

    # Se o filtro do teu app usa exatamente esse texto:
    if "Fornecedor" in df_base.columns:
        df_base["Fornecedor"] = df_base["Fornecedor"].replace({"SEM_INFO": "SEM FORNECEDOR CADASTRADO"})

    # Numéricos: BASE_NUM_COLS + meses (SAFE e LEGACY)
    cols_num = list(BASE_NUM_COLS)

    # SAFE
    for m in labels_safe:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}", f"Qtd_{m}"])
    # LEGACY
    for m in labels_legacy:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}", f"Qtd_{m}"])

    cols_num = list(dict.fromkeys(cols_num))  # dedupe preservando ordem

    _ensure_columns(df_base, cols_num, 0.0)
    for col in cols_num:
        df_base[col] = pd.to_numeric(df_base[col], errors="coerce").fillna(0.0)

    # Aux e concorrentes
    _ensure_columns(df_base, AUX_NUM_COLS, 0.0)
    if col_conc_1 not in df_base.columns:
        df_base[col_conc_1] = 0.0
    if col_conc_2 not in df_base.columns:
        df_base[col_conc_2] = 0.0

    # =========================================================
    # TRIMESTRE / 6M / ANO: usa LEGACY por padrão (compat)
    # (mantido como estava)
    # =========================================================
    labels_3m = labels_legacy[-3:]
    labels_6m = labels_legacy[-6:]

    cols_fat_3m = [f"Fat_{m}" for m in labels_3m]
    cols_marg_3m = [f"Marg_Val_{m}" for m in labels_3m]
    cols_fat_6m = [f"Fat_{m}" for m in labels_6m]
    cols_marg_6m = [f"Marg_Val_{m}" for m in labels_6m]
    cols_fat_ano = [f"Fat_{m}" for m in labels_legacy]
    cols_marg_ano = [f"Marg_Val_{m}" for m in labels_legacy]

    _ensure_columns(df_base, cols_fat_3m, 0.0)
    _ensure_columns(df_base, cols_marg_3m, 0.0)
    _ensure_columns(df_base, cols_fat_6m, 0.0)
    _ensure_columns(df_base, cols_marg_6m, 0.0)
    _ensure_columns(df_base, cols_fat_ano, 0.0)
    _ensure_columns(df_base, cols_marg_ano, 0.0)

    df_base["Fat_Total_Trimestre"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Valor_Margem_Total_Trimestre"] = df_base[cols_marg_3m].sum(axis=1)
    df_base["Margem_Media_Trimestre"] = _safe_div(df_base["Valor_Margem_Total_Trimestre"], df_base["Fat_Total_Trimestre"])

    # Curva ABC
    df_base["Curva_ABC"] = _calc_curva_abc(df_base, "Fat_Total_Trimestre")

    # Aliases que teu grid/plots tipicamente usam
    df_base["ABC"] = df_base["Curva_ABC"]
    df_base["Categ"] = df_base["Area"]

    # =========================================
    # MÊS-REF para "Nov": mês ANTERIOR ao selecionado (ref_month - 1)
    # =========================================
    # encontra o SAFE correspondente ao closed_month dentro da janela
    idx_closed = (closed_month_start.year - start_month.year) * 12 + (closed_month_start.month - start_month.month)

    if 0 <= idx_closed < len(labels_safe):
        mes_ref_safe = labels_safe[idx_closed]
    else:
        # fallback: último mês da janela
        mes_ref_safe = labels_safe[-1]

    fat_ref = f"Fat_{mes_ref_safe}"
    marg_ref = f"Marg_Val_{mes_ref_safe}"
    qtd_ref = f"Qtd_{mes_ref_safe}"

    mes_ref_label = safe_to_legacy.get(mes_ref_safe, mes_ref_safe)
    logger.info("mes_ref_safe(NOV)= %s | label=%s", mes_ref_safe, mes_ref_label)

    missing = [c for c in (fat_ref, marg_ref, qtd_ref) if c not in df_base.columns]
    if missing:
        raise RuntimeError(f"Colunas do mês-ref (NOV) ausentes: {missing}. mes_ref_safe={mes_ref_safe}")

    # =========================================
    # Derivadas do mês ref (NOV = mês fechado)
    # =========================================
    df_base["Qtd_Nov"] = df_base[qtd_ref].fillna(0.0).round().astype(int)
    df_base["Preco_Atual"] = _safe_div(df_base[fat_ref], df_base[qtd_ref])
    df_base["Custo"] = _safe_div(df_base[fat_ref] - df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Unit"] = _safe_div(df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Perc"] = _safe_div(df_base[marg_ref], df_base[fat_ref])

    # Aliases grid
    df_base["Qtd Nov"] = df_base["Qtd_Nov"]
    df_base["Preço Atual"] = df_base["Preco_Atual"]
    df_base["Marg R$"] = df_base["Marg_Unit"]
    df_base["Marg %"] = df_base["Marg_Perc"]
    df_base["Marg Atual %"] = df_base["Marg_Perc"]

    # =========================================================
    # CONTRATO com o front (view_builders)
    # =========================================================
    df_base["Preco_Mais_Recente"] = df_base["Preco_Atual"]
    df_base["Custo_Mais_Recente"] = df_base["Custo"]

    df_base["Qtd_Media_Mensal"] = df_base["Qtd_Nov"]

    # Tab3 do view_builders usa Fat_Nov / Marg_Val_Nov
    df_base["Fat_Nov"] = df_base[fat_ref]
    df_base["Marg_Val_Nov"] = df_base[marg_ref]

    # =========================================================
    # HISTÓRICOS (6M / 3M / Pico) - usando LEGACY (compat)
    # =========================================================
    qtd_cols_ano = [f"Qtd_{m}" for m in labels_legacy]
    qtd_cols_6m = [f"Qtd_{m}" for m in labels_legacy[-6:]]
    qtd_cols_3m = [f"Qtd_{m}" for m in labels_legacy[-3:]]

    _ensure_columns(df_base, qtd_cols_ano, 0.0)
    _ensure_columns(df_base, qtd_cols_6m, 0.0)
    _ensure_columns(df_base, qtd_cols_3m, 0.0)

    df_base["Hist_Qtd_Media_6M"] = df_base[qtd_cols_6m].mean(axis=1)
    df_base["Hist_Qtd_Media_3M"] = df_base[qtd_cols_3m].mean(axis=1)

    df_base["Hist_Qtd_Pico"] = df_base[qtd_cols_ano].max(axis=1)
    pico_col = df_base[qtd_cols_ano].idxmax(axis=1).astype(str)
    df_base["Hist_Mes_Pico"] = pico_col.str.replace("Qtd_", "", regex=False)

    # Benchmarks globais (mantido)
    logger.info("Calculando Benchmarks Globais...")

    df_base["Temp_Fat_Ano"] = df_base[cols_fat_ano].sum(axis=1)
    df_base["Temp_Marg_Ano"] = df_base[cols_marg_ano].sum(axis=1)
    df_base["Temp_Fat_6M"] = df_base[cols_fat_6m].sum(axis=1)
    df_base["Temp_Marg_6M"] = df_base[cols_marg_6m].sum(axis=1)
    df_base["Temp_Fat_3M"] = df_base[cols_fat_3m].sum(axis=1)
    df_base["Temp_Marg_3M"] = df_base[cols_marg_3m].sum(axis=1)

    df_bench = df_base.groupby("Area")[
        ["Temp_Fat_Ano", "Temp_Marg_Ano", "Temp_Fat_6M", "Temp_Marg_6M", "Temp_Fat_3M", "Temp_Marg_3M"]
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

    logger.info("Base carregada do Postgres: %d linhas", len(df_base))
    return df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global
