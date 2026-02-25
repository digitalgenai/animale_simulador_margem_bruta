from __future__ import annotations

import logging
import re
import unicodedata
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
    SIM_COLS_DEFAULTS,
    col_conc_1,
    col_conc_2,
    COLUNA_AGREGACAO_PRINCIPAL,
    PGSCHEMA_DEFAULT,
    PGTABLE_DEFAULT,
    N_MESES_JANELA,
    # coleta (concorrentes)
    PGSCHEMA_COLETA_DEFAULT,
    COLETA_TABLE_MISSAO_DEFAULT,
    COLETA_TABLE_MISSAO_PRODUTO_DEFAULT,
    COLETA_TABLE_CONCORRENTE_DEFAULT,
    COLETA_TABLE_PRODUTO_DEFAULT,
)

logger = logging.getLogger(__name__)

# ======================================================================================
# Month context (exportável pra plots/callbacks)
# ======================================================================================

_MONTH_CTX: Dict[str, object] = {
    "start_month": None,
    "ref_month": None,  # mês/ano selecionado no UI (primeiro dia)
    "closed_month": None,  # mantido por compat, mas aqui = ref_month (mês selecionado)
    "ref_month_safe": None,  # "YYYY_MM" do ref_month
    "closed_month_safe": None,  # mantido por compat, mas aqui = ref_month_safe
    "n": None,
    "months_ts": None,  # List[pd.Timestamp]
    "labels_safe": None,  # List[str] ex: "2026_01" (sem hífen)
    "labels_legacy": None,  # List[str] ex: "Mar", "Abr", ... (alinhado)
    "labels_in_use": None,  # List[str]
    "legacy_to_safe": None,  # Dict[str,str]
    "safe_to_legacy": None,  # Dict[str,str]

    # Para o seletor Mês/Ano (range real do dataset)
    "available_months_ts": None,  # List[pd.Timestamp] (meses existentes no dataset)
    "available_labels_safe": None,  # List[str] "YYYY_MM"
    "available_labels_human": None,  # List[str] "Jan/2026"
}

_PT_ABBR = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
_PT_ABBR_SET = {m.lower() for m in _PT_ABBR}

# Cache simples dos meses disponíveis (pra não ficar fazendo DISTINCT sempre)
_AVAILABLE_MONTHS_CACHE: Dict[str, List[pd.Timestamp]] = {}

# Cache de metadados do schema coleta (tabelas/colunas)
_COLETA_TABLES_CACHE: Dict[str, Dict[str, str]] = {}  # schema -> {lower_name: actual_name}
_COLETA_COLS_CACHE: Dict[str, Dict[str, str]] = {}  # "schema.table" -> {lower_col: actual_col}


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


def _fetch_available_months(engine: Engine, full_table: str) -> List[pd.Timestamp]:
    """
    Busca meses existentes no dataset (DISTINCT date_trunc('month', data_venda)).
    Cacheia por tabela no processo.
    """
    if full_table in _AVAILABLE_MONTHS_CACHE and _AVAILABLE_MONTHS_CACHE[full_table]:
        return _AVAILABLE_MONTHS_CACHE[full_table]

    sql = text(
        f"""
        SELECT DISTINCT date_trunc('month', data_venda)::date AS mes
        FROM {full_table}
        WHERE data_venda IS NOT NULL
        ORDER BY mes
        """
    )
    dfm = pd.read_sql(sql, engine)
    if dfm.empty or "mes" not in dfm.columns:
        _AVAILABLE_MONTHS_CACHE[full_table] = []
        return []

    months = pd.to_datetime(dfm["mes"], errors="coerce").dropna().dt.to_period("M").dt.to_timestamp().dt.normalize()
    out = sorted(months.unique().tolist())
    _AVAILABLE_MONTHS_CACHE[full_table] = out
    return out


# ======================================================================================
# Concorrência (schema coleta)
# ======================================================================================

def _norm_txt(s: str) -> str:
    if s is None:
        return ""
    s2 = str(s).strip().lower()
    s2 = unicodedata.normalize("NFKD", s2)
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    return s2


def _quote_ident(name: str) -> str:
    s = str(name)
    # identificadores simples (sem quote) -> mais rápido/limpo
    if re.match(r"^[a-z_][a-z0-9_]*$", s):
        return s
    return '"' + s.replace('"', '""') + '"'


def _qual(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _get_coleta_tables_lower(engine: Engine, schema: str) -> Dict[str, str]:
    if schema in _COLETA_TABLES_CACHE:
        return _COLETA_TABLES_CACHE[schema]

    sql = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        """
    )
    df = pd.read_sql(sql, engine, params={"schema": schema})
    mp: Dict[str, str] = {}
    if not df.empty and "table_name" in df.columns:
        for t in df["table_name"].astype(str).tolist():
            mp[t.lower()] = t
    _COLETA_TABLES_CACHE[schema] = mp
    return mp


def _resolve_table(engine: Engine, schema: str, candidates: List[str]) -> Optional[str]:
    tables = _get_coleta_tables_lower(engine, schema)
    if not tables:
        return None
    for c in candidates:
        if not c:
            continue
        key = str(c).strip().lower()
        if key in tables:
            return tables[key]
    return None


def _get_coleta_cols_lower(engine: Engine, schema: str, table: str) -> Dict[str, str]:
    key = f"{schema}.{table}"
    if key in _COLETA_COLS_CACHE:
        return _COLETA_COLS_CACHE[key]

    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        """
    )
    df = pd.read_sql(sql, engine, params={"schema": schema, "table": table})
    mp: Dict[str, str] = {}
    if not df.empty and "column_name" in df.columns:
        for c in df["column_name"].astype(str).tolist():
            mp[c.lower()] = c
    _COLETA_COLS_CACHE[key] = mp
    return mp


def _pick_col(cols_lower: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if not c:
            continue
        key = str(c).strip().lower()
        if key in cols_lower:
            return cols_lower[key]
    return None


def _parse_price_series(s: pd.Series) -> pd.Series:
    """
    Converte preços vindos do coleta (às vezes texto com vírgula/mascara) -> float.
    """
    if s is None:
        return pd.Series(dtype=float)
    out = s.astype(str).str.strip()
    out = out.str.replace(",", ".", regex=False)
    out = out.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(out, errors="coerce").fillna(0.0)


def _map_concorrente_to_target_col(nome_conc: str) -> Optional[str]:
    """
    Mapeia concorrente -> coluna do simulador (col_conc_1 / col_conc_2).
    Heurística (robusta): contém 'petz' ou 'procampo' no nome normalizado.
    """
    n = _norm_txt(nome_conc or "")
    if not n:
        return None
    if "petz" in n:
        return col_conc_1
    if "procampo" in n:
        return col_conc_2
    return None


def _norm_barcode_series(s: pd.Series) -> pd.Series:
    """
    Normaliza barcode para "só dígitos" e remove sufixo ".0" (quando veio como float).
    """
    if s is None:
        return pd.Series(dtype=str)
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    out = out.str.replace(r"[^\d]+", "", regex=True)
    return out


def _load_competitor_prices(engine: Engine, ref_month_start: Optional[pd.Timestamp]) -> pd.DataFrame:
    """
    Busca preços de concorrentes no schema coleta e devolve em formato wide por COD_BARRAS:

        ["Cod_Barras", col_conc_1, col_conc_2]

    Regra:
      - Busca em coleta.missoes + coleta.missao_produtos + coleta.concorrentes
      - Usa SEMPRE mp.codigo (barcode) como chave (alinhado ao seu import_preco_concorrentes.py)
      - Preço: COALESCE(NULLIF(promo,0), NULLIF(base,0))
      - Primeiro tenta filtrar pelo mês selecionado (date_trunc('month', criadaEm) == ref_month)
        e se vier vazio, cai pro mais recente geral.
    """
    schema = PGSCHEMA_COLETA_DEFAULT

    try:
        # Resolve tabelas (candidates + env override)
        missao_tbl = _resolve_table(
            engine,
            schema,
            [COLETA_TABLE_MISSAO_DEFAULT, "missoes", "missao"],
        )
        mp_tbl = _resolve_table(
            engine,
            schema,
            [COLETA_TABLE_MISSAO_PRODUTO_DEFAULT, "missao_produtos", "missao_produto", "missaoprodutos", "missaoproduto"],
        )
        conc_tbl = _resolve_table(
            engine,
            schema,
            [COLETA_TABLE_CONCORRENTE_DEFAULT, "concorrentes", "concorrente"],
        )
        # prod_tbl não é necessário pra chave (usaremos mp.codigo), mas deixo resolver por compat/log
        prod_tbl = _resolve_table(
            engine,
            schema,
            [COLETA_TABLE_PRODUTO_DEFAULT, "produtos", "produto"],
        )

        logger.info(
            "coleta tables resolved: missao=%s mp=%s conc=%s prod=%s",
            missao_tbl,
            mp_tbl,
            conc_tbl,
            prod_tbl,
        )

        if not missao_tbl or not mp_tbl or not conc_tbl:
            logger.warning(
                "Concorrência (coleta) indisponível: tabelas essenciais não encontradas. missao=%s mp=%s conc=%s",
                missao_tbl,
                mp_tbl,
                conc_tbl,
            )
            return pd.DataFrame()

        m_cols = _get_coleta_cols_lower(engine, schema, missao_tbl)
        mp_cols = _get_coleta_cols_lower(engine, schema, mp_tbl)
        c_cols = _get_coleta_cols_lower(engine, schema, conc_tbl)

        # Colunas essenciais
        m_id = _pick_col(m_cols, ["id", "missao_id", "missaoid"])
        m_conc_id = _pick_col(m_cols, ["concorrenteId", "concorrente_id", "id_concorrente", "concorrenteid"])

        c_id = _pick_col(c_cols, ["id", "concorrente_id", "concorrenteid"])
        c_nome = _pick_col(c_cols, ["nome", "name", "descricao", "razao_social"])

        mp_missao_id = _pick_col(mp_cols, ["missaoId", "missao_id", "id_missao", "missaoid"])
        mp_preco = _pick_col(mp_cols, ["precoConcorrente", "preco_concorrente", "precoconcorrente"])
        mp_preco_promo = _pick_col(
            mp_cols,
            ["precoConcorrentePromocao", "preco_concorrente_promocao", "precoconcorrentepromocao"],
        )
        mp_id = _pick_col(mp_cols, ["id", "missao_produto_id", "missaoproduto_id", "missaoprodutos_id"])

        # Chave: barcode em missao_produtos.codigo
        mp_barcode = _pick_col(mp_cols, ["codigo", "cod_barras", "ean", "gtin", "barcode", "sku", "cod_produto"])

        if not (m_id and m_conc_id and c_id and c_nome and mp_missao_id and mp_barcode and (mp_preco or mp_preco_promo)):
            logger.warning(
                "Concorrência (coleta): colunas essenciais ausentes. "
                "m_id=%s m_conc_id=%s c_id=%s c_nome=%s mp_missao_id=%s mp_barcode=%s mp_preco=%s mp_preco_promo=%s",
                m_id,
                m_conc_id,
                c_id,
                c_nome,
                mp_missao_id,
                mp_barcode,
                mp_preco,
                mp_preco_promo,
            )
            return pd.DataFrame()

        # Data em missoes (pra filtro por mês / mais recente)
        m_dt = _pick_col(
            m_cols,
            ["criadaEm", "data", "data_missao", "data_execucao", "data_coleta", "dt", "dt_execucao", "dt_coleta",
             "createdAt", "updatedAt", "created_at", "updated_at"],
        )

        m_alias = "m"
        c_alias = "c"
        mp_alias = "mp"

        missao_q = _qual(schema, missao_tbl)
        conc_q = _qual(schema, conc_tbl)
        mp_q = _qual(schema, mp_tbl)

        barcode_expr = f"{mp_alias}.{_quote_ident(mp_barcode)}::text"
        conc_expr = f"{c_alias}.{_quote_ident(c_nome)}"

        # Preço: prioriza promo quando > 0, senão base.
        if mp_preco_promo and mp_preco:
            promo_expr = f"{mp_alias}.{_quote_ident(mp_preco_promo)}"
            base_expr = f"{mp_alias}.{_quote_ident(mp_preco)}"
            preco_expr = f"COALESCE(NULLIF({promo_expr}, 0), NULLIF({base_expr}, 0))"
        elif mp_preco_promo:
            promo_expr = f"{mp_alias}.{_quote_ident(mp_preco_promo)}"
            preco_expr = f"NULLIF({promo_expr}, 0)"
        else:
            base_expr = f"{mp_alias}.{_quote_ident(mp_preco)}"
            preco_expr = f"NULLIF({base_expr}, 0)"

        dt_expr = f"{m_alias}.{_quote_ident(m_dt)}" if m_dt else "NULL"

        # ORDER BY p/ DISTINCT ON (mais recente)
        if m_dt:
            order_tail = f"{dt_expr} DESC NULLS LAST"
            if mp_id:
                order_tail += f", {mp_alias}.{_quote_ident(mp_id)} DESC"
        else:
            order_tail = f"{mp_alias}.{_quote_ident(mp_id)} DESC" if mp_id else "1"

        def _query(use_month_filter: bool) -> pd.DataFrame:
            where_parts = [f"{preco_expr} IS NOT NULL", f"{barcode_expr} IS NOT NULL", f"trim({barcode_expr}) <> ''"]

            params = {}
            if use_month_filter and m_dt and isinstance(ref_month_start, pd.Timestamp):
                where_parts.append(f"date_trunc('month', {dt_expr})::date = :ref_month")
                params["ref_month"] = pd.Timestamp(ref_month_start).normalize().date()

            where_sql = " AND ".join(where_parts)

            sqlq = text(
                f"""
                SELECT DISTINCT ON ({barcode_expr}, {conc_expr})
                    {barcode_expr} AS "Cod_Barras",
                    {conc_expr}    AS "Concorrente",
                    {preco_expr}   AS "Preco",
                    {dt_expr}      AS "Dt"
                FROM {missao_q} {m_alias}
                JOIN {conc_q} {c_alias}
                    ON {c_alias}.{_quote_ident(c_id)} = {m_alias}.{_quote_ident(m_conc_id)}
                JOIN {mp_q} {mp_alias}
                    ON {mp_alias}.{_quote_ident(mp_missao_id)} = {m_alias}.{_quote_ident(m_id)}
                WHERE {where_sql}
                ORDER BY {barcode_expr}, {conc_expr}, {order_tail}
                """
            )
            try:
                return pd.read_sql(sqlq, engine, params=params)
            except Exception:
                logger.exception("Falha ao consultar concorrência em %s (use_month_filter=%s).", schema, use_month_filter)
                return pd.DataFrame()

        # 1) tenta filtrar pelo mês selecionado
        df_raw = _query(use_month_filter=True)

        # 2) fallback: mais recente geral
        if df_raw.empty:
            df_raw = _query(use_month_filter=False)

        logger.info("df_raw rows=%d cols=%s", len(df_raw), list(df_raw.columns))
        if not df_raw.empty:
            logger.info("concorrentes sample=%s", df_raw["Concorrente"].dropna().astype(str).head(10).tolist())
            logger.info("barcode sample=%s", df_raw["Cod_Barras"].dropna().astype(str).head(10).tolist())

        if df_raw.empty:
            return pd.DataFrame()

        # Normalização
        df_raw["Cod_Barras"] = _norm_barcode_series(df_raw["Cod_Barras"])
        df_raw["Concorrente"] = df_raw["Concorrente"].astype(str).str.strip()
        df_raw["Preco"] = _parse_price_series(df_raw["Preco"])

        # mapeia apenas concorrentes do simulador (PETZ / PROCAMPO)
        df_raw["__target_col"] = df_raw["Concorrente"].apply(_map_concorrente_to_target_col)
        df_raw = df_raw[df_raw["__target_col"].notna()].copy()
        if df_raw.empty:
            return pd.DataFrame()

        # mantém apenas preços > 0
        df_raw = df_raw[df_raw["Preco"] > 0].copy()
        if df_raw.empty:
            return pd.DataFrame()

        # wide (barcode)
        df_wide = (
            df_raw.pivot_table(
                index="Cod_Barras",
                columns="__target_col",
                values="Preco",
                aggfunc="max",
            )
            .reset_index()
        )

        if col_conc_1 not in df_wide.columns:
            df_wide[col_conc_1] = 0.0
        if col_conc_2 not in df_wide.columns:
            df_wide[col_conc_2] = 0.0

        df_wide[col_conc_1] = pd.to_numeric(df_wide[col_conc_1], errors="coerce").fillna(0.0)
        df_wide[col_conc_2] = pd.to_numeric(df_wide[col_conc_2], errors="coerce").fillna(0.0)

        logger.info("df_raw after map/filter rows=%d", len(df_raw))
        logger.info(
            "df_wide filled: petz=%d procampo=%d",
            int((df_wide[col_conc_1] > 0).sum()),
            int((df_wide[col_conc_2] > 0).sum()),
        )

        return df_wide[["Cod_Barras", col_conc_1, col_conc_2]]

    except Exception:
        logger.exception("Falha geral ao carregar concorrência do schema coleta.")
        return pd.DataFrame()


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
      - AGORA: métricas "Ref" (Qtd_Ref, Fat_Ref, Marg_Val_Ref etc.) usam o PRÓPRIO mês selecionado.
        (antes usava mês anterior "fechado", por isso Jan/2026 mostrava Dez/2025)

    Importante (pedido):
      - schema 'stage' => somente obt_faturamento (faturamento)
      - schema 'coleta' => concorrentes (preços) via missoes/concorrentes/missao_produtos
      - JOIN de concorrência: POR COD_BARRAS (df_base.Cod_Barras <-> coleta.missao_produtos.codigo)
    """
    schema = schema or PGSCHEMA_DEFAULT
    table = table or PGTABLE_DEFAULT
    n = int(n_months or N_MESES_JANELA)
    if n < 2:
        n = 2

    full = f"{schema}.{table}"
    logger.info("Carregando base do Postgres: %s", full)

    engine = engine or get_engine()

    # 1) meses disponíveis (rápido)
    available_months_ts = _fetch_available_months(engine, full)
    available_labels_safe = [pd.Timestamp(m).strftime("%Y_%m") for m in available_months_ts]
    available_labels_human = [_human_month_label(pd.Timestamp(m)) for m in available_months_ts]

    if not available_months_ts:
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # 2) ref_month_start do UI
    if ref_year is not None and ref_month is not None:
        try:
            ref_month_start = pd.Timestamp(int(ref_year), int(ref_month), 1).normalize()
        except Exception:
            ref_month_start = max(available_months_ts)
    else:
        ref_month_start = max(available_months_ts)

    # garante dentro do range real do dataset
    ref_month_start = _clamp_month(ref_month_start, available_months_ts)

    # 3) janela de meses (termina em ref_month_start)
    start_month = (ref_month_start - pd.DateOffset(months=n - 1)).normalize()
    end_month_excl = (ref_month_start + pd.DateOffset(months=1)).normalize()

    months_ts = _build_months_ts(start_month, n)
    labels_safe = _labels_safe_from_ts(months_ts)
    labels_legacy = _build_labels_legacy(start_month, n)
    labels_legacy = _dedupe_labels(labels_legacy)

    safe_to_legacy = dict(zip(labels_safe, labels_legacy))
    legacy_to_safe = dict(zip(labels_legacy, labels_safe))

    # Compat: mantemos chaves "closed_month" mas agora elas representam o mês selecionado
    _MONTH_CTX.update(
        {
            "start_month": start_month,
            "ref_month": ref_month_start,
            "closed_month": ref_month_start,
            "ref_month_safe": ref_month_start.strftime("%Y_%m"),
            "closed_month_safe": ref_month_start.strftime("%Y_%m"),
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
        "Janela: start=%s ref(UI)=%s end_excl=%s (n=%d)",
        start_month.date(),
        ref_month_start.date(),
        end_month_excl.date(),
        n,
    )

    # 4) Query AGREGADA (bem mais rápida): SKU x mês (stage.obt_faturamento)
    sql = text(
        f"""
        SELECT
            cod_produto,
            produto,
            cod_barras,
            fornecedor,
            fabricante,
            area,
            date_trunc('month', data_venda)::date AS mes,
            SUM(qtd_venda)   AS qtd,
            SUM(total_item)  AS fat,
            SUM(lucro_total) AS marg_val
        FROM {full}
        WHERE data_venda IS NOT NULL
        AND date_trunc('month', data_venda)::date >= :start_month
        AND date_trunc('month', data_venda)::date <= :ref_month
        GROUP BY
            cod_produto, produto, cod_barras, fornecedor, fabricante, area, mes
        """
    )

    df_win = pd.read_sql(
        sql,
        engine,
        params={"start_month": start_month, "ref_month": ref_month_start},
    )

    logger.info("df_win rows=%s", len(df_win))
    if not df_win.empty:
        logger.info("df_win mes(min/max)=%s..%s", df_win["mes"].min(), df_win["mes"].max())
    else:
        logger.warning("df_win veio vazio para start=%s end=%s (ref=%s)", start_month, end_month_excl, ref_month_start)

    if df_win.empty:
        logger.warning("Sem dados na janela SQL [%s..%s).", start_month.date(), end_month_excl.date())
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # Tipos / normalização (df_win já vem agregado)
    df_win["mes"] = pd.to_datetime(df_win["mes"], errors="coerce").dt.to_period("M").dt.to_timestamp().dt.normalize()
    df_win = df_win.dropna(subset=["mes"]).copy()

    for c in ["cod_produto", "produto", "cod_barras", "fornecedor", "fabricante", "area"]:
        df_win[c] = df_win[c].astype(str).fillna("").str.strip()

    df_win["qtd"] = pd.to_numeric(df_win["qtd"], errors="coerce").fillna(0.0)
    df_win["fat"] = pd.to_numeric(df_win["fat"], errors="coerce").fillna(0.0)
    df_win["marg_val"] = pd.to_numeric(df_win["marg_val"], errors="coerce").fillna(0.0)

    # Mes label SAFE por posição (mapeia timestamp -> safe)
    map_ts_to_safe = {pd.Timestamp(ts).normalize(): lab for ts, lab in zip(months_ts, labels_safe)}
    df_win["mes_label_safe"] = df_win["mes"].map(map_ts_to_safe).fillna("")

    df_win = df_win[df_win["mes_label_safe"] != ""].copy()
    if df_win.empty:
        df_base = pd.DataFrame()
        return df_base, {}, {}, {}, ["SEM DADOS"], []

    # Dimensões por SKU
    base_dim = (
        df_win.groupby("cod_produto", as_index=False)
        .agg(
            Produto=("produto", "first"),
            Cod_Barras=("cod_barras", "first"),
            Fornecedor=("fornecedor", "first"),
            Fabricante=("fabricante", "first"),
            Area=("area", "first"),
        )
        .rename(columns={"cod_produto": "SKU"})
    )

    # Pivots (SAFE)
    fat_pvt = df_win.pivot_table(index="cod_produto", columns="mes_label_safe", values="fat", aggfunc="sum").fillna(0.0)
    marg_pvt = df_win.pivot_table(index="cod_produto", columns="mes_label_safe", values="marg_val", aggfunc="sum").fillna(0.0)
    qtd_pvt = df_win.pivot_table(index="cod_produto", columns="mes_label_safe", values="qtd", aggfunc="sum").fillna(0.0)

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
    # DUPLICA COLUNAS PRA LEGACY (em lote, menos fragmentação)
    # ----------------------------------------------------------------------------------
    new_cols: Dict[str, pd.Series] = {}
    for safe, legacy in safe_to_legacy.items():
        fat_s = f"Fat_{safe}"
        marg_s = f"Marg_Val_{safe}"
        qtd_s = f"Qtd_{safe}"

        fat_l = f"Fat_{legacy}"
        marg_l = f"Marg_Val_{legacy}"
        qtd_l = f"Qtd_{legacy}"

        if fat_s in df_base.columns and fat_l not in df_base.columns:
            new_cols[fat_l] = df_base[fat_s]
        if marg_s in df_base.columns and marg_l not in df_base.columns:
            new_cols[marg_l] = df_base[marg_s]
        if qtd_s in df_base.columns and qtd_l not in df_base.columns:
            new_cols[qtd_l] = df_base[qtd_s]

    if new_cols:
        df_base = pd.concat([df_base, pd.DataFrame(new_cols)], axis=1)

    # -------------------------
    # Pipeline compatível Excel
    # -------------------------
    for col in TEXT_COLS:
        if col in df_base.columns:
            df_base[col] = df_base[col].astype(str).str.strip().replace(["nan", "NaN", ""], "SEM_INFO")
            if col == "Cod_Barras":
                df_base[col] = df_base[col].str.replace(r"\.0$", "", regex=True)
        else:
            df_base[col] = "-"

    for txt in ["Fornecedor", "Fabricante", "Area", "Produto"]:
        if txt in df_base.columns:
            df_base[txt] = df_base[txt].astype(str).str.strip().replace(["", "nan", "NaN"], "SEM_INFO")

    if "Fornecedor" in df_base.columns:
        df_base["Fornecedor"] = df_base["Fornecedor"].replace({"SEM_INFO": "SEM FORNECEDOR CADASTRADO"})

    # Normaliza barcode base (chave p/ merge de concorrência)
    if "Cod_Barras" in df_base.columns:
        df_base["__barcode"] = _norm_barcode_series(df_base["Cod_Barras"])
    else:
        df_base["__barcode"] = ""

    # Numéricos: BASE_NUM_COLS + meses (SAFE e LEGACY)
    cols_num = list(BASE_NUM_COLS)

    for m in labels_safe:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}", f"Qtd_{m}"])
    for m in labels_legacy:
        cols_num.extend([f"Fat_{m}", f"Marg_Val_{m}", f"Qtd_{m}"])

    cols_num = list(dict.fromkeys(cols_num))
    _ensure_columns(df_base, cols_num, 0.0)
    for col in cols_num:
        df_base[col] = pd.to_numeric(df_base[col], errors="coerce").fillna(0.0)

    _ensure_columns(df_base, AUX_NUM_COLS, 0.0)
    if col_conc_1 not in df_base.columns:
        df_base[col_conc_1] = 0.0
    if col_conc_2 not in df_base.columns:
        df_base[col_conc_2] = 0.0

    # =========================================================
    # CONCORRÊNCIA (coleta): preenche col_conc_1 / col_conc_2
    # JOIN: POR COD_BARRAS (df_base.__barcode <-> df_comp.__barcode)
    # =========================================================
    try:
        df_comp = _load_competitor_prices(engine, ref_month_start)
        if df_comp is not None and not df_comp.empty:
            df_comp = df_comp.copy()
            df_comp["__barcode"] = _norm_barcode_series(df_comp["Cod_Barras"])

            logger.info("df_comp rows=%d cols=%s", len(df_comp), list(df_comp.columns))
            logger.info("df_base barcode sample=%s", df_base["__barcode"].dropna().astype(str).head(10).tolist())
            logger.info("df_comp barcode sample=%s", df_comp["__barcode"].dropna().astype(str).head(10).tolist())

            df_base = df_base.merge(
                df_comp.drop(columns=["Cod_Barras"], errors="ignore"),
                on="__barcode",
                how="left",
                suffixes=("", "__coleta"),
            )

            for c in (col_conc_1, col_conc_2):
                c_new = f"{c}__coleta"
                if c_new in df_base.columns:
                    s_new = pd.to_numeric(df_base[c_new], errors="coerce")
                    s_old = pd.to_numeric(df_base[c], errors="coerce")
                    df_base[c] = np.where(s_new.notna() & (s_new > 0), s_new, s_old).astype(float)
                    df_base.drop(columns=[c_new], inplace=True, errors="ignore")

            df_base[col_conc_1] = pd.to_numeric(df_base[col_conc_1], errors="coerce").fillna(0.0)
            df_base[col_conc_2] = pd.to_numeric(df_base[col_conc_2], errors="coerce").fillna(0.0)

            logger.info(
                "precos preenchidos: petz=%d procampo=%d",
                int((df_base[col_conc_1] > 0).sum()),
                int((df_base[col_conc_2] > 0).sum()),
            )
    except Exception:
        logger.exception("Falha ao aplicar concorrência do schema coleta (continua com 0.0).")

    # =========================================================
    # TRIMESTRE / 6M / ANO: usa LEGACY por padrão (compat)
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

    # ======= MÊS REF AGORA = MÊS SELECIONADO (último mês da janela) =======
    mes_ref_safe = labels_safe[-1]
    fat_ref = f"Fat_{mes_ref_safe}"
    marg_ref = f"Marg_Val_{mes_ref_safe}"
    qtd_ref = f"Qtd_{mes_ref_safe}"

    mes_ref_label = safe_to_legacy.get(mes_ref_safe, mes_ref_safe)
    logger.info("mes_ref_safe(MÊS_SELECIONADO)= %s | label=%s", mes_ref_safe, mes_ref_label)

    missing = [c for c in (fat_ref, marg_ref, qtd_ref) if c not in df_base.columns]
    if missing:
        raise RuntimeError(f"Colunas do mês-ref ausentes: {missing}. mes_ref_safe={mes_ref_safe}")

    # Derivadas do mês ref (mês selecionado)
    df_base["Qtd_Ref"] = df_base[qtd_ref].fillna(0.0).round().astype(int)
    df_base["Preco_Atual"] = _safe_div(df_base[fat_ref], df_base[qtd_ref])
    df_base["Custo"] = _safe_div(df_base[fat_ref] - df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Unit"] = _safe_div(df_base[marg_ref], df_base[qtd_ref])
    df_base["Marg_Perc"] = _safe_div(df_base[marg_ref], df_base[fat_ref])

    df_base["Qtd Nov"] = df_base["Qtd_Ref"]
    df_base["Preço Atual"] = df_base["Preco_Atual"]
    df_base["Marg R$"] = df_base["Marg_Unit"]
    df_base["Marg %"] = df_base["Marg_Perc"]
    df_base["Marg Atual %"] = df_base["Marg_Perc"]

    df_base["Preco_Mais_Recente"] = df_base["Preco_Atual"]
    df_base["Custo_Mais_Recente"] = df_base["Custo"]
    df_base["Qtd_Media_Mensal"] = df_base["Qtd_Ref"]

    df_base["Fat_Ref"] = df_base[fat_ref]
    df_base["Marg_Val_Ref"] = df_base[marg_ref]

    df_base["Curva_ABC"] = _calc_curva_abc(df_base, "Fat_Ref")
    df_base["ABC"] = df_base["Curva_ABC"]
    df_base["Categ"] = df_base["Area"]

    # Históricos
    qtd_cols_ano_safe = [f"Qtd_{m}" for m in labels_safe]
    qtd_cols_6m_safe = [f"Qtd_{m}" for m in labels_safe[-6:]]
    qtd_cols_3m_safe = [f"Qtd_{m}" for m in labels_safe[-3:]]

    _ensure_columns(df_base, qtd_cols_ano_safe, 0.0)
    _ensure_columns(df_base, qtd_cols_6m_safe, 0.0)
    _ensure_columns(df_base, qtd_cols_3m_safe, 0.0)

    df_base["Hist_Qtd_Media_6M"] = df_base[qtd_cols_6m_safe].mean(axis=1)
    df_base["Hist_Qtd_Media_3M"] = df_base[qtd_cols_3m_safe].mean(axis=1)
    df_base["Hist_Qtd_Pico"] = df_base[qtd_cols_ano_safe].max(axis=1)

    pico_col_safe = df_base[qtd_cols_ano_safe].idxmax(axis=1).astype(str)
    pico_safe = pico_col_safe.str.replace("Qtd_", "", regex=False)
    df_base["Hist_Mes_Pico"] = pico_safe.map(lambda s: safe_to_legacy.get(str(s), str(s)))

    # Benchmarks globais
    logger.info("Calculando Benchmarks Globais...")

    # copy rápido pra reduzir fragmentação depois de muita coluna
    df_base = df_base.copy()

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
        df_base.groupby(COLUNA_AGREGACAO_PRINCIPAL)["Fat_Ref"]
        .sum()
        .sort_values(ascending=False)
    )
    lista_fornecedores = forn_ranking.index.tolist() or ["SEM DADOS"]
    lista_categorias_global = sorted(df_base["Area"].astype(str).unique().tolist())

    logger.info("Base carregada do Postgres: %d linhas", len(df_base))
    return df_base, bench_ano, bench_6m, bench_3m, lista_fornecedores, lista_categorias_global