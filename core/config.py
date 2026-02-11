from __future__ import annotations

import os
import sys

# --- Caminho base ---
def get_current_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


NOME_PLANILHA = "base_simulador.xlsx"
BASE_SIMULADOR_PATH = os.getenv(
    "BASE_SIMULADOR_PATH",
    os.path.join(os.path.dirname(get_current_dir()), NOME_PLANILHA),
)

# --- Stage ---
PGSCHEMA_DEFAULT = os.getenv("PGSCHEMA", "stage")
PGTABLE_DEFAULT = os.getenv("PGTABLE", "obt_faturamento")

# --- Coleta (concorrentes) ---
PGSCHEMA_COLETA_DEFAULT = os.getenv("PGSCHEMA_COLETA", "coleta")
COLETA_TABLE_MISSAO_DEFAULT = os.getenv("COLETA_TABLE_MISSAO", "missao")
COLETA_TABLE_MISSAO_PRODUTO_DEFAULT = os.getenv("COLETA_TABLE_MISSAO_PRODUTO", "missao_produto")
COLETA_TABLE_CONCORRENTE_DEFAULT = os.getenv("COLETA_TABLE_CONCORRENTE", "concorrente")
COLETA_TABLE_PRODUTO_DEFAULT = os.getenv("COLETA_TABLE_PRODUTO", "produto")

# Janela móvel em meses (evita depender do len(LISTA_MESES_ANO))
N_MESES_JANELA = int(os.getenv("N_MESES_JANELA", "12"))

# --- Constantes do negócio ---
TAXA_DEDUCAO_FATURAMENTO = 0.2203
TAXA_ESTETICA_SAUDE = 0.0208

col_conc_1 = "Preço PETZ"
col_conc_2 = "Preço PROCAMPO"
NOME_CONC_1 = "PETZ"
NOME_CONC_2 = "PROCAMPO"
COLUNA_AGREGACAO_PRINCIPAL = "Fornecedor"

ALERTA_PRECO_BAIXO = " 🔻"
ALERTA_PRECO_ALTO = " 🔺"

# --- Meses ---
LISTA_MESES_ANO = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

# --- Colunas textuais ---
TEXT_COLS = ["Fornecedor", "Fabricante", "Area", "Produto", "SKU", "Curva_ABC", "Cod_Barras", "Hist_Mes_Pico"]

# --- Colunas numéricas base (contrato com view_builders) ---
BASE_NUM_COLS = [
    "Preco_Mais_Recente",
    "Custo_Mais_Recente",
    "Hist_Qtd_Media_6M",
    "Hist_Qtd_Media_3M",
    "Hist_Qtd_Pico",
    "Qtd_Media_Mensal",
    "Fat_Total_Trimestre",
    "Valor_Margem_Total_Trimestre",
    "Margem_Media_Trimestre",
]

AUX_NUM_COLS = ["Area_Margem_Media"]

SIM_COLS_DEFAULTS = {
    "Sim_Manual_Ativa": False,
    "Sim_Preco_Manual": 0.0,
    "Sim_Margem_Manual": 0.0,
    "Sim_Conc_Ativa": False,
    "Sim_Conc_Delta": 0.0,
}

MAX_ROWS_T1_T2 = 500
