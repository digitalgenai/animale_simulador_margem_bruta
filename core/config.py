from __future__ import annotations

import os
import sys

# --- 1. Configuração de Caminho ---
# --- (Ver se está rodando no terminal ou no .exe) ---
def get_current_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


# Arquivo base
NOME_PLANILHA = "base_simulador.xlsx"

# Permite override via variável de ambiente
# Ex: BASE_SIMULADOR_PATH=/data/base_simulador.xlsx
BASE_SIMULADOR_PATH = os.getenv(
    "BASE_SIMULADOR_PATH",
    os.path.join(os.path.dirname(get_current_dir()), NOME_PLANILHA),
)

# Constantes do negócio
TAXA_DEDUCAO_FATURAMENTO = 0.2203

col_conc_1 = "Preço PETZ"
col_conc_2 = "Preço PROCAMPO"
NOME_CONC_1 = "PETZ"
NOME_CONC_2 = "PROCAMPO"
COLUNA_AGREGACAO_PRINCIPAL = "Fornecedor"

ALERTA_PRECO_BAIXO = " 🔻"
ALERTA_PRECO_ALTO = " 🔺"

# Meses (mantendo exatamente a lista original: Jan..Nov)
LISTA_MESES_ANO = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov"]

# Conjuntos usados nos benchmarks
MESES_6M = ["Jun", "Jul", "Ago", "Set", "Out", "Nov"]
MESES_3M = ["Set", "Out", "Nov"]

# Colunas textuais que o original tratava
TEXT_COLS = ["Fornecedor", "Fabricante", "Area", "Produto", "SKU", "Curva_ABC", "Cod_Barras", "Hist_Mes_Pico"]

# Colunas numéricas base
BASE_NUM_COLS = [
    "Preco_Mais_Recente",
    "Custo_Mais_Recente",
    "Hist_Qtd_Media_6M",
    "Hist_Qtd_Media_3M",
    "Hist_Qtd_Pico",
    "Qtd_Media_Mensal",
    "Fat_Total_Trimestre",
    "Valor_Margem_Total_Trimestre",
    # as colunas do Nov são usadas em Tab3 (Fat_Nov / Marg_Val_Nov) e existem na lista mensal abaixo,
    # mas mantemos aqui o comportamento original: numéricas ausentes viram 0.
]

# Colunas auxiliares (o original garantia existência)
AUX_NUM_COLS = ["Area_Margem_Media"]

# Colunas de simulação (equivalentes às adicionadas no df_base do desktop)
SIM_COLS_DEFAULTS = {
    "Sim_Manual_Ativa": False,
    "Sim_Preco_Manual": 0.0,
    "Sim_Margem_Manual": 0.0,
    "Sim_Conc_Ativa": False,
    "Sim_Conc_Delta": 0.0,
}

# Layout / limites
MAX_ROWS_T1_T2 = 500
