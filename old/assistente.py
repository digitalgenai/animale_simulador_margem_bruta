import pandas as pd
import os
import sys
import FreeSimpleGUI as sg
import numpy as np

# --- 1. Configuração de Caminho ---
# --- (Ver se está rodando no terminal ou no .exe) ---
if getattr(sys, 'frozen', False):
    diretorio_atual = os.path.dirname(sys.executable)
else:
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))

nome_planilha = "base_simulador.xlsx"
caminho_base_simulador = os.path.join(diretorio_atual, nome_planilha)

TAXA_DEDUCAO_FATURAMENTO = 0.2203
col_conc_1 = 'Preço PETZ'      
col_conc_2 = 'Preço PROCAMPO'
NOME_CONC_1 = 'PETZ'      
NOME_CONC_2 = 'PROCAMPO'
COLUNA_AGREGACAO_PRINCIPAL = 'Fornecedor'

ALERTA_PRECO_BAIXO = ' 🔻'
ALERTA_PRECO_ALTO = ' 🔺'

# --- Funções Auxiliares ---
def fmt_real(val): return f"R$ {val:,.2f}" if isinstance(val, (int, float)) and not np.isnan(val) else "-"
def fmt_perc(val): return f"{val:.1%}" if isinstance(val, (int, float)) and not np.isnan(val) else "-"

def fmt_media(val):
    try:
        val = float(val)
        return f"{val:,.2f}" if not np.isnan(val) else "-"
    except: return "-"

def fmt_qtd(val):
    try:
        val = float(val)
        return f"{val:,.0f}" if not np.isnan(val) else "-"
    except: return "-"

def fmt_str(val): return str(val) if val and str(val).strip() != '' and str(val).lower() != 'nan' else "-"

def get_menor_concorrente(row):
    c1 = row.get(col_conc_1, 0); c2 = row.get(col_conc_2, 0)
    c1 = c1 if isinstance(c1, (int, float)) and not np.isnan(c1) else 0
    c2 = c2 if isinstance(c2, (int, float)) and not np.isnan(c2) else 0
    if c1 > 0 and c2 > 0: return min(c1, c2)
    elif c1 > 0: return c1
    elif c2 > 0: return c2
    return 0.0

def dif_concorrente_custom(preco_atual, conc_referencia):
    if conc_referencia == 0: return 0.0
    try: return (preco_atual - conc_referencia) / conc_referencia
    except: return 0.0

def calcular_custo_necessario(preco_alvo, margem_alvo):
    if preco_alvo <= 0: return 0.0
    try: return (preco_alvo * (1 - TAXA_DEDUCAO_FATURAMENTO)) - (preco_alvo * margem_alvo)
    except: return 0.0

def calcular_margem_real_percentual(custo, preco):
    if preco <= 0: return 0.0
    try: return ((preco * (1 - TAXA_DEDUCAO_FATURAMENTO)) - custo) / preco
    except: return 0.0

def calcular_margem_real_valor(custo, preco):
    if preco <= 0: return 0.0
    try: return (preco * (1 - TAXA_DEDUCAO_FATURAMENTO)) - custo
    except: return 0.0

# --- 2. Carga de Dados e Pré-Cálculos ---
bench_ano = {}
bench_6m = {}
bench_3m = {}

try:
    print("Carregando base...")
    if not os.path.exists(caminho_base_simulador):
        sg.popup_error(f"Arquivo não encontrado:\n{caminho_base_simulador}")
        sys.exit()

    df_base = pd.read_excel(caminho_base_simulador)
    df_base.columns = df_base.columns.str.strip()
   
    if 'Cod. Produto' in df_base.columns: df_base.rename(columns={'Cod. Produto': 'SKU'}, inplace=True)
    df_base.dropna(subset=['Produto'], inplace=True)
   
    # Tratamento Textos
    for col in ['Fornecedor', 'Fabricante', 'Area', 'Produto', 'SKU', 'Curva_ABC', 'Cod_Barras', 'Hist_Mes_Pico']:
        if col in df_base.columns:
            df_base[col] = df_base[col].astype(str).str.strip().replace(['nan', 'NaN', ''], 'SEM_INFO')
            if col == 'Cod_Barras': df_base[col] = df_base[col].str.replace(r'\.0$', '', regex=True)
        else:
            df_base[col] = '-'

    # Tratamento Numéricos
    cols_num = ['Preco_Mais_Recente', 'Custo_Mais_Recente', 'Hist_Qtd_Media_6M', 'Hist_Qtd_Media_3M', 'Hist_Qtd_Pico', 'Qtd_Media_Mensal', 'Fat_Total_Trimestre', 'Valor_Margem_Total_Trimestre']
    lista_meses_ano = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov']
    for m in lista_meses_ano:
        cols_num.extend([f'Fat_{m}', f'Marg_Val_{m}'])
   
    for col in cols_num:
        if col not in df_base.columns: df_base[col] = 0.0
        df_base[col] = pd.to_numeric(df_base[col], errors='coerce').fillna(0)

    for col in ['Area_Margem_Media']:
        if col not in df_base.columns: df_base[col] = 0.0
    if col_conc_1 not in df_base.columns: df_base[col_conc_1] = 0.0
    if col_conc_2 not in df_base.columns: df_base[col_conc_2] = 0.0

    df_base['Margem_Media_Trimestre'] = (df_base['Valor_Margem_Total_Trimestre'] / df_base['Fat_Total_Trimestre']).fillna(0)
   
    # --- CÁLCULO DOS BENCHMARKS GLOBAIS ---
    print("Calculando Benchmarks Globais...")
    cols_fat_ano = [f'Fat_{m}' for m in lista_meses_ano]
    cols_marg_ano = [f'Marg_Val_{m}' for m in lista_meses_ano]
    cols_fat_6m = [f'Fat_{m}' for m in ['Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov']]
    cols_marg_6m = [f'Marg_Val_{m}' for m in ['Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov']]
    cols_fat_3m = [f'Fat_{m}' for m in ['Set', 'Out', 'Nov']]
    cols_marg_3m = [f'Marg_Val_{m}' for m in ['Set', 'Out', 'Nov']]

    # Cria colunas temporárias
    df_base['Temp_Fat_Ano'] = df_base[cols_fat_ano].sum(axis=1)
    df_base['Temp_Marg_Ano'] = df_base[cols_marg_ano].sum(axis=1)
    df_base['Temp_Fat_6M'] = df_base[cols_fat_6m].sum(axis=1)
    df_base['Temp_Marg_6M'] = df_base[cols_marg_6m].sum(axis=1)
    df_base['Temp_Fat_3M'] = df_base[cols_fat_3m].sum(axis=1)
    df_base['Temp_Marg_3M'] = df_base[cols_marg_3m].sum(axis=1)

    # Agrupa
    df_bench = df_base.groupby('Area')[['Temp_Fat_Ano', 'Temp_Marg_Ano', 'Temp_Fat_6M', 'Temp_Marg_6M', 'Temp_Fat_3M', 'Temp_Marg_3M']].sum()
    df_bench['Bench_Ano'] = df_bench['Temp_Marg_Ano'] / df_bench['Temp_Fat_Ano']
    df_bench['Bench_6M'] = df_bench['Temp_Marg_6M'] / df_bench['Temp_Fat_6M']
    df_bench['Bench_3M'] = df_bench['Temp_Marg_3M'] / df_bench['Temp_Fat_3M']
   
    bench_ano = df_bench['Bench_Ano'].to_dict()
    bench_6m = df_bench['Bench_6M'].to_dict()
    bench_3m = df_bench['Bench_3M'].to_dict()
   
    
    cols_to_drop = ['Temp_Fat_Ano', 'Temp_Marg_Ano', 'Temp_Fat_6M', 'Temp_Marg_6M', 'Temp_Fat_3M', 'Temp_Marg_3M']
    df_base.drop(columns=cols_to_drop, inplace=True, errors='ignore')
   
    df_base = df_base.set_index('Produto')

    if 'Sim_Manual_Ativa' not in df_base.columns:
        df_base['Sim_Manual_Ativa'] = False
        df_base['Sim_Preco_Manual'] = 0.0
        df_base['Sim_Margem_Manual'] = 0.0

    if 'Sim_Conc_Ativa' not in df_base.columns:
        df_base['Sim_Conc_Ativa'] = False
        df_base['Sim_Conc_Delta'] = 0.0

    # Listas Globais
    forn_ranking = df_base.groupby(COLUNA_AGREGACAO_PRINCIPAL)['Fat_Total_Trimestre'].sum().sort_values(ascending=False)
    lista_fornecedores = forn_ranking.index.tolist()
    if not lista_fornecedores: lista_fornecedores = ["SEM DADOS"]
   
    lista_categorias_global = sorted(df_base['Area'].unique().tolist())

except Exception as e:
    sg.popup_error(f"Erro inicialização: {e}")
    sys.exit()

# --- 3. Interface Gráfica ---
sg.theme('LightBlue')

def criar_layout_resumo(suffix):
    col_breakdown = [
        [sg.Text("Top Categorias (Forn. vs Benchmarks):", font=("Helvetica", 9, "bold"))],
        [sg.Multiline("", key=f'-RES_BREAKDOWN{suffix}-', size=(90, 6), font=("Courier New", 9), disabled=True, background_color='#f0f0f0')]
    ]
    col_historico = [
        [sg.Text("Detalhes (Inteligência Temporal):", font=("Helvetica", 9, "bold"), text_color='navy')],
        [sg.Text("Produto:", size=(7,1)), sg.Text("Selecione...", key=f'-HIST_PROD{suffix}-', size=(35,1), font=("Helvetica", 9, "italic"))],
        [sg.Text("Méd 6M:", size=(7,1)), sg.Text("-", key=f'-HIST_6M{suffix}-', font=("Helvetica", 9, "bold")),
         sg.Text("Méd 3M:", size=(7,1)), sg.Text("-", key=f'-HIST_3M{suffix}-', font=("Helvetica", 9, "bold"))],
        [sg.Text("Venda Nov:", size=(10,1)), sg.Text("-", key=f'-HIST_NOV{suffix}-', font=("Helvetica", 9, "bold"), text_color='blue'),
         sg.Text("Pico:", size=(7,1)), sg.Text("-", key=f'-HIST_PICO{suffix}-', font=("Helvetica", 9, "bold"), text_color='green')]
    ]
    return [
        [
            sg.Text("Fat. Total:", font=("Helvetica", 10)), sg.Text("-", key=f'-RES_FAT{suffix}-', font=("Helvetica", 10, "bold"), size=(12,1)),
            sg.Text("Margem Média:", font=("Helvetica", 10)), sg.Text("-", key=f'-RES_MARG{suffix}-', font=("Helvetica", 10, "bold"), size=(8,1), text_color='blue'),
            sg.VerticalSeparator(),
            sg.Text("Total SKUs:", font=("Helvetica", 10)), sg.Text("-", key=f'-RES_QTD_SKU{suffix}-', font=("Helvetica", 10, "bold"), size=(5,1)),
            sg.Text("A:", font=("Helvetica", 10, "bold"), text_color='green'), sg.Text("-", key=f'-RES_SKU_A{suffix}-', size=(3,1)),
            sg.Text("B:", font=("Helvetica", 10, "bold"), text_color='#bda404'), sg.Text("-", key=f'-RES_SKU_B{suffix}-', size=(3,1)),
            sg.Text("C:", font=("Helvetica", 10, "bold"), text_color='red'), sg.Text("-", key=f'-RES_SKU_C{suffix}-', size=(3,1)),
        ],
        [sg.HorizontalSeparator()],
        [sg.Column(col_breakdown, expand_x=True, vertical_alignment='top'), sg.VerticalSeparator(), sg.Column(col_historico, expand_x=True, vertical_alignment='top', background_color='#e6f2ff', p=5)]
    ]

# -- Layout ABA 1 --
cols_tab1 = ['SKU', 'Produto', 'ABC', 'Categ', 'Qtd Nov', 'Preço Atual', 'Custo', 'Marg R$', 'Marg %',
             NOME_CONC_1, NOME_CONC_2, 'Dif % (Menor)',
             'Sim Preço', 'Sim Marg', 'Sim Custo Nec']

layout_tab1 = [
    [sg.Frame("Resumo Financeiro", criar_layout_resumo('_T1'), expand_x=True)],
    [sg.Text("Objetivo: Definir Preço/Margem para calcular Custo Alvo.", font=("Helvetica", 8, "italic"), text_color='gray')],
    [sg.Table(values=[['' for _ in cols_tab1]], headings=cols_tab1, auto_size_columns=False,
              col_widths=[8, 20, 4, 10, 7, 9, 9, 9, 8, 8, 8, 9, 9, 8, 10],
              justification='right', num_rows=15, key='-TABELA_T1-',
              enable_events=True, bind_return_key=True, select_mode=sg.TABLE_SELECT_MODE_BROWSE,
              font=("Helvetica", 9), expand_x=True, expand_y=True)]
]

# -- Layout ABA 2 --
cols_tab2 = ['SKU', 'Produto', 'ABC', 'Categ', 'Qtd Nov', 'Preço Atual', 'Custo', 'Marg Atual %',
             NOME_CONC_1, NOME_CONC_2, 'Dif Atual (Menor)',
             'DELTA ALVO %', 'Sim Preço (Conc)', 'Sim Margem (Result)']

layout_tab2 = [
    [sg.Frame("Resumo Mercado", criar_layout_resumo('_T2'), expand_x=True)],
    [sg.Text("Objetivo: Definir Delta % vs Concorrente para ver a Margem Resultante.", font=("Helvetica", 8, "italic"), text_color='gray')],
    [sg.Table(values=[['' for _ in cols_tab2]], headings=cols_tab2, auto_size_columns=False,
              col_widths=[8, 20, 4, 10, 7, 9, 9, 10, 9, 9, 8, 11, 11, 11],
              justification='right', num_rows=15, key='-TABELA_T2-',
              enable_events=True, bind_return_key=True, select_mode=sg.TABLE_SELECT_MODE_BROWSE,
              font=("Helvetica", 9), expand_x=True, expand_y=True)]
]

# -- Layout ABA 3  --
cols_tab3 = ['Fornecedor', 'Fat Nov', 'Margem Nov R$', 'Margem Nov %']

layout_filtros_t3 = [
    [sg.Text("1. Selecione a Categoria (Principal):", font=("Helvetica", 9, "bold"), text_color='navy'),
     sg.Combo(lista_categorias_global, key='-CAT_T3-', size=(25,1), enable_events=True, readonly=True),
     sg.Text("-->", font=("Helvetica", 10, "bold")),
     sg.Text("2. Fornecedor (Opcional):", font=("Helvetica", 9, "bold"), text_color='navy'),
     sg.Combo([], key='-FORN_T3-', size=(25,1), enable_events=True, readonly=True)]
]

layout_tab3 = [
    [sg.Frame("Filtros Específicos (Prioridade Invertida)", layout_filtros_t3, expand_x=True, background_color='#e1e1e1')],
    [sg.Frame("Resumo Categoria", criar_layout_resumo('_T3'), expand_x=True)],
    [sg.Table(values=[['' for _ in cols_tab3]], headings=cols_tab3, auto_size_columns=False,
              col_widths=[30, 15, 15, 12],
              justification='right', num_rows=15, key='-TABELA_T3-',
              enable_events=True, bind_return_key=True, select_mode=sg.TABLE_SELECT_MODE_BROWSE,
              font=("Helvetica", 9), expand_x=True, expand_y=True)]
]

# -- Topo Global (Afeta T1 e T2) --
layout_topo = [
    [
        sg.Text(f"{COLUNA_AGREGACAO_PRINCIPAL}:", font=("Helvetica", 10, "bold")), sg.Combo(lista_fornecedores, key='-FORN-', size=(25,1), enable_events=True, readonly=True),
        sg.Text("Fabr:", font=("Helvetica", 10, "bold")), sg.Combo([], key='-FAB-', size=(15,1), enable_events=True, readonly=True),
        sg.Text("Categ:", font=("Helvetica", 10, "bold")), sg.Combo([], key='-CAT-', size=(15,1), enable_events=True, readonly=True),
        sg.VerticalSeparator(),
        sg.Text("Meta Fin. (%):", font=("Helvetica", 9), text_color='navy'),
        sg.Input("30.0", key='-META_T1-', size=(5,1), justification='right'),
        sg.Text("| Delta Padrão (%):", font=("Helvetica", 9), text_color='#b75402'),
        sg.Input("0.0", key='-META_T2-', size=(5,1), justification='right', visible=True),
        sg.Button("Recalcular", key='-REFRESH-', button_color=('white', 'navy')),
        sg.Push(),
        sg.Button("Exportar", key='-EXPORTAR-', button_color=('black', 'lightgreen'))
    ]
]

layout_final = [
    [sg.Frame("Filtros Globais (Para Abas 1 e 2)", layout_topo, expand_x=True)],
    [sg.TabGroup([
        [sg.Tab("1.Visão de Custo", layout_tab1, key='-TAB1-'),
         sg.Tab("2.Visão de Precificação", layout_tab2, key='-TAB2-'),
         sg.Tab("3.Visão Categ (Fornecedores)", layout_tab3, key='-TAB3-')]
    ], key='-TAB_GROUP-', expand_x=True, expand_y=True, enable_events=True)]
]

window = sg.Window('Simulador v76.8 - Correção Inicialização', layout_final, finalize=True, resizable=True)
window.maximize()
window['-TABELA_T1-'].bind('<Double-Button-1>', '+DOUBLE_CLICK+')
window['-TABELA_T2-'].bind('<Double-Button-1>', '+DOUBLE_CLICK+')
window['-TABELA_T3-'].bind('<Double-Button-1>', '+DOUBLE_CLICK+')

# --- 4. Lógica Principal ---
df_view_atual = pd.DataFrame()
meta_t1_atual = 0.30
meta_t2_atual = 0.00

def atualizar_interface(suffix_override=None):
    aba_ativa = values['-TAB_GROUP-']
   
    # Define Suffix
    if suffix_override: suffix = suffix_override
    else:
        if aba_ativa == '-TAB1-': suffix = '_T1'
        elif aba_ativa == '-TAB2-': suffix = '_T2'
        else: suffix = '_T3'
   
    if df_view_atual.empty:
        window[f'-TABELA{suffix}-'].update(values=[])
        return

    # Resumos
    fat_total = df_view_atual['Fat_Total_Trimestre'].sum()
    marg_pond = (df_view_atual['Fat_Total_Trimestre'] * df_view_atual['Margem_Media_Trimestre']).sum() / fat_total if fat_total > 0 else 0
   
    cols_fat_5m = ['Fat_Jun', 'Fat_Jul', 'Fat_Ago', 'Fat_Set', 'Fat_Out']
    cols_marg_val_5m = ['Marg_Val_Jun', 'Marg_Val_Jul', 'Marg_Val_Ago', 'Marg_Val_Set', 'Marg_Val_Out']
    margem_5m = df_view_atual[cols_marg_val_5m].sum().sum() / df_view_atual[cols_fat_5m].sum().sum() if df_view_atual[cols_fat_5m].sum().sum() > 0 else 0

    window[f'-RES_FAT{suffix}-'].update(fmt_real(fat_total))
    window[f'-RES_MARG{suffix}-'].update(fmt_perc(marg_pond))
   
    counts_abc = df_view_atual['Curva_ABC'].value_counts()
    window[f'-RES_QTD_SKU{suffix}-'].update(len(df_view_atual))
    window[f'-RES_SKU_A{suffix}-'].update(counts_abc.get('A', 0))
    window[f'-RES_SKU_B{suffix}-'].update(counts_abc.get('B', 0))
    window[f'-RES_SKU_C{suffix}-'].update(counts_abc.get('C', 0))

    # Breakdown
    df_bd = df_view_atual.groupby('Area').agg(Fat=('Fat_Total_Trimestre', 'sum'), Marg=('Valor_Margem_Total_Trimestre', 'sum')).sort_values('Fat', ascending=False).head(5)
    bd_txt = f"{'CATEGORIA':<15} {'FAT(Nov)':>12} {'MG(Nov)':>8} {'GL(Year)':>8}\n" + ("-"*50) + "\n"
    for cat, row in df_bd.iterrows():
        m_perc = row['Marg'] / row['Fat'] if row['Fat'] > 0 else 0
        bg_ano = bench_ano.get(cat, 0)
        bd_txt += f"{cat[:15]:<15} {fmt_real(row['Fat']):>12} {fmt_perc(m_perc):>8} {fmt_perc(bg_ano):>8}\n"
    window[f'-RES_BREAKDOWN{suffix}-'].update(bd_txt)

    novos_valores = []
    cores = []
   
    # --- LOGICA ABA 3: AGREGADA POR FORNECEDOR ---
    if aba_ativa == '-TAB3-':
        # Agrupa os dados filtrados (pela Categoria) por Fornecedor
        df_agg = df_view_atual.groupby('Fornecedor')[['Fat_Nov', 'Marg_Val_Nov']].sum()
        df_agg = df_agg.sort_values('Fat_Nov', ascending=False)
       
        for forn_nome, row in df_agg.iterrows():
            f_nov = row['Fat_Nov']
            m_nov_val = row['Marg_Val_Nov']
            m_nov_perc = m_nov_val / f_nov if f_nov > 0 else 0
           
            novos_valores.append([
                forn_nome,
                fmt_real(f_nov),
                fmt_real(m_nov_val),
                fmt_perc(m_nov_perc)
            ])
            cores.append((len(novos_valores)-1, 'white')) # Cor padrão

    else:
        # --- LOGICA ABAS 1 e 2: DETALHE POR PRODUTO ---
        df_limit = df_view_atual.head(500)
       
        for index, (nome_prod, row_view) in enumerate(df_limit.iterrows()):
            try:
                dados_reais = df_base.loc[nome_prod]
                if isinstance(dados_reais, pd.DataFrame): dados_reais = dados_reais.iloc[0]
               
                p_atual = dados_reais['Preco_Mais_Recente']
                c_atual = dados_reais['Custo_Mais_Recente']
               
                sim_manual_ativa = dados_reais['Sim_Manual_Ativa']
                sim_preco_man = dados_reais['Sim_Preco_Manual']
                sim_marg_man = dados_reais['Sim_Margem_Manual']
               
                sim_conc_ativa = dados_reais['Sim_Conc_Ativa']
                sim_conc_delta = dados_reais['Sim_Conc_Delta']
            except:
                p_atual = 0; c_atual = 0
                sim_manual_ativa = False; sim_conc_ativa = False
           
            menor_conc = get_menor_concorrente(row_view)
            val_conc1 = row_view.get(col_conc_1, 0)
            val_conc2 = row_view.get(col_conc_2, 0)

            cor = 'white'
            marg_real = calcular_margem_real_percentual(c_atual, p_atual)
            if marg_real < 0: cor = "#D51313"
            elif menor_conc > 0 and p_atual < menor_conc and (menor_conc - p_atual) > 1: cor = '#FFF59D'
           
            # TAB 1
            if aba_ativa == '-TAB1-':
                dif_conc = dif_concorrente_custom(p_atual, menor_conc)
                if sim_manual_ativa:
                    sim_p = sim_preco_man
                    sim_m = sim_marg_man
                    sim_c = calcular_custo_necessario(sim_p, sim_m)
                else:
                    sim_p = menor_conc if menor_conc > 0 else p_atual
                    sim_m = meta_t1_atual
                    sim_c = calcular_custo_necessario(sim_p, sim_m)

                novos_valores.append([
                    fmt_str(row_view['SKU']), nome_prod, fmt_str(row_view['Curva_ABC']), fmt_str(row_view['Area']), fmt_qtd(row_view['Qtd_Media_Mensal']),
                    fmt_real(p_atual), fmt_real(c_atual), fmt_real(calcular_margem_real_valor(c_atual, p_atual)), fmt_perc(marg_real),
                    fmt_real(val_conc1), fmt_real(val_conc2), fmt_perc(dif_conc),
                    fmt_real(sim_p), fmt_perc(sim_m), fmt_real(sim_c)
                ])
               
            # TAB 2
            elif aba_ativa == '-TAB2-':
                dif_atual = dif_concorrente_custom(p_atual, menor_conc)
                delta_target = sim_conc_delta if sim_conc_ativa else meta_t2_atual
               
                if menor_conc > 0: sim_p_conc = menor_conc * (1 + delta_target)
                else: sim_p_conc = p_atual
               
                sim_marg_result = calcular_margem_real_percentual(c_atual, sim_p_conc)
                delta_str = fmt_perc(delta_target)
                if sim_conc_ativa: delta_str += " (M)"

                novos_valores.append([
                    fmt_str(row_view['SKU']), nome_prod, fmt_str(row_view['Curva_ABC']), fmt_str(row_view['Area']),
                    fmt_qtd(row_view['Qtd_Media_Mensal']),
                    fmt_real(p_atual), fmt_real(c_atual),
                    fmt_perc(marg_real),
                    fmt_real(val_conc1), fmt_real(val_conc2), fmt_perc(dif_atual),
                    delta_str,
                    fmt_real(sim_p_conc), fmt_perc(sim_marg_result)
                ])
           
            cores.append((index, cor))

    tabela_key = f'-TABELA{suffix}-'
    window[tabela_key].update(values=novos_valores, row_colors=cores)

# --- LOOP DE EVENTOS ---
while True:
    event, values = window.read()
    if event in (sg.WIN_CLOSED, 'Sair'): break

    # --- Lógica Global (Abas 1 e 2) ---
    if event == '-FORN-':
        forn_sel = values['-FORN-']
        if forn_sel:
            df_forn = df_base[df_base[COLUNA_AGREGACAO_PRINCIPAL] == forn_sel].copy()
            lista_fab = sorted(df_forn['Fabricante'].unique().tolist()); lista_fab.insert(0, "[TODOS]")
            window['-FAB-'].update(value="[TODOS]", values=lista_fab)
            lista_cat = sorted(df_forn['Area'].unique().tolist()); lista_cat.insert(0, "[TODAS]")
            window['-CAT-'].update(value="[TODAS]", values=lista_cat)
            event = '-REFRESH-'
           
    if event in ('-FAB-', '-CAT-', '-REFRESH-', '-TAB_GROUP-'):
        aba = values['-TAB_GROUP-']
       
        # Meta Fin
        try: meta_t1_atual = float(values['-META_T1-'].replace(',', '.')) / 100
        except: meta_t1_atual = 0.30
       
        try: meta_t2_atual = float(values['-META_T2-'].replace(',', '.')) / 100
        except: meta_t2_atual = 0.00
       
        # Se for Aba 1 ou 2, usa filtros globais
        if aba in ['-TAB1-', '-TAB2-']:
            forn = values['-FORN-']; fab = values['-FAB-']; cat = values['-CAT-']
            if forn:
                df_temp = df_base[df_base[COLUNA_AGREGACAO_PRINCIPAL] == forn]
                if fab and fab != "[TODOS]": df_temp = df_temp[df_temp['Fabricante'] == fab]
                if cat and cat != "[TODAS]": df_temp = df_temp[df_temp['Area'] == cat]
               
                df_temp['ABC_Order'] = df_temp['Curva_ABC'].map({'A':0,'B':1,'C':2}).fillna(3)
                df_view_atual = df_temp.sort_values(['ABC_Order', 'Fat_Total_Trimestre'], ascending=[True, False])
                atualizar_interface()
       
        # Se mudou para Aba 3, mas não mexeu nos filtros internos ainda
        elif aba == '-TAB3-':
            if values['-CAT_T3-']: event = '-FORN_T3-'

    # --- Lógica Exclusiva da Aba 3 (Invertida) ---
    if event == '-CAT_T3-':
        cat_sel_t3 = values['-CAT_T3-']
        if cat_sel_t3:
            df_cat = df_base[df_base['Area'] == cat_sel_t3]
            rank_forn_cat = df_cat.groupby('Fornecedor')['Fat_Total_Trimestre'].sum().sort_values(ascending=False)
            lista_forn_cat = rank_forn_cat.index.tolist()
            lista_forn_cat.insert(0, "[TODOS]")
            window['-FORN_T3-'].update(values=lista_forn_cat, value="[TODOS]")
            event = '-FORN_T3-'
           
    if event == '-FORN_T3-':
        cat_sel_t3 = values['-CAT_T3-']
        forn_sel_t3 = values['-FORN_T3-']
       
        if cat_sel_t3:
            df_temp_t3 = df_base[df_base['Area'] == cat_sel_t3]
            if forn_sel_t3 and forn_sel_t3 != "[TODOS]":
                df_temp_t3 = df_temp_t3[df_temp_t3['Fornecedor'] == forn_sel_t3]
           
            df_view_atual = df_temp_t3
           
            atualizar_interface(suffix_override='_T3')

    # --- Cliques na Tabela ---
    aba_atual = values['-TAB_GROUP-']
    key_tabela = '-TABELA_T1-' if aba_atual == '-TAB1-' else ('-TABELA_T2-' if aba_atual == '-TAB2-' else '-TABELA_T3-')
   
    if event in ('-TABELA_T1-', '-TABELA_T2-', '-TABELA_T3-'):
        if not df_view_atual.empty and values[key_tabela]:
            if aba_atual == '-TAB3-': pass
            else:
                try:
                    row_idx = values[key_tabela][0]
                    prod_sel = df_view_atual.head(500).index[row_idx]
                    row = df_base.loc[prod_sel]
                    suffix = '_T1' if aba_atual == '-TAB1-' else '_T2'
                   
                    if isinstance(row, pd.DataFrame): row = row.iloc[0]
                    window[f'-HIST_PROD{suffix}-'].update(f"{prod_sel[:30]}...")
                    window[f'-HIST_6M{suffix}-'].update(fmt_media(row['Hist_Qtd_Media_6M']))
                    window[f'-HIST_3M{suffix}-'].update(fmt_media(row['Hist_Qtd_Media_3M']))
                    window[f'-HIST_NOV{suffix}-'].update(fmt_media(row['Qtd_Media_Mensal']))
                    window[f'-HIST_PICO{suffix}-'].update(f"{row['Hist_Mes_Pico']} ({fmt_qtd(row['Hist_Qtd_Pico'])})")
                except: pass

    # Double Click (Popups) - Apenas para abas 1 e 2
    if event.endswith('+DOUBLE_CLICK+'):
        if aba_atual == '-TAB3-': continue
        if df_view_atual.empty or not values[key_tabela]: continue
       
        try:
            row_idx = values[key_tabela][0]
            prod_sel = df_view_atual.head(500).index[row_idx]
            row = df_base.loc[prod_sel]
            menor_conc = get_menor_concorrente(row)

            if aba_atual == '-TAB1-':
                is_manual = row['Sim_Manual_Ativa']
                val_p = row['Sim_Preco_Manual'] if is_manual else (menor_conc if menor_conc > 0 else row['Preco_Mais_Recente'])
                val_m = (row['Sim_Margem_Manual'] * 100) if is_manual else (meta_t1_atual * 100)
                custo_calc = calcular_custo_necessario(val_p, val_m/100)
               
                layout_pop = [
                    [sg.Text(f"Financeiro: {prod_sel[:30]}", font=("bold"))],
                    [sg.TabGroup([[
                        sg.Tab("Definir Margem", [[sg.Text("Preço R$:"), sg.Input(f"{val_p:.2f}", key='-P-', size=(8,1))],
                                                [sg.Text("Margem %:"), sg.Input(f"{val_m:.1f}", key='-M-', size=(8,1))]], key='-T_MARG-'),
                        sg.Tab("Definir Custo", [[sg.Text("Preço R$:"), sg.Input(f"{val_p:.2f}", key='-P2-', size=(8,1))],
                                                [sg.Text("Custo R$:"), sg.Input(f"{custo_calc:.2f}", key='-C-', size=(8,1))]], key='-T_CUST-')
                    ]], key='-TG-')],
                    [sg.Button("Salvar", key='-SAVE-'), sg.Button("Reset", key='-RESET-')]
                ]
                win_p = sg.Window("Simulação Fin.", layout_pop, modal=True)
                ev_p, v_p = win_p.read()
                win_p.close()
               
                if ev_p == '-SAVE-':
                    if v_p['-TG-'] == '-T_MARG-':
                        df_base.at[prod_sel, 'Sim_Preco_Manual'] = float(v_p['-P-'].replace(',','.'))
                        df_base.at[prod_sel, 'Sim_Margem_Manual'] = float(v_p['-M-'].replace(',','.'))/100
                    else:
                        np_val = float(v_p['-P2-'].replace(',','.'))
                        nc_val = float(v_p['-C-'].replace(',','.'))
                        nm_val = 1 - TAXA_DEDUCAO_FATURAMENTO - (nc_val/np_val) if np_val > 0 else 0
                        df_base.at[prod_sel, 'Sim_Preco_Manual'] = np_val
                        df_base.at[prod_sel, 'Sim_Margem_Manual'] = nm_val
                    df_base.at[prod_sel, 'Sim_Manual_Ativa'] = True
                    atualizar_interface()

                elif ev_p == '-RESET-':
                    df_base.at[prod_sel, 'Sim_Manual_Ativa'] = False
                    atualizar_interface()

            elif aba_atual == '-TAB2-':
                delta_atual = row['Sim_Conc_Delta'] if row['Sim_Conc_Ativa'] else meta_t2_atual
                p_atual_conc = menor_conc * (1+delta_atual) if menor_conc > 0 else row['Preco_Mais_Recente']
               
                layout_pop = [
                    [sg.Text(f"Mercado: {prod_sel[:30]}", font=("bold"))],
                    [sg.Text(f"Menor Concorrente: {fmt_real(menor_conc)}", text_color='blue')],
                    [sg.HorizontalSeparator()],
                    [sg.Text("Diferença Alvo (%):"), sg.Input(f"{delta_atual*100:.1f}", key='-DELTA-', size=(8,1)), sg.Text("% (Ex: -5.0 para 5% abaixo)")],
                    [sg.Text(f"Preço Resultante Estimado: {fmt_real(p_atual_conc)}", text_color='gray', font=("italic"))],
                    [sg.Button("Salvar Delta", key='-SAVE-'), sg.Button("Usar Padrão", key='-RESET-')]
                ]
                win_p = sg.Window("Simulação Mercado", layout_pop, modal=True)
                ev_p, v_p = win_p.read()
                win_p.close()
               
                if ev_p == '-SAVE-':
                    novo_delta = float(v_p['-DELTA-'].replace(',','.')) / 100
                    df_base.at[prod_sel, 'Sim_Conc_Delta'] = novo_delta
                    df_base.at[prod_sel, 'Sim_Conc_Ativa'] = True
                    atualizar_interface()
                elif ev_p == '-RESET-':
                    df_base.at[prod_sel, 'Sim_Conc_Ativa'] = False
                    atualizar_interface()

        except Exception as e: print(f"Erro click: {e}")

    # Exportar
    if event == '-EXPORTAR-':
        if df_view_atual.empty: continue
        aba = values['-TAB_GROUP-']
        nome_tipo = "CATEGORIA" if aba == '-TAB3-' else ("FINANCEIRO" if aba == '-TAB1-' else "MERCADO")
        nome_arquivo = f"Simulacao_{nome_tipo}.xlsx"
        try:
            df_view_atual.to_excel(nome_arquivo)
            sg.popup_ok(f"Exportado: {nome_arquivo}")
        except: sg.popup_error("Erro exportar")

window.close()
