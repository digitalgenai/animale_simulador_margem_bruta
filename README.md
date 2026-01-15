# Animale — Simulador de Margem Bruta (Web)

Aplicação web (Dash + dash-ag-grid) para simular **margem bruta** e apoiar decisões de **precificação** e **custo-alvo**, a partir de uma base Excel (`base_simulador.xlsx`).

---

## Visão geral

O simulador carrega uma planilha base e disponibiliza 3 visões:

### 1) Visão de Custo (Tab 1)
Objetivo: **definir preço/margem** e obter o **custo necessário (custo-alvo)**.  
- Cálculo principal: `calcular_custo_necessario(preco_alvo, margem_alvo)`
- Duplo clique em uma linha abre o **modal financeiro** (Definir Margem / Definir Custo).

### 2) Visão de Precificação (Tab 2)
Objetivo: definir um **delta (%) vs. menor concorrente** para visualizar **margem resultante**.  
- Menor concorrente é calculado a partir de 2 colunas configuradas (ex.: PETZ/PROCAMPO).
- Duplo clique em uma linha abre o **modal de mercado** para setar o delta.

### 3) Visão Categoria (Tab 3)
Objetivo: ver agregação por **fornecedor** dentro de uma **categoria** (com filtro opcional de fornecedor).  
- Agrega `Fat_Nov` e `Marg_Val_Nov` por fornecedor.

---

## Requisitos

- Python 3.11+ (para rodar local)
- Docker + Docker Compose (para rodar em container)

Dependências Python principais:
- `dash`, `dash-bootstrap-components`, `dash-ag-grid`
- `pandas`, `numpy`, `openpyxl`

---

## Estrutura do projeto (sugestão)

```

.
├─ app.py
├─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
├─ data/
│  └─ base_simulador.xlsx
└─ core/
├─ config.py
├─ data_loader.py
├─ view_builders.py
├─ calculations.py
└─ formatters.py

````

---

## Base de dados (Excel)

O sistema lê um Excel definido por `BASE_SIMULADOR_PATH`.

### Caminho da planilha
Por padrão procura `base_simulador.xlsx` no diretório pai do `core/` (comportamento do `core.config`).  
Você pode sobrescrever via variável de ambiente:

- `BASE_SIMULADOR_PATH=/data/base_simulador.xlsx`

### Colunas importantes (mínimo esperado)
A aplicação tenta criar colunas ausentes com defaults, mas a base deve conter ao menos as colunas textuais de identificação e as numéricas principais:

**Identificação / filtros**
- `Fornecedor`, `Fabricante`, `Area`, `Produto`, `SKU`, `Curva_ABC`

**Preço / custo**
- `Preco_Mais_Recente`, `Custo_Mais_Recente`

**Vendas e margem**
- `Fat_Total_Trimestre`, `Valor_Margem_Total_Trimestre`, `Qtd_Media_Mensal`
- `Fat_{Mes}` e `Marg_Val_{Mes}` para meses em `LISTA_MESES_ANO`

**Concorrentes**
- configuradas em `core.config`:
  - `col_conc_1` (ex.: `"Preço PETZ"`)
  - `col_conc_2` (ex.: `"Preço PROCAMPO"`)

> Observação: se houver duplicidade em `Produto`, o loader cria uma chave única `Produto_Key = "Produto [SKU]"`.

---

## Como rodar local (Python)

1) Crie um ambiente virtual e instale dependências:
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
````

2. Defina o caminho da base (opcional):

```bash
export BASE_SIMULADOR_PATH=/caminho/para/base_simulador.xlsx
```

3. Rode o app:

```bash
python app.py
```

Acesse:

* [http://127.0.0.1:8050](http://127.0.0.1:8050)

> Para expor na rede (LAN/servidor), rode com `host="0.0.0.0"` no `app.run(...)`.

---

## Como rodar com Docker Compose

### 1) Prepare a base

Crie a pasta `data/` e coloque a planilha:

```bash
mkdir -p data
# copie sua base_simulador.xlsx para: ./data/base_simulador.xlsx
```

### 2) Suba o serviço

```bash
docker compose up --build
```

Acesse:

* [http://localhost:8050](http://localhost:8050)

### docker-compose.yml (referência)

```yaml
services:
  simulador-web:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: simulador-web
    ports:
      - "8050:8050"
    environment:
      - BASE_SIMULADOR_PATH=/data/base_simulador.xlsx
      - PYTHONUNBUFFERED=1
    volumes:
      - ./data:/data:ro
    restart: unless-stopped
```

> Importante: dentro de container, o Dash precisa escutar em `0.0.0.0`.
> Ajuste o final do `app.py` para:
>
> ```python
> app.run(debug=True, host="0.0.0.0", port=8050)
> ```

---

## Como usar (fluxo)

1. Selecione **Fornecedor** (e opcionalmente **Fabricante** e **Categoria**)
2. Ajuste:

   * **Meta Fin. (%)** (Tab 1) — margem alvo padrão para custo-alvo
   * **Delta Padrão (%)** (Tab 2) — delta padrão vs concorrente
3. Clique **Recalcular**
4. Interações:

   * **Clique** numa célula: atualiza o painel **Detalhes (Inteligência Temporal)**
   * **Duplo clique** numa linha:

     * Tab 1: abre **modal financeiro**
     * Tab 2: abre **modal mercado**
5. Clique **Exportar** para baixar a base filtrada com simulações aplicadas.

---

## Regras de simulação

As simulações ficam em `dcc.Store(storage_type="session")`:

* `store-sim["manual"]` (Tab 1)

  * `{"ativa": True, "preco": <float>, "margem": <float>}`

* `store-sim["conc"]` (Tab 2)

  * `{"ativa": True, "delta": <float>}`

**Tab 1 (Financeiro)**

* Se simulação manual ativa: usa preço/margem informados
* Caso contrário: usa `menor_conc` (se existir) ou `preço atual` + `meta_t1`

**Tab 2 (Mercado)**

* Se simulação ativa: usa `delta` informado
* Caso contrário: usa `meta_t2`

---

## Troubleshooting

### “Não abre modal no duplo clique”

* Garanta que o grid tem `getRowId="params.data.id"`
* O evento de double click não carrega `data` completo; o app resolve buscando a linha via `rowId` em `rowData`.

### “SchemaTypeValidationError / retorno None”

* Callbacks com múltiplos Outputs **sempre** precisam retornar uma tupla/lista com o mesmo tamanho de Outputs (use `no_update` no fallback).

### “Planilha não encontrada”

* Confirme `BASE_SIMULADOR_PATH` (local ou Docker) e o volume `./data:/data:ro`.