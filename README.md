# Relatório de Performance Orçamentária

Este projeto é um robô de análise de dados em Python que gera relatórios interativos em HTML sobre a performance orçamentária de unidades de negócio.

## Visão Geral

O script principal, `gerar_relatorio.py`, realiza as seguintes tarefas:
1.  Conecta-se a um banco de dados SQL Server para buscar dados de planejamento e execução a partir de uma `VIEW` pré-processada.
2.  Processa e padroniza os dados, classificando projetos como "Exclusivos" ou "Compartilhados".
3.  Gera um relatório HTML interativo para uma unidade de negócio específica.
4.  O relatório inclui uma visão geral com KPIs, gráficos de evolução mensal e tabelas analíticas trimestrais.

## Estrutura do Projeto

├── drivers/
│ └── Microsoft.AnalysisServices.AdomdClient.dll (Adicionar manualmente)
├── queries/
│ ├── cc.sql
│ └── nacional.sql
├── processamento/
│ ├── correcao_chaves.py
│ ├── correcao_dados.py
│ ├── enriquecimento.py
│ └── validacao.py
├── .env (Arquivo local para segredos)
├── .env.example (Template do arquivo .env)
├── .gitignore
├── config.py
├── database.py
├── extracao.py
├── gerar_relatorio.py (Script principal para os relatórios HTML)
├── inicializacao.py
├── logger_config.py
├── main.py (Script original do robô de enriquecimento)
├── mapa_correcoes.json
├── README.md
├── requirements.txt
└── utils.py


## Instalação

Siga os passos abaixo para configurar o ambiente de desenvolvimento.

**Pré-requisitos:**
*   Python 3.9+
*   Acesso aos bancos de dados `FINANCA` e `HubDados`.

**Passos:**

1.  **Clone o repositório:**
    ```bash
    git clone <url-do-seu-repositorio>
    cd <nome-da-pasta>
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    # Windows
    python -m venv .venv
    .\.venv\Scripts\activate

    # macOS/Linux
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure as variáveis de ambiente:**
    *   Crie uma cópia do arquivo `.env.example` e renomeie-a para `.env`.
    *   Preencha as variáveis com os dados de conexão corretos para seus servidores de banco de dados.

5.  **Adicione os Drivers:**
    *   Certifique-se de que a pasta `drivers` existe no diretório raiz.
    *   Coloque o arquivo `Microsoft.AnalysisServices.AdomdClient.dll` dentro da pasta `drivers`.

## Uso

Para gerar um relatório HTML, execute o script `gerar_relatorio.py`:

```bash
python gerar_relatorio.py
