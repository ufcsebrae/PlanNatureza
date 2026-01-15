# -*- coding: utf-8 -*-
"""
gerar_relatorio.py

Script dedicado à GERAÇÃO de relatórios de performance orçamentária em HTML.
Ele pode ser executado de forma interativa ou via linha de comando para
gerar relatórios para uma ou todas as unidades disponíveis na base de dados.
"""
import argparse
import logging
import sys
import os
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

# --- Inicialização Crítica ---
try:
    from logger_config import configurar_logger
    configurar_logger("geracao_relatorios.log")
    from inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError, Exception) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha gravíssima na inicialização do relatório: %s", e, exc_info=True)
    sys.exit(1)

logger = logging.getLogger(__name__)

# --- Importações do Projeto ---
try:
    from config import CONFIG
    from database import get_conexao
except ImportError as e:
    logger.critical("Erro de importação: %s. Verifique config.py e database.py.", e)
    sys.exit(1)

# --- Funções de Formatação e Lógica de Geração ---

def formatar_numero_kpi(num):
    if num is None or pd.isna(num): return "N/A"
    if abs(num) >= 1_000_000: return f"R$ {num/1_000_000:,.2f} M"
    if abs(num) >= 1_000: return f"R$ {num/1_000:,.1f} k"
    return f"R$ {num:,.2f}"

def formatar_valor_tabela(num):
    if num is None or pd.isna(num) or num == 0: return "-"
    if abs(num) >= 1_000_000: return f"{num/1_000_000:,.1f}M"
    if abs(num) >= 1_000: return f"{num/1_000:,.1f}k"
    return f"{num:,.0f}"

pio.templates.default = "plotly_white"
pd.options.display.float_format = '{:,.2f}'.format

def obter_unidades_disponiveis(engine_db: Any) -> List[str]:
    logger.info("Consultando unidades de negócio disponíveis no banco de dados...")
    PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
    ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
    query_unidades = f"SELECT DISTINCT nm_unidade FROM dbo.vw_Analise_Planejado_vs_Executado_v2 WHERE nm_ppa = '{PPA_FILTRO}' AND nm_ano = {ANO_FILTRO}"
    try:
        df_unidades = pd.read_sql(query_unidades, engine_db)
        if df_unidades.empty:
            logger.warning("Nenhuma unidade encontrada na base de dados para os filtros atuais.")
            return []
        unidades = sorted(df_unidades['nm_unidade'].str.upper().str.replace('SP - ', '', regex=False).str.strip().unique())
        logger.info(f"{len(unidades)} unidades encontradas na base de dados.")
        return unidades
    except Exception as e:
        logger.exception(f"Falha ao consultar as unidades disponíveis: {e}")
        return []

def selecionar_unidades_interativamente(unidades_disponiveis: List[str]) -> List[str]:
    if not unidades_disponiveis:
        return []
    print("\n--- Unidades Disponíveis para Geração de Relatório ---")
    for i, unidade in enumerate(unidades_disponiveis, 1):
        print(f"  {i:2d}) {unidade}")
    print("  all) Gerar para todas as unidades")
    print("-" * 55)
    while True:
        escolha_str = input("Escolha os números das unidades (separados por vírgula), 'all' para todas, ou enter para sair: ").strip()
        if not escolha_str:
            logger.info("Operação cancelada pelo usuário.")
            return []
        if escolha_str.lower() == 'all':
            return unidades_disponiveis
        try:
            indices_escolhidos = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            unidades_selecionadas = []
            valido = True
            for idx in indices_escolhidos:
                if 0 <= idx < len(unidades_disponiveis):
                    unidades_selecionadas.append(unidades_disponiveis[idx])
                else:
                    print(f"Erro: O número {idx + 1} está fora do intervalo válido (1-{len(unidades_disponiveis)}).")
                    valido = False
            if valido:
                return unidades_selecionadas
        except ValueError:
            print("Entrada inválida. Por favor, digite números separados por vírgula (ex: 1, 3, 5) ou 'all'.")

def gerar_relatorio_para_unidade(
    unidade_alvo: str,
    df_base: pd.DataFrame
) -> None:
    logger.info(f"Iniciando geração de relatório para a unidade: {unidade_alvo}")
    df_unidade_filtrada = df_base[df_base['nm_unidade_padronizada'] == unidade_alvo].copy()
    if df_unidade_filtrada.empty: 
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_alvo}' no DataFrame base. Pulando.")
        return

    df_exclusivos_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Compartilhado'].copy()
    total_planejado_unidade = df_unidade_filtrada['vl_planejado'].sum()
    total_executado_unidade = df_unidade_filtrada['vl_executado'].sum()
    perc_total = (total_executado_unidade / total_planejado_unidade * 100) if total_planejado_unidade > 0 else 0
    kpi_total_projetos = df_unidade_filtrada['nm_projeto'].nunique()
    kpi_total_acoes = df_unidade_filtrada.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
    kpi_total_planejado_str = formatar_numero_kpi(total_planejado_unidade)
    kpi_total_executado_str = formatar_numero_kpi(total_executado_unidade)
    planejado_exclusivos = df_exclusivos_unidade['vl_planejado'].sum()
    executado_exclusivos = df_exclusivos_unidade['vl_executado'].sum()
    perc_exclusivos = (executado_exclusivos / planejado_exclusivos * 100) if planejado_exclusivos > 0 else 0
    kpi_exclusivos_projetos = df_exclusivos_unidade['nm_projeto'].nunique()
    kpi_exclusivos_acoes = df_exclusivos_unidade.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
    kpi_exclusivos_planejado_str = formatar_numero_kpi(planejado_exclusivos)
    kpi_exclusivos_executado_str = formatar_numero_kpi(executado_exclusivos)
    planejado_compartilhados = df_compartilhados_unidade['vl_planejado'].sum()
    executado_compartilhados = df_compartilhados_unidade['vl_executado'].sum()
    perc_compartilhados = (executado_compartilhados / planejado_compartilhados * 100) if planejado_compartilhados > 0 else 0
    kpi_compartilhados_projetos = df_compartilhados_unidade['nm_projeto'].nunique()
    kpi_compartilhados_acoes = df_compartilhados_unidade.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
    kpi_compartilhados_planejado_str = formatar_numero_kpi(planejado_compartilhados)
    kpi_compartilhados_executado_str = formatar_numero_kpi(executado_compartilhados)
    logger.info(f"Cálculos de KPIs concluídos para {unidade_alvo}.")

    logger.info(f"Gerando gráficos para {unidade_alvo}...")
    perc_contrib_exclusivos = (executado_exclusivos / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
    perc_contrib_compartilhados = (executado_compartilhados / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
    texto_contribuicao = f"% do Tot.: <br> Exclusivos: {perc_contrib_exclusivos:.2f}% | Compartilhados: {perc_contrib_compartilhados:.2f}%"

    fig_gauge_total = go.Figure(go.Indicator(mode="gauge+number", value=perc_total, title={'text': "Execução Total"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "#004085"}}))
    fig_gauge_total.add_annotation(x=0.5, y=-0.18, text=texto_contribuicao, showarrow=False, font=dict(size=12, color="#6c757d"))
    fig_gauge_total.update_layout(height=250, margin=dict(t=50, b=30))
    fig_gauge_exclusivos = go.Figure(go.Indicator(mode="gauge+number", value=perc_exclusivos, title={'text': "Projetos Exclusivos"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "green"}}))
    fig_gauge_exclusivos.update_layout(height=250, margin=dict(t=50, b=20))
    fig_gauge_compartilhados = go.Figure(go.Indicator(mode="gauge+number", value=perc_compartilhados, title={'text': "Projetos Compartilhados"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "goldenrod"}}))
    fig_gauge_compartilhados.update_layout(height=250, margin=dict(t=50, b=20))
    
    HOVER_TEMPLATE_VALOR = '<b>%{data.name}</b><br>Valor: R$ %{y:,.2f}<extra></extra>'
    execucao_mensal_exclusivos = df_exclusivos_unidade.groupby('nm_mes_num', observed=False).agg(Planejado=('vl_planejado', 'sum'), Executado=('vl_executado', 'sum')).reset_index()
    fig_line_valor_exclusivos = go.Figure()
    fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['nm_mes_num'], y=execucao_mensal_exclusivos['Planejado'], mode='lines', name='Planejado', line=dict(color='lightblue', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['nm_mes_num'], y=execucao_mensal_exclusivos['Executado'], mode='lines', name='Executado', line=dict(color='green'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_exclusivos.update_layout(title='Valores Mensais - Exclusivos', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

    execucao_mensal_compartilhados = df_compartilhados_unidade.groupby('nm_mes_num', observed=False).agg(Planejado=('vl_planejado', 'sum'), Executado=('vl_executado', 'sum')).reset_index()
    fig_line_valor_compartilhados = go.Figure()
    fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['nm_mes_num'], y=execucao_mensal_compartilhados['Planejado'], mode='lines', name='Planejado', line=dict(color='moccasin', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['nm_mes_num'], y=execucao_mensal_compartilhados['Executado'], mode='lines', name='Executado', line=dict(color='goldenrod'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_compartilhados.update_layout(title='Valores Mensais - Compartilhados', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

    def criar_grafico_execucao_trimestral(df, tipo_projeto, total_anual_planejado):
        cor_map = {'Exclusivos': 'Green', 'Compartilhados': 'goldenrod'}
        cor_map_light = {'Exclusivos': 'lightgreen', 'Compartilhados': 'moccasin'}
        if df.empty: return go.Figure().update_layout(title=f'Execução Percentual - {tipo_projeto} (Sem Dados)')
        dados_trimestrais = df.groupby('nm_trimestre', observed=False).agg(Planejado_T=('vl_planejado', 'sum'), Executado_T=('vl_executado', 'sum')).reset_index()
        dados_trimestrais['%_Exec_Trimestral'] = np.where(dados_trimestrais['Planejado_T'] > 0, (dados_trimestrais['Executado_T'] / dados_trimestrais['Planejado_T']) * 100, 0)
        dados_trimestrais['Exec_Acumulado'] = dados_trimestrais['Executado_T'].cumsum()
        dados_trimestrais['%_Acum_Total'] = np.where(total_anual_planejado > 0, (dados_trimestrais['Exec_Acumulado'] / total_anual_planejado) * 100, 0)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dados_trimestrais['nm_trimestre'], y=dados_trimestrais['%_Exec_Trimestral'], mode='lines+markers', name='Execução no Trimestre (%)', line=dict(color=cor_map_light[tipo_projeto], dash='dot'), hovertemplate='<b>%{x}</b><br>Exec no Trimestre: %{y:.1f}%<extra></extra>'))
        fig.add_trace(go.Scatter(x=dados_trimestrais['nm_trimestre'], y=dados_trimestrais['%_Acum_Total'], mode='lines+markers', name='Acumulado sobre Total Anual (%)', line=dict(color=cor_map[tipo_projeto]), hovertemplate='<b>%{x}</b><br>Acumulado sobre Total: %{y:.1f}%<extra></extra>'))
        fig.add_hline(y=100, line_width=2, line_dash="dash", line_color="gray", annotation_text="Meta 100%", annotation_position="bottom right")
        fig.update_layout(title=f'Execução Percentual - {tipo_projeto}', xaxis_title='Trimestre', yaxis_title='Percentual (%)', hovermode='x unified', yaxis=dict(ticksuffix='%'), legend=dict(yanchor="top", y=0.98, xanchor="left", x=0.01))
        return fig

    fig_perc_exclusivos = criar_grafico_execucao_trimestral(df_exclusivos_unidade, 'Exclusivos', planejado_exclusivos)
    fig_perc_compartilhados = criar_grafico_execucao_trimestral(df_compartilhados_unidade, 'Compartilhados', planejado_compartilhados)
    logger.info(f"Todos os gráficos foram gerados para {unidade_alvo}.\n")

    logger.info(f"Gerando tabelas analíticas para {unidade_alvo}...")
    def criar_tabela_analitica_trimestral(df, index_col, index_col_name):
        if df.empty: return pd.DataFrame({index_col_name: []})
        base = df.groupby([index_col, 'nm_trimestre'], observed=False).agg(Executado=('vl_executado', 'sum'), Planejado=('vl_planejado', 'sum')).reset_index()
        itens_unicos = sorted(df[index_col].unique())
        dados_brutos = []
        for item in itens_unicos:
            row_data = {'Projeto': item}
            total_exec_ano = 0
            for t in ['1T', '2T', '3T', '4T']:
                trim_data = base[(base[index_col] == item) & (base['nm_trimestre'] == t)]
                executado = trim_data['Executado'].iloc[0] if not trim_data.empty else 0
                planejado = trim_data['Planejado'].iloc[0] if not trim_data.empty else 0
                row_data[f'{t}_Exec'] = executado
                row_data[f'{t}_%'] = (executado / planejado * 100) if planejado > 0 else 0.0
                total_exec_ano += executado
            row_data['Total_Exec_Ano'] = total_exec_ano
            dados_brutos.append(row_data)

        tabela_bruta = pd.DataFrame(dados_brutos)
        total_geral = df.groupby('nm_trimestre', observed=False).agg(Executado=('vl_executado', 'sum'), Planejado=('vl_planejado', 'sum')).reset_index()
        total_row_data = {'Projeto': 'Total Geral'}
        total_exec_ano_geral = 0
        for t in ['1T', '2T', '3T', '4T']:
            trim_data = total_geral[total_geral['nm_trimestre'] == t]
            executado = trim_data['Executado'].iloc[0] if not trim_data.empty else 0
            planejado = trim_data['Planejado'].iloc[0] if not trim_data.empty else 0
            total_row_data[f'{t}_Exec'] = executado
            total_row_data[f'{t}_%'] = (executado / planejado * 100) if planejado > 0 else 0.0
            total_exec_ano_geral += executado
        total_row_data['Total_Exec_Ano'] = total_exec_ano_geral
        tabela_bruta = pd.concat([tabela_bruta, pd.DataFrame([total_row_data])], ignore_index=True)
        tabela_bruta['sort_order'] = np.where(tabela_bruta['Projeto'] == 'Total Geral', 1, 0)
        tabela_bruta = tabela_bruta.sort_values(by=['sort_order', 'Total_Exec_Ano'], ascending=[True, False]).drop(columns=['sort_order'])
        tabela_formatada = pd.DataFrame()
        tabela_formatada[index_col_name] = tabela_bruta['Projeto']
        for t in ['1T', '2T', '3T', '4T']:
            tabela_formatada[f'{t} Exec'] = tabela_bruta[f'{t}_Exec'].apply(formatar_valor_tabela)
            tabela_formatada[f'{t} %'] = tabela_bruta[f'{t}_%'].apply('{:.1f}%'.format)
        tabela_formatada['Total Exec Ano'] = tabela_bruta['Total_Exec_Ano'].apply(formatar_valor_tabela)
        return tabela_formatada
    
    tb_projetos_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'nm_projeto', 'Projeto')
    tb_projetos_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'nm_projeto', 'Projeto')
    tb_natureza_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'nm_desc_natureza_orcamentaria_origem', 'Natureza Orçamentária')
    tb_natureza_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'nm_desc_natureza_orcamentaria_origem', 'Natureza Orçamentária')
    logger.info(f"Tabelas analíticas criadas com sucesso para {unidade_alvo}.\n")

    logger.info(f"Montando e salvando o arquivo HTML para {unidade_alvo}...")
    html_gauge_total = pio.to_html(fig_gauge_total, full_html=False, include_plotlyjs='cdn')
    html_gauge_exclusivos = pio.to_html(fig_gauge_exclusivos, full_html=False, include_plotlyjs=False)
    html_gauge_compartilhados = pio.to_html(fig_gauge_compartilhados, full_html=False, include_plotlyjs=False)
    html_line_valor_exclusivos = pio.to_html(fig_line_valor_exclusivos, full_html=False, include_plotlyjs=False)
    html_line_valor_compartilhados = pio.to_html(fig_line_valor_compartilhados, full_html=False, include_plotlyjs=False)
    html_perc_exclusivos = fig_perc_exclusivos.to_html(full_html=False, include_plotlyjs=False)
    html_perc_compartilhados = fig_perc_compartilhados.to_html(full_html=False, include_plotlyjs=False)
    html_table_projetos_exc = tb_projetos_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
    html_table_projetos_comp = tb_projetos_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
    html_table_natureza_exc = tb_natureza_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
    html_table_natureza_comp = tb_natureza_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)

    html_string = f'''
    <html>
    <head>
        <title>Relatório: {unidade_alvo}</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 20px; background-color: #f8f9fa; }}
            h1, h2, h3, h4 {{ color: #004085; }}
            .card {{ margin-top: 20px; box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2); }}
            .card-header {{ background-color: #004085; color: white; }}
            .kpi-box, .kpi-values-box {{ text-align: center; padding: 10px 0; }}
            .kpi-box {{ border-top: 1px solid #dee2e6; }}
            .kpi-value {{ font-size: 1.8rem; font-weight: bold; color: #004085; }}
            .kpi-label {{ font-size: 0.9rem; color: #6c757d; text-transform: uppercase; }}
            .kpi-values-box {{ display: flex; justify-content: space-around; }}
            .kpi-financial-value {{ font-size: 1.5rem; font-weight: bold; }}
            .kpi-financial-label {{ font-size: 0.8rem; color: #6c757d; text-transform: uppercase; }}
            .green-text {{ color: #28a745; }}
            .blue-text {{ color: #007bff; }}
            .plotly-graph-div {{ min-height: 350px; }}
            .gauge-container .plotly-graph-div {{ min-height: 250px; }}
            .table-responsive {{ max-height: 450px; overflow-y: auto; }}
            .table th, .table td {{ text-align: left; vertical-align: middle; white-space: nowrap; padding: 0.4rem;}}
            .table td:not(:first-child), .table th:not(:first-child) {{ text-align: right; }}
        </style>
    </head>
    <body>
    <div class="container-fluid">
        <div class="text-center mt-4"><h1>Relatório de Performance Orçamentária</h1><h2 class="text-muted">{unidade_alvo}</h2></div><hr>
        <div class="card"><div class="card-header"><h3>Visão Geral da Execução</h3></div><div class="card-body gauge-container"><div class="row">
        <div class="col-lg-4">{html_gauge_total}<div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_total_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_total_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div><div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_total_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_total_acoes}</div><div class="kpi-label">Ações</div></div></div></div></div>
        <div class="col-lg-4">{html_gauge_exclusivos}<div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_exclusivos_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_exclusivos_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div><div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_exclusivos_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_exclusivos_acoes}</div><div class="kpi-label">Ações</div></div></div></div></div>
        <div class="col-lg-4">{html_gauge_compartilhados}<div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_compartilhados_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_compartilhados_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div><div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_compartilhados_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_compartilhados_acoes}</div><div class="kpi-label">Ações</div></div></div></div></div>
        </div></div></div>
        <div class="card"><div class="card-header"><h3>Evolução Mensal (Planejado vs. Executado)</h3></div><div class="card-body"><div class="row"><div class="col-lg-6">{html_line_valor_exclusivos}</div><div class="col-lg-6">{html_line_valor_compartilhados}</div></div></div></div>
        <div class="card"><div class="card-header"><h3>Execução Percentual Trimestral</h3></div><div class="card-body"><div class="row"><div class="col-lg-6">{html_perc_exclusivos}</div><div class="col-lg-6">{html_perc_compartilhados}</div></div></div></div>
        <div class="card"><div class="card-header"><h3>Análise Detalhada por Trimestre</h3></div><div class="card-body">
        <h4 class="mt-3">Projetos Exclusivos</h4><div class="table-responsive">{html_table_projetos_exc}</div>
        <h4 class="mt-4">Projetos Compartilhados</h4><div class="table-responsive">{html_table_projetos_comp}</div>
        <hr class="my-4">
        <h4 class="mt-4">Natureza Orçamentária (Projetos Exclusivos)</h4><div class="table-responsive">{html_table_natureza_exc}</div>
        <h4 class="mt-4">Natureza Orçamentária (Projetos Compartilhados)</h4><div class="table-responsive">{html_table_natureza_comp}</div>
        </div></div>
    </div>
    </body>
    </html>
    '''
    
    caminho_arquivo_html = CONFIG.paths.relatorios_dir / f"relatorio_{unidade_alvo.replace(' ', '_')}.html"
    caminho_arquivo_html.parent.mkdir(parents=True, exist_ok=True)
    
    with open(caminho_arquivo_html, 'w', encoding='utf-8') as f:
        f.write(html_string)

    logger.info(f"Relatório salvo com sucesso em: '{caminho_arquivo_html}'")

def main() -> None:
    parser = argparse.ArgumentParser(description="Gera relatórios de performance orçamentária.")
    parser.add_argument("--unidade", type=str, help="Gera o relatório para uma unidade de negócio específica.")
    parser.add_argument("--todas-unidades", action="store_true", help="Gera relatórios para todas as unidades disponíveis na base de dados.")
    args = parser.parse_args()

    CONFIG.paths.relatorios_dir.mkdir(exist_ok=True)
    CONFIG.paths.logs_dir.mkdir(exist_ok=True)
        
    logger.info("Estabelecendo conexão com o banco de dados...")
    try:
        engine_db = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        logger.info("Conexão estabelecida com sucesso.")
    except Exception as e:
        logger.critical(f"Falha crítica ao conectar ao banco de dados: {e}")
        sys.exit(1)

    unidades_a_gerar = []

    if args.unidade:
        unidades_a_gerar = [args.unidade.upper().strip()]
    elif args.todas_unidades:
        unidades_a_gerar = obter_unidades_disponiveis(engine_db)
    else:
        unidades_disponiveis = obter_unidades_disponiveis(engine_db)
        if unidades_disponiveis:
            unidades_a_gerar = selecionar_unidades_interativamente(unidades_disponiveis)

    if not unidades_a_gerar:
        logger.info("Nenhuma unidade selecionada para geração. Encerrando.")
    else:
        logger.info(f"Relatórios serão gerados para as seguintes unidades: {', '.join(unidades_a_gerar)}")
        
        PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
        ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
        sql_query = f"SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2 WHERE nm_ppa = '{PPA_FILTRO}' AND nm_ano = {ANO_FILTRO}"
        
        logger.info("Carregando dados base do banco de dados (uma única vez)...")
        df_base_total = pd.read_sql(sql_query, engine_db)
        
        # --- CORREÇÃO: A lógica de criação da coluna 'tipo_projeto' foi movida para cá ---
        df_base_total['nm_unidade_padronizada'] = df_base_total['nm_unidade'].str.upper().str.replace('SP - ', '', regex=False).str.strip()
        unidades_por_projeto = df_base_total.groupby('nm_projeto')['nm_unidade_padronizada'].nunique().reset_index()
        unidades_por_projeto.rename(columns={'nm_unidade_padronizada': 'contagem_unidades'}, inplace=True)
        unidades_por_projeto['tipo_projeto'] = np.where(unidades_por_projeto['contagem_unidades'] > 1, 'Compartilhado', 'Exclusivo')
        df_base_total = pd.merge(df_base_total, unidades_por_projeto[['nm_projeto', 'tipo_projeto']], on='nm_projeto', how='left')
        df_base_total['nm_mes_num'] = pd.to_numeric(df_base_total['nm_mes'], errors='coerce')
        mapa_trimestre_num = {1: '1T', 2: '1T', 3: '1T', 4: '2T', 5: '2T', 6: '2T', 7: '3T', 8: '3T', 9: '3T', 10: '4T', 11: '4T', 12: '4T'}
        df_base_total['nm_trimestre'] = df_base_total['nm_mes_num'].map(mapa_trimestre_num)
        trimestre_dtype = pd.CategoricalDtype(categories=['1T', '2T', '3T', '4T'], ordered=True)
        df_base_total['nm_trimestre'] = df_base_total['nm_trimestre'].astype(trimestre_dtype)
        # --- Fim da Correção ---

        for unidade in unidades_a_gerar:
            gerar_relatorio_para_unidade(unidade, df_base=df_base_total)

    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE RELATÓRIOS ---")

if __name__ == "__main__":
    main()

