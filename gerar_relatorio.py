# -*- coding: utf-8 -*-
"""
gerar_relatorio.py

Script dedicado à GERAÇÃO de relatórios de performance orçamentária em HTML.
"""
import argparse
import logging
import sys
import os
from typing import Any, Dict, List

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


# --- Funções de Formatação e Lógica ---

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

# Retorna ao template padrão que funciona bem
pio.templates.default = "plotly_white"
pd.options.display.float_format = '{:,.2f}'.format

def obter_unidades_disponiveis(engine_db: Any) -> List[str]:
    logger.info("Consultando unidades de negócio disponíveis no banco de dados...")
    PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
    ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
    query_unidades = "SELECT DISTINCT UNIDADE FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
    params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
    try:
        df_unidades = pd.read_sql(query_unidades, engine_db, params=params)
        if df_unidades.empty:
            logger.warning("Nenhuma unidade encontrada na base de dados para os filtros atuais.")
            return []
        unidades = sorted(df_unidades['UNIDADE'].str.upper().str.replace('SP - ', '', regex=False).str.strip().unique())
        logger.info(f"{len(unidades)} unidades encontradas na base de dados.")
        return unidades
    except Exception as e:
        logger.exception(f"Falha ao consultar as unidades disponíveis: {e}")
        return []

def selecionar_unidades_interativamente(unidades_disponiveis: List[str]) -> List[str]:
    if not unidades_disponiveis: return []
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
            unidades_selecionadas = [unidades_disponiveis[idx] for idx in indices_escolhidos if 0 <= idx < len(unidades_disponiveis)]
            if len(unidades_selecionadas) < len(indices_escolhidos):
                logger.warning("Alguns números eram inválidos e foram ignorados.")
            return unidades_selecionadas
        except (ValueError, IndexError):
            print("Entrada inválida. Por favor, digite números separados por vírgula (ex: 1, 3, 5) ou 'all'.")

def criar_tabela_analitica_trimestral(df, index_col, index_col_name):
    if df.empty: return pd.DataFrame({index_col_name: []})
    base = df.groupby([index_col, 'nm_trimestre'], observed=False).agg(Executado=('Valor_Executado', 'sum'), Planejado=('Valor_Planejado', 'sum')).reset_index()
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
    total_geral = df.groupby('nm_trimestre', observed=False).agg(Executado=('Valor_Executado', 'sum'), Planejado=('Valor_Planejado', 'sum')).reset_index()
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


def gerar_relatorio_para_unidade(unidade_alvo: str, df_base: pd.DataFrame) -> None:
    """Gera o relatório HTML para uma unidade específica a partir de um DataFrame base."""
    logger.info(f"Iniciando geração de relatório para a unidade: {unidade_alvo}")

    df_unidade_filtrada = df_base[df_base['nm_unidade_padronizada'] == unidade_alvo].copy()
    if df_unidade_filtrada.empty: 
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_alvo}' no DataFrame base. Pulando.")
        return

    # Cálculos de KPIs
    df_exclusivos_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Compartilhado'].copy()

    total_planejado_unidade = df_unidade_filtrada['Valor_Planejado'].sum()
    total_executado_unidade = df_unidade_filtrada['Valor_Executado'].sum()
    perc_total = (total_executado_unidade / total_planejado_unidade * 100) if total_planejado_unidade > 0 else 0
    kpi_total_projetos = df_unidade_filtrada['PROJETO'].nunique()
    kpi_total_acoes = df_unidade_filtrada.groupby(['PROJETO', 'ACAO'], observed=True).ngroups
    kpi_total_planejado_str = formatar_numero_kpi(total_planejado_unidade)
    kpi_total_executado_str = formatar_numero_kpi(total_executado_unidade)
    
    planejado_exclusivos = df_exclusivos_unidade['Valor_Planejado'].sum()
    executado_exclusivos = df_exclusivos_unidade['Valor_Executado'].sum()
    perc_exclusivos = (executado_exclusivos / planejado_exclusivos * 100) if planejado_exclusivos > 0 else 0
    kpi_exclusivos_projetos = df_exclusivos_unidade['PROJETO'].nunique()
    kpi_exclusivos_acoes = df_exclusivos_unidade.groupby(['PROJETO', 'ACAO'], observed=True).ngroups
    kpi_exclusivos_planejado_str = formatar_numero_kpi(planejado_exclusivos)
    kpi_exclusivos_executado_str = formatar_numero_kpi(executado_exclusivos)

    planejado_compartilhados = df_compartilhados_unidade['Valor_Planejado'].sum()
    executado_compartilhados = df_compartilhados_unidade['Valor_Executado'].sum()
    perc_compartilhados = (executado_compartilhados / planejado_compartilhados * 100) if planejado_compartilhados > 0 else 0
    kpi_compartilhados_projetos = df_compartilhados_unidade['PROJETO'].nunique()
    kpi_compartilhados_acoes = df_compartilhados_unidade.groupby(['PROJETO', 'ACAO'], observed=True).ngroups
    kpi_compartilhados_planejado_str = formatar_numero_kpi(planejado_compartilhados)
    kpi_compartilhados_executado_str = formatar_numero_kpi(executado_compartilhados)
    logger.info(f"Cálculos de KPIs concluídos para {unidade_alvo}.")

    logger.info(f"Gerando gráficos para {unidade_alvo}...")
    
    perc_contrib_exclusivos = (executado_exclusivos / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
    perc_contrib_compartilhados = (executado_compartilhados / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
    texto_contribuicao = f"% do Tot.: <br> Exclusivos: {perc_contrib_exclusivos:.2f}% | Compartilhados: {perc_contrib_compartilhados:.2f}%"

    fig_gauge_total = go.Figure(go.Indicator(mode="gauge+number", value=perc_total, title={'text': "Execução Total"}, number={'valueformat': '.1f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "#004085"}}))
    fig_gauge_total.add_annotation(x=0.5, y=-0.18, text=texto_contribuicao, showarrow=False, font=dict(size=12, color="#6c757d"))
    fig_gauge_total.update_layout(height=250, margin=dict(t=50, b=30))
    fig_gauge_exclusivos = go.Figure(go.Indicator(mode="gauge+number", value=perc_exclusivos, title={'text': "Projetos Exclusivos"}, number={'valueformat': '.1f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "green"}}))
    fig_gauge_exclusivos.update_layout(height=250, margin=dict(t=50, b=20))
    fig_gauge_compartilhados = go.Figure(go.Indicator(mode="gauge+number", value=perc_compartilhados, title={'text': "Projetos Compartilhados"}, number={'valueformat': '.1f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "goldenrod"}}))
    fig_gauge_compartilhados.update_layout(height=250, margin=dict(t=50, b=20))
    
    HOVER_TEMPLATE_VALOR = '<b>%{data.name}</b><br>Valor: R$ %{y:,.2f}<extra></extra>'
    execucao_mensal_exclusivos = df_exclusivos_unidade.groupby('MES', observed=False).agg(Planejado=('Valor_Planejado', 'sum'), Executado=('Valor_Executado', 'sum')).reset_index()
    fig_line_valor_exclusivos = go.Figure()
    fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['MES'], y=execucao_mensal_exclusivos['Planejado'], mode='lines', name='Planejado', line=dict(color='lightblue', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['MES'], y=execucao_mensal_exclusivos['Executado'], mode='lines', name='Executado', line=dict(color='green'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_exclusivos.update_layout(title='Valores Mensais - Exclusivos', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

    execucao_mensal_compartilhados = df_compartilhados_unidade.groupby('MES', observed=False).agg(Planejado=('Valor_Planejado', 'sum'), Executado=('Valor_Executado', 'sum')).reset_index()
    fig_line_valor_compartilhados = go.Figure()
    fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['MES'], y=execucao_mensal_compartilhados['Planejado'], mode='lines', name='Planejado', line=dict(color='moccasin', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['MES'], y=execucao_mensal_compartilhados['Executado'], mode='lines', name='Executado', line=dict(color='goldenrod'), hovertemplate=HOVER_TEMPLATE_VALOR))
    fig_line_valor_compartilhados.update_layout(title='Valores Mensais - Compartilhados', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

    def criar_grafico_execucao_trimestral(df, tipo_projeto, total_anual_planejado):
        cor_map = {'Exclusivos': 'green', 'Compartilhados': 'goldenrod'}
        cor_map_light = {'Exclusivos': 'lightgreen', 'Compartilhados': 'moccasin'}
        if df.empty: return go.Figure().update_layout(title=f'Execução Percentual - {tipo_projeto} (Sem Dados)')
        dados_trimestrais = df.groupby('nm_trimestre', observed=False).agg(Planejado_T=('Valor_Planejado', 'sum'), Executado_T=('Valor_Executado', 'sum')).reset_index()
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
    tb_projetos_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'PROJETO', 'Projeto')
    tb_projetos_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'PROJETO', 'Projeto')
    tb_natureza_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'Descricao_Natureza_Orcamentaria', 'Natureza Orçamentária')
    tb_natureza_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'Descricao_Natureza_Orcamentaria', 'Natureza Orçamentária')
    logger.info(f"Tabelas analíticas criadas com sucesso para {unidade_alvo}.\n")

    logger.info(f"Montando e salvando o arquivo HTML para {unidade_alvo}...")
    try:
        template_path = CONFIG.paths.templates_dir / "template_relatorio.html"
        template_string = template_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        logger.error(f"Arquivo de template não encontrado em '{template_path}'.")
        return
    except Exception as e:
        logger.exception(f"Erro ao ler o arquivo de template: {e}")
        return

    contexto = {
        "unidade_alvo": unidade_alvo,
        "html_gauge_total": pio.to_html(fig_gauge_total, full_html=False, include_plotlyjs='cdn'),
        "html_gauge_exclusivos": pio.to_html(fig_gauge_exclusivos, full_html=False, include_plotlyjs=False),
        "html_gauge_compartilhados": pio.to_html(fig_gauge_compartilhados, full_html=False, include_plotlyjs=False),
        "kpi_total_planejado_str": kpi_total_planejado_str,
        "kpi_total_executado_str": kpi_total_executado_str,
        "kpi_total_projetos": kpi_total_projetos,
        "kpi_total_acoes": kpi_total_acoes,
        "kpi_exclusivos_planejado_str": kpi_exclusivos_planejado_str,
        "kpi_exclusivos_executado_str": kpi_exclusivos_executado_str,
        "kpi_exclusivos_projetos": kpi_exclusivos_projetos,
        "kpi_exclusivos_acoes": kpi_exclusivos_acoes,
        "kpi_compartilhados_planejado_str": kpi_compartilhados_planejado_str,
        "kpi_compartilhados_executado_str": kpi_compartilhados_executado_str,
        "kpi_compartilhados_projetos": kpi_compartilhados_projetos,
        "kpi_compartilhados_acoes": kpi_compartilhados_acoes,
        "html_line_valor_exclusivos": pio.to_html(fig_line_valor_exclusivos, full_html=False, include_plotlyjs=False),
        "html_line_valor_compartilhados": pio.to_html(fig_line_valor_compartilhados, full_html=False, include_plotlyjs=False),
        "html_perc_exclusivos": fig_perc_exclusivos.to_html(full_html=False, include_plotlyjs=False),
        "html_perc_compartilhados": fig_perc_compartilhados.to_html(full_html=False, include_plotlyjs=False),
        "html_table_projetos_exc": tb_projetos_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False),
        "html_table_projetos_comp": tb_projetos_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False),
        "html_table_natureza_exc": tb_natureza_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False),
        "html_table_natureza_comp": tb_natureza_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False),
    }

    html_final = template_string.format(**contexto)
    
    caminho_arquivo_html = CONFIG.paths.relatorios_dir / f"relatorio_{unidade_alvo.replace(' ', '_')}.html"
    caminho_arquivo_html.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho_arquivo_html, 'w', encoding='utf-8') as f:
        f.write(html_final)
    logger.info(f"Relatório salvo com sucesso em: '{caminho_arquivo_html}'")

def main() -> None:
    parser = argparse.ArgumentParser(description="Gera relatórios de performance orçamentária.")
    parser.add_argument("--unidade", type=str, help="Gera o relatório para uma unidade de negócio específica.")
    parser.add_argument("--todas-unidades", action="store_true", help="Gera relatórios para todas as unidades disponíveis na base de dados.")
    args = parser.parse_args()

    CONFIG.paths.relatorios_dir.mkdir(parents=True, exist_ok=True)
    CONFIG.paths.logs_dir.mkdir(parents=True, exist_ok=True)
        
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
        
        sql_query = "SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
        params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
        
        logger.info("Carregando dados base do banco de dados (uma única vez)...")
        df_base_total = pd.read_sql(sql_query, engine_db, params=params)
        
        # Como os nomes das colunas da função são simples, não precisamos mais renomear.
        df_base_total['nm_unidade_padronizada'] = df_base_total['UNIDADE'].str.upper().str.replace('SP - ', '', regex=False).str.strip()
        
        unidades_por_projeto = df_base_total.groupby('PROJETO')['nm_unidade_padronizada'].nunique().reset_index()
        unidades_por_projeto.rename(columns={'nm_unidade_padronizada': 'contagem_unidades'}, inplace=True)
        unidades_por_projeto['tipo_projeto'] = np.where(unidades_por_projeto['contagem_unidades'] > 1, 'Compartilhado', 'Exclusivo')
        df_base_total = pd.merge(df_base_total, unidades_por_projeto[['PROJETO', 'tipo_projeto']], on='PROJETO', how='left')
        
        df_base_total['nm_mes_num'] = pd.to_numeric(df_base_total['MES'], errors='coerce')
        mapa_trimestre_num = {1: '1T', 2: '1T', 3: '1T', 4: '2T', 5: '2T', 6: '2T', 7: '3T', 8: '3T', 9: '3T', 10: '4T', 11: '4T', 12: '4T'}
        df_base_total['nm_trimestre'] = df_base_total['nm_mes_num'].map(mapa_trimestre_num)
        trimestre_dtype = pd.CategoricalDtype(categories=['1T', '2T', '3T', '4T'], ordered=True)
        df_base_total['nm_trimestre'] = df_base_total['nm_trimestre'].astype(trimestre_dtype)

        for unidade in unidades_a_gerar:
            gerar_relatorio_para_unidade(unidade, df_base=df_base_total)

    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE RELATÓRIOS ---")

if __name__ == "__main__":
    main()
