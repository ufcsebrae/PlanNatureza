# gerar_relatorio.py
import argparse
import logging
import sys
import pandas as pd
import json
import plotly.graph_objects as go
import numpy as np 

try:
    from processamento.processamento_dados_base import obter_dados_processados, formatar_brl
    from comunicacao.enviar_relatorios import carregar_gerentes_do_csv
    from config.config import CONFIG
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro ao importar módulos necessários. Verifique as pastas 'processamento', 'comunicacao' e 'config'.")
    sys.exit(1)

logger = logging.getLogger(__name__)

def selecionar_unidades_interativamente(unidades_map: dict) -> list[str]:
    if not unidades_map: return []
    lista_exibicao = sorted([(v['nome_novo'], k) for k, v in unidades_map.items()])
    print("\n--- Unidades Disponíveis para Geração de Relatório ---")
    for i, (nome_novo, _) in enumerate(lista_exibicao, 1):
        print(f"  {i:2d}) {nome_novo}")
    print("  all) Gerar para todas as unidades")
    print("-" * 55)
    while True:
        escolha_str = input("Escolha os números (ex: 1, 3, 5), 'all' ou enter para sair: ").strip()
        if not escolha_str: return []
        if escolha_str.lower() == 'all':
            return [item[1] for item in lista_exibicao]
        try:
            indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            return [lista_exibicao[i][1] for i in indices if 0 <= i < len(lista_exibicao)]
        except (ValueError, IndexError):
            print("Entrada inválida.")

def gerar_relatorio_para_unidade(unidade_antiga: str, unidade_nova: str, df_base_total: pd.DataFrame):
    logger.info(f"Iniciando a geração do dashboard para: '{unidade_nova}' (dados de: '{unidade_antiga}')...")
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_antiga].copy()
    if df_unidade.empty:
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_antiga}'. Relatório não gerado.")
        return

    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados = df_unidade[df_unidade['tipo_projeto'] == 'Compartilhado'].copy()

    kpi_total_planejado = df_unidade['Valor_Planejado'].sum()
    kpi_exclusivo_planejado = df_exclusivos.get('Valor_Planejado', pd.Series([0])).sum()
    kpi_compartilhado_planejado = df_compartilhados.get('Valor_Planejado', pd.Series([0])).sum()

    kpi_dict = {
        "__UNIDADE_ALVO__": unidade_nova,
        "__KPI_TOTAL_PERC__": f"{(df_unidade['Valor_Executado'].sum() / kpi_total_planejado * 100) if kpi_total_planejado > 0 else 0:.1f}%",
        "__KPI_TOTAL_VALORES__": f"{formatar_brl(df_unidade['Valor_Executado'].sum())} de {formatar_brl(kpi_total_planejado)}",
        "__KPI_EXCLUSIVO_PERC__": f"{(df_exclusivos['Valor_Executado'].sum() / kpi_exclusivo_planejado * 100) if kpi_exclusivo_planejado > 0 else 0:.1f}%",
        "__KPI_EXCLUSIVO_VALORES__": f"{formatar_brl(df_exclusivos['Valor_Executado'].sum())} de {formatar_brl(kpi_exclusivo_planejado)}",
        "__KPI_COMPARTILHADO_PERC__": f"{(df_compartilhados['Valor_Executado'].sum() / kpi_compartilhado_planejado * 100) if kpi_compartilhado_planejado > 0 else 0:.1f}%",
        "__KPI_COMPARTILHADO_VALORES__": f"{formatar_brl(df_compartilhados['Valor_Executado'].sum())} de {formatar_brl(kpi_compartilhado_planejado)}",
    }
    
    # --- Início da Geração de Dados para Gráficos ---
    dados_graficos = {}
    inercia_html = '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados de projetos exclusivos para calcular a inércia.</div>'
    if not df_exclusivos.empty:
        
        def calcular_inercia(group):
            plan_group = group[group['Valor_Planejado'] > 0]
            if plan_group.empty: return np.nan
            
            mes_primeiro_plan = plan_group['MES'].min()

            gasto_group = group[group['Valor_Executado'] > 0]
            if gasto_group.empty: return np.nan
            
            mes_primeiro_gasto = gasto_group['MES'].min()
            
            return mes_primeiro_gasto - mes_primeiro_plan

        # Agrupa por uma combinação única para calcular a inércia de cada um
        df_inercia = df_exclusivos.groupby(['PROJETO', 'ACAO', 'NATUREZA_FINAL']).apply(calcular_inercia, include_groups=False)
        df_inercia = df_inercia.dropna().reset_index(name='inercia_meses')
        # Filtra apenas inércias positivas (atrasos reais)
        df_inercia = df_inercia[df_inercia['inercia_meses'] > 0]

        if not df_inercia.empty:
            # Encontra o registro de maior inércia para cada natureza
            idx_max_inercia = df_inercia.groupby('NATUREZA_FINAL')['inercia_meses'].idxmax()
            df_maior_inercia_por_natureza = df_inercia.loc[idx_max_inercia]
            
            # Ordena do maior para o menor
            df_maior_inercia_por_natureza = df_maior_inercia_por_natureza.sort_values(by='inercia_meses', ascending=False)
            
            # Prepara o texto para a tooltip
            hover_text = [f"<b>Projeto:</b> {row['PROJETO']}<br><b>Atraso:</b> {row['inercia_meses']:.0f} meses" for index, row in df_maior_inercia_por_natureza.iterrows()]

            fig_inercia = go.Figure()
            fig_inercia.add_trace(go.Bar(
                x=df_maior_inercia_por_natureza['inercia_meses'],
                y=df_maior_inercia_por_natureza['NATUREZA_FINAL'],
                orientation='h', # Gráfico de barras horizontais
                marker_color='#ef4444', # Cor vermelha para indicar alerta
                text=df_maior_inercia_por_natureza['inercia_meses'], # Mostra o número de meses na barra
                textposition='outside',
                hoverinfo='text',
                hovertext=hover_text
            ))

            fig_inercia.update_layout(
                title_text='Piores Atrasos (Inércia) por Natureza Orçamentária',
                xaxis_title_text='Meses de Atraso para Iniciar o Gasto',
                yaxis_title_text='Natureza Orçamentária',
                plot_bgcolor='white',
                yaxis=dict(autorange="reversed"), # A maior barra fica no topo
                margin=dict(l=250) # Espaço para nomes longos de natureza
            )
            inercia_html = fig_inercia.to_html(full_html=False)

    kpi_dict["__INERCIA_PLACEHOLDER__"] = inercia_html
    df_trend = df_unidade.groupby(['MES', 'tipo_projeto'])['Valor_Executado'].sum().unstack(fill_value=0).reindex(range(1, 13), fill_value=0)
    df_trend['Total'] = df_trend.sum(axis=1)
    dados_graficos['trend'] = {"labels": ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'], "executed_total": df_trend['Total'].tolist(), "executed_exclusivo": df_trend.get('Exclusivo', pd.Series([0]*12)).tolist(), "executed_compartilhado": df_trend.get('Compartilhado', pd.Series([0]*12)).tolist()}

    
    def criar_dados_treemap_com_projetos(df_source):
        if df_source is None or df_source.empty: return {}
        df_agg = df_source.groupby(['NATUREZA_FINAL', 'PROJETO'])['Valor_Executado'].sum().reset_index()
        df_agg = df_agg[df_agg['Valor_Executado'] > 0]
        if df_agg.empty: return {}
        def format_projetos(group):
            top_projetos = group.nlargest(3, 'Valor_Executado')
            return '<br>'.join([f"- {row.PROJETO} ({formatar_brl(row.Valor_Executado)})" for _, row in top_projetos.iterrows()])
        projetos_por_natureza = df_agg.groupby('NATUREZA_FINAL').apply(format_projetos, include_groups=False).to_dict()
        df_natureza_sum = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().reset_index()
        return {'labels': df_natureza_sum['NATUREZA_FINAL'].tolist(), 'parents': [""] * len(df_natureza_sum), 'values': df_natureza_sum['Valor_Executado'].tolist(), 'projetos': df_natureza_sum['NATUREZA_FINAL'].map(projetos_por_natureza).fillna('').tolist()}

    dados_graficos['treemap_exclusivo'] = criar_dados_treemap_com_projetos(df_exclusivos)
    dados_graficos['treemap_compartilhado'] = criar_dados_treemap_com_projetos(df_compartilhados)

    df_unidade['saldo_nao_executado'] = df_unidade['Valor_Planejado'].fillna(0) - df_unidade['Valor_Executado'].fillna(0)
    saldos_por_projeto_unidade = df_unidade.groupby(['PROJETO', 'tipo_projeto'])['saldo_nao_executado'].sum().reset_index()
    df_top_ocioso_agg = saldos_por_projeto_unidade[saldos_por_projeto_unidade['saldo_nao_executado'] > 0].nlargest(7, 'saldo_nao_executado')
    df_pivot_ocioso = df_top_ocioso_agg.pivot_table(index='PROJETO', columns='tipo_projeto', values='saldo_nao_executado', fill_value=0)
    if not df_top_ocioso_agg.empty:
        df_pivot_ocioso = df_pivot_ocioso.reindex(df_top_ocioso_agg['PROJETO'])
        def formatar_acoes(group):
            acoes_agrupadas = group.groupby('ACAO')['saldo_nao_executado'].sum()
            top_acoes = acoes_agrupadas[acoes_agrupadas > 0].nlargest(3)
            return [f"- {acao}: {formatar_brl(saldo)}" for acao, saldo in top_acoes.items()]
        df_detalhes_ocioso = df_unidade[df_unidade['PROJETO'].isin(df_top_ocioso_agg['PROJETO'])]
        detalhes_por_projeto = df_detalhes_ocioso.groupby('PROJETO').apply(formatar_acoes, include_groups=False).reindex(df_pivot_ocioso.index)
        detalhes_exclusivo, detalhes_compartilhado = [], []
        for projeto, row in df_pivot_ocioso.iterrows():
            detalhe_formatado = detalhes_por_projeto.get(projeto, [])
            tipo_projeto_real = df_top_ocioso_agg.loc[df_top_ocioso_agg['PROJETO'] == projeto, 'tipo_projeto'].iloc[0]
            if tipo_projeto_real == 'Exclusivo':
                detalhes_exclusivo.append(detalhe_formatado)
                detalhes_compartilhado.append([])
            else:
                detalhes_compartilhado.append(detalhe_formatado)
                detalhes_exclusivo.append([])
    else:
        detalhes_exclusivo, detalhes_compartilhado = [], []

    dados_graficos['idle_budget'] = { "labels": df_pivot_ocioso.index.tolist(), "values_exclusivo": df_pivot_ocioso.get('Exclusivo', pd.Series(0, index=df_pivot_ocioso.index)).fillna(0).tolist(), "values_compartilhado": df_pivot_ocioso.get('Compartilhado', pd.Series(0, index=df_pivot_ocioso.index)).fillna(0).tolist(), "detalhes_exclusivo": detalhes_exclusivo, "detalhes_compartilhado": detalhes_compartilhado }
    
    def criar_dados_exec_sem_plan(df_source):
        if df_source is None or df_source.empty: return {}
        df_agg_total = df_source.groupby(['NATUREZA_FINAL', 'PROJETO']).agg( Valor_Planejado_Total=('Valor_Planejado', 'sum'), Valor_Executado_Total=('Valor_Executado', 'sum') ).reset_index()
        df_sem_plan_agg = df_agg_total[ (df_agg_total['Valor_Planejado_Total'] <= 0) & (df_agg_total['Valor_Executado_Total'] > 0) ].copy()
        if df_sem_plan_agg.empty: return {}
        def formatar_projetos_sp(group):
            top_projetos = group.nlargest(3, 'Valor_Executado_Total')
            return [f"- {row.PROJETO}: {formatar_brl(row.Valor_Executado_Total)}" for _, row in top_projetos.iterrows()]
        df_sum = df_sem_plan_agg.groupby('NATUREZA_FINAL')['Valor_Executado_Total'].sum().sort_values(ascending=False)
        if df_sum.empty: return {}
        detalhes_projetos_series = df_sem_plan_agg.groupby('NATUREZA_FINAL').apply(formatar_projetos_sp, include_groups=False).reindex(df_sum.index)
        detalhes_projetos = [item if isinstance(item, list) else [] for item in detalhes_projetos_series]
        return { "labels": df_sum.index.tolist(), "values": df_sum.values.tolist(), "projetos": detalhes_projetos }

    dados_graficos['unplanned_exclusivo'] = criar_dados_exec_sem_plan(df_exclusivos)
    dados_graficos['unplanned_compartilhado'] = criar_dados_exec_sem_plan(df_compartilhados)
    
    logger.info(f"[{unidade_nova}] Dados para os gráficos agregados.")
    
    # GERAÇÃO DO SUNBURST (PLOTLY)
 # --- Geração dos Gráficos Plotly ---
    sunburst_html = '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados de projetos exclusivos para exibir.</div>'
    heatmap_html = '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados de projetos exclusivos para exibir.</div>'

    if not df_exclusivos.empty:
        df_sun = df_exclusivos.groupby(['PROJETO', 'NATUREZA_FINAL']).agg({'Valor_Planejado': 'sum', 'Valor_Executado': 'sum'}).reset_index()
        df_sun = df_sun[df_sun['Valor_Planejado'] > 0]
        if not df_sun.empty:
            df_sun['perc_exec'] = (df_sun['Valor_Executado'] / df_sun['Valor_Planejado']) * 100
            
            # SUNBURST
            fig_sun = go.Figure()
            cores_projeto = df_sun.groupby('PROJETO').apply(lambda x: (x['Valor_Executado'].sum() / x['Valor_Planejado'].sum())*100 if x['Valor_Planejado'].sum() > 0 else 0, include_groups=False).tolist()
            fig_sun.add_trace(go.Sunburst(labels=df_sun['NATUREZA_FINAL'].tolist() + df_sun['PROJETO'].unique().tolist(), parents=df_sun['PROJETO'].tolist() + [""] * df_sun['PROJETO'].nunique(), values=df_sun['Valor_Planejado'].tolist() + df_sun.groupby('PROJETO')['Valor_Planejado'].sum().tolist(), branchvalues='total', marker=dict(colors=df_sun['perc_exec'].tolist() + cores_projeto, colorscale='RdYlGn', cmin=0, cmax=120, colorbar=dict(title='% Executado')), hovertemplate='<b>%{label}</b><br>Planejado: %{value:,.2f}<br>Execução: %{color:.1f}%<extra></extra>'))
            fig_sun.update_layout(margin=dict(t=10, l=10, r=10, b=10))
            sunburst_html = fig_sun.to_html(full_html=False)

            # HEATMAP
            pivot_df = df_sun.pivot_table(index='PROJETO', columns='NATUREZA_FINAL', values='perc_exec', fill_value=None)
            if not pivot_df.empty:
                
                # --- ALTERAÇÃO PRINCIPAL: Cálculo de Altura Dinâmica ---
                num_projetos = len(pivot_df.index)
                # Damos 35px para cada projeto, com um mínimo de 400px de altura total
                dynamic_height = max(400, num_projetos * 35)
                
                fig_heat = go.Figure(data=go.Heatmap(z=pivot_df.values, x=pivot_df.columns, y=pivot_df.index, colorscale='RdYlGn', zmin=0, zmid=80, zmax=120, hovertemplate='Projeto: %{y}<br>Natureza: %{x}<br>Execução: %{z:.1f}%<extra></extra>', xgap=1, ygap=1))
                
                # Usa a altura dinâmica no layout
                fig_heat.update_layout(
                    yaxis_nticks=num_projetos, 
                    xaxis_tickangle=-45, 
                    height=dynamic_height, 
                    margin=dict(l=250) # Margem esquerda para nomes longos de projeto
                )
                heatmap_html = fig_heat.to_html(full_html=False)

    kpi_dict["__SUNBURST_PLACEHOLDER__"] = sunburst_html
    kpi_dict["__HEATMAP_PLACEHOLDER__"] = heatmap_html
    
    try:
        template_path = CONFIG.paths.templates_dir / "dashboard_template.html"
        template_string = template_path.read_text(encoding='utf-8')
        final_html = template_string
        for key, value in kpi_dict.items():
            final_html = final_html.replace(key, str(value))
        
        json_string = json.dumps(dados_graficos, indent=None, ensure_ascii=False)
        final_html = final_html.replace('__JSON_DATA_PLACEHOLDER__', json_string)
        
        output_sanitized_name = unidade_nova.replace(' ', '_').replace('/', '_')
        output_filename = f"dashboard_{output_sanitized_name}.html"
        output_path = CONFIG.paths.docs_dir / output_filename
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        logger.info(f"Dashboard para '{unidade_nova}' salvo com sucesso em: '{output_path}'")
    except Exception as e:
        logger.exception(f"Ocorreu um erro ao gerar o HTML para '{unidade_nova}': {e}")

def main():
    parser = argparse.ArgumentParser(description="Gera dashboards de performance orçamentária por unidade.")
    parser.add_argument("--unidade", type=str, help="Gera o dashboard para uma unidade específica (usar o nome novo).")
    parser.add_argument("--todas", action="store_true", help="Gera relatórios para todas as unidades disponíveis.")
    args = parser.parse_args()

    CONFIG.paths.docs_dir.mkdir(parents=True, exist_ok=True)
    df_base_total = obter_dados_processados()
    if df_base_total is None or df_base_total.empty:
        logger.error("A base de dados não pôde ser carregada. Encerrando.")
        sys.exit(1)
        
    gerentes_info = carregar_gerentes_do_csv()
    if not gerentes_info:
        logger.error("Arquivo de gerentes não pôde ser carregado. Encerrando.")
        sys.exit(1)

    unidades_antigas_disponiveis = df_base_total['UNIDADE_FINAL'].unique()
    
    unidades_map = {}
    for nome_antigo in unidades_antigas_disponiveis:
        nome_antigo_tratado = nome_antigo.replace("UNIDADE ", "").strip()
        info = gerentes_info.get(nome_antigo_tratado.upper())
        if info:
            unidades_map[nome_antigo] = info
        else:
            unidades_map[nome_antigo] = {'nome_novo': nome_antigo_tratado}

    unidades_a_gerar_chaves = []
    if args.unidade:
        nome_novo_arg = args.unidade.upper()
        chave_encontrada = next((k for k, v in unidades_map.items() if v['nome_novo'].upper() == nome_novo_arg), None)
        if chave_encontrada:
            unidades_a_gerar_chaves = [chave_encontrada]
        else:
            logger.error(f"Unidade '{args.unidade}' não encontrada no mapeamento.")
    elif args.todas:
        unidades_a_gerar_chaves = list(unidades_map.keys())
    else:
        unidades_a_gerar_chaves = selecionar_unidades_interativamente(unidades_map)

    if unidades_a_gerar_chaves:
        logger.info(f"Gerando dashboards para: {', '.join([unidades_map[k]['nome_novo'] for k in unidades_a_gerar_chaves])}")
        for chave_antiga in unidades_a_gerar_chaves:
            nome_novo = unidades_map[chave_antiga]['nome_novo']
            gerar_relatorio_para_unidade(chave_antiga, nome_novo, df_base_total)
    else:
        logger.info("Nenhuma unidade selecionada. Encerrando.")
    
    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE DASHBOARD ---")

if __name__ == "__main__":
    main()
