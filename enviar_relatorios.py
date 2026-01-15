# enviar_relatorios.py
"""
Script responsável pelo ENVIO dos relatórios de performance via Outlook.
Ele automatiza a criação do rascunho do e-mail com resumo, screenshot e
link, deixando-o pronto para ser enviado ou revisado no Outlook.
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import urllib3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import win32com.client as win32

# --- Supressão de Avisos Específicos ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Inicialização Crítica ---
try:
    from logger_config import configurar_logger
    configurar_logger("envio_relatorios.log")
    from inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError, Exception) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha gravíssima na inicialização do envio: %s", e, exc_info=True)
    sys.exit(1)

logger = logging.getLogger(__name__)

# --- Importações do Projeto ---
try:
    from config import CONFIG
    from database import get_conexao
except ImportError as e:
    logger.critical("Erro de importação: %s. Verifique config.py e database.py.", e)
    sys.exit(1)


# --- Funções Auxiliares de Lógica e Apresentação ---

def carregar_gerentes_do_csv() -> Dict[str, Dict[str, str]]:
    caminho_csv = CONFIG.paths.gerentes_csv
    if not caminho_csv.exists():
        logger.error(f"Arquivo de gerentes não encontrado: {caminho_csv}")
        return {}
    try:
        df_gerentes = pd.read_csv(caminho_csv)
        colunas_necessarias = {'unidade', 'email', 'gerente'}
        if not colunas_necessarias.issubset(df_gerentes.columns):
            raise ValueError(f"O arquivo CSV deve conter as colunas: {', '.join(colunas_necessarias)}")
        gerentes_dict = {
            str(row['unidade']).upper().strip(): {'email': str(row['email']), 'gerente': str(row['gerente'])}
            for _, row in df_gerentes.iterrows()
        }
        logger.info(f"{len(gerentes_dict)} gerentes carregados de '{caminho_csv.name}'.")
        return gerentes_dict
    except Exception as e:
        logger.exception(f"Falha ao processar o arquivo CSV de gerentes: {e}")
        return {}

def enviar_via_outlook(destinatario: str, assunto: str, corpo_html: str, anexo_path: Optional[Path] = None) -> bool:
    """
    Cria e exibe um e-mail no Outlook, pronto para ser enviado.
    """
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        mail.Subject = assunto
        mail.HTMLBody = corpo_html

        if anexo_path and anexo_path.exists():
            # O anexo é incorporado como imagem no corpo (inline)
            attachment = mail.Attachments.Add(str(anexo_path.resolve()))
            attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", "screenshot")
            # Substitui a referência da imagem no HTML para usar o Content-ID
            corpo_html = corpo_html.replace('src="cid:screenshot"', f'src="cid:{anexo_path.name}"')
            mail.HTMLBody = corpo_html
        
        # .Display() abre o e-mail para revisão.
        # .Send() enviaria diretamente, mas Display() é mais seguro para começar.
        mail.Display()
        
        logger.info(f"E-mail para {destinatario} criado e exibido no Outlook para revisão.")
        return True
    except Exception as e:
        logger.exception(f"Falha ao criar e-mail no Outlook para {destinatario}.")
        return False


def capturar_screenshot_relatorio(html_path: Path) -> Optional[Path]:
    """Abre um arquivo HTML local em um navegador headless e tira um screenshot."""
    logger.info(f"Capturando screenshot de '{html_path.name}'...")
    screenshot_path = CONFIG.paths.relatorios_dir / f"screenshot_{html_path.stem}.png"
    
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--hide-scrollbars")
    
    os.environ['WDM_SSL_VERIFY'] = '0'
    os.environ['WDM_LOCAL'] = '1'

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"file:///{html_path.resolve()}")
        time.sleep(3)
        driver.save_screenshot(str(screenshot_path))
        driver.quit()
        
        logger.info(f"Screenshot salvo em: '{screenshot_path}'")
        return screenshot_path
    except Exception as e:
        logger.error(f"Falha ao capturar screenshot para '{html_path.name}': {e}")
        return None

def gerar_resumo_executivo(unidade: str, df_base: pd.DataFrame) -> str:
    """Analisa os dados de uma unidade e gera 3 bullet points com insights."""
    logger.info(f"Gerando resumo executivo para a unidade: {unidade}")
    df_unidade = df_base[df_base['nm_unidade_padronizada'] == unidade]
    if df_unidade.empty:
        return "<li>Não foram encontrados dados suficientes para gerar um resumo.</li>"

    total_planejado = df_unidade['vl_planejado'].sum()
    total_executado = df_unidade['vl_executado'].sum()
    perc_execucao = (total_executado / total_planejado * 100) if total_planejado > 0 else 0
    resumo1 = f"A unidade atingiu <strong>{perc_execucao:.1f}%</strong> da sua meta orçamentária total (Executado: R$ {total_executado:,.2f} de R$ {total_planejado:,.2f})."

    projeto_maior_execucao = df_unidade.groupby('nm_projeto')['vl_executado'].sum().idxmax()
    valor_maior_execucao = df_unidade.groupby('nm_projeto')['vl_executado'].sum().max()
    resumo2 = f"O projeto de maior destaque em execução financeira é <strong>'{projeto_maior_execucao}'</strong>, com R$ {valor_maior_execucao:,.2f} já realizados."
    
    df_com_orcamento = df_unidade[df_unidade['vl_planejado'] > 0].copy()
    if not df_com_orcamento.empty:
        df_com_orcamento['perc_exec'] = df_com_orcamento['vl_executado'] / df_com_orcamento['vl_planejado']
        projeto_menor_execucao = df_com_orcamento.groupby('nm_projeto')['perc_exec'].mean().idxmin()
        perc_menor_execucao = df_com_orcamento.groupby('nm_projeto')['perc_exec'].mean().min() * 100
        resumo3 = f"Ponto de atenção: o projeto <strong>'{projeto_menor_execucao}'</strong> apresenta a menor taxa de execução ({perc_menor_execucao:.1f}%), indicando uma oportunidade de análise."
    else:
        resumo3 = "Todos os projetos com orçamento tiveram alguma execução."

    return f"<ul><li>{resumo1}</li><li>{resumo2}</li><li>{resumo3}</li></ul>"


def main() -> None:
    parser = argparse.ArgumentParser(description="Envia relatórios de performance via Outlook.")
    args = parser.parse_args() # Argumentos não são mais necessários, mas mantemos a estrutura.

    relatorios_dir = CONFIG.paths.relatorios_dir
    relatorios_gerados = sorted(list(relatorios_dir.glob("*.html")))

    if not relatorios_gerados:
        logger.warning("Nenhum relatório HTML encontrado na pasta 'relatorios/'. Execute 'gerar_relatorio.py' primeiro.")
        sys.exit(0)

    gerentes = carregar_gerentes_do_csv()
    engine_db = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    
    print("\n--- Relatórios Disponíveis para Envio ---")
    for i, path in enumerate(relatorios_gerados, 1):
        print(f"  {i:2d}) {path.name}")
    print("  all) Enviar todos os relatórios")
    print("-" * 45)

    escolha_str = input("Escolha os relatórios para enviar (por número, ex: 1, 3), 'all' ou enter para sair: ").strip()

    if not escolha_str:
        logger.info("Nenhum relatório selecionado. Encerrando.")
        sys.exit(0)
    
    relatorios_para_enviar = []
    if escolha_str.lower() == 'all':
        relatorios_para_enviar = relatorios_gerados
    else:
        try:
            indices_escolhidos = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            for idx in indices_escolhidos:
                if 0 <= idx < len(relatorios_gerados):
                    relatorios_para_enviar.append(relatorios_gerados[idx])
                else:
                    logger.warning(f"Número {idx + 1} é inválido e será ignorado.")
        except ValueError:
            logger.error("Entrada inválida. Encerrando.")
            sys.exit(1)

    if not relatorios_para_enviar:
        logger.info("Nenhum relatório válido selecionado. Encerrando.")
        sys.exit(0)

    logger.info("Carregando dados base para gerar resumos...")
    PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
    ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
    sql_query = f"SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2 WHERE nm_ppa = '{PPA_FILTRO}' AND nm_ano = {ANO_FILTRO}"
    df_base_total = pd.read_sql(sql_query, engine_db)
    df_base_total['nm_unidade_padronizada'] = df_base_total['nm_unidade'].str.upper().str.replace('SP - ', '', regex=False).str.strip()

    for html_path in relatorios_para_enviar:
        unidade_nome = html_path.stem.replace("relatorio_", "").replace("_", " ")
        logger.info(f"\nPreparando envio para a unidade: {unidade_nome}")
        
        info_gerente = gerentes.get(unidade_nome)
        
        if info_gerente:
            print(f"\nEncontrado em gerentes.csv: Unidade '{unidade_nome}', Gerente: '{info_gerente['gerente']}', Email: '{info_gerente['email']}'")
            confirmacao = input("Confirmar criação do e-mail para este destinatário? (S/n/e para editar): ").lower().strip()
            if confirmacao == 'n':
                logger.info(f"Envio para {unidade_nome} pulado pelo usuário.")
                continue
            elif confirmacao == 'e':
                info_gerente['gerente'] = input(f"  Novo nome do gerente (anterior: {info_gerente['gerente']}): ")
                info_gerente['email'] = input(f"  Novo email (anterior: {info_gerente['email']}): ")
        else:
            logger.warning(f"Nenhum gerente encontrado para a unidade '{unidade_nome}' em gerentes.csv.")
            if input("Deseja inserir os dados manualmente para este envio? (s/n): ").lower() == 's':
                info_gerente = {'gerente': input("  Nome do gerente: "), 'email': input("  Email do gerente: ")}
            else:
                logger.info(f"Envio para {unidade_nome} abortado.")
                continue

        resumo_executivo = gerar_resumo_executivo(unidade_nome, df_base_total)
        screenshot_path = capturar_screenshot_relatorio(html_path)
        
        github_pages_link = f"https://ufcsebrae.github.io/PlanNatureza/relatorios/{html_path.name}"
        
        corpo_email = f"""
        <html>
            <body>
                <p>Prezado(a) {info_gerente['gerente']},</p>
                <p>Abaixo estão os destaques da performance orçamentária da sua unidade:</p>
                {resumo_executivo}
                <p>Uma prévia visual do seu painel está abaixo.</p>
                <p><strong><a href="{github_pages_link}">Para ver o detalhamento interativo e filtrar por setor, acesse o painel completo aqui.</a></strong></p>
                <br>
                <img src="cid:screenshot">
                <br>
                <p>Atenciosamente,</p>
                <p>Equipe de Dados</p>
            </body>
        </html>
        """
        
        enviar_via_outlook(
            destinatario=info_gerente['email'],
            assunto=f"Performance Orçamentária: {unidade_nome}",
            corpo_html=corpo_email,
            anexo_path=screenshot_path
        )
        
        if screenshot_path and screenshot_path.exists():
            os.remove(screenshot_path)
            logger.info(f"Screenshot temporário '{screenshot_path.name}' removido.")

if __name__ == "__main__":
    main()
