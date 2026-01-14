# main.py

import argparse
import logging
import pandas as pd

# (O resto das importações permanece o mesmo)
# 1. INICIALIZAÇÃO CRÍTICA
try:
    from logger_config import configurar_logger
    configurar_logger()
    from inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except Exception as e:
    logging.critical("Falha gravíssima na inicialização: %s", e)
    exit(1)

# 2. IMPORTAÇÕES DO PROJETO
from extracao import obter_dados_brutos
from processamento.enriquecimento import enriquecer_orcado_com_cc
from processamento.validacao import (
    preparar_dados_para_validacao,
    aplicar_mapa_correcoes,
    carregar_mapa_correcoes,
    salvar_mapa_correcoes
)
from processamento.correcao_chaves import iniciar_correcao_interativa_chaves
from comunicacao.carregamento import carregar_dataframe_para_sql
from database import get_conexao
from config import CONFIG


logger = logging.getLogger(__name__)


def run_pipeline(args: argparse.Namespace) -> None:
    """
    Executa o fluxo completo: extração, validação, correção, enriquecimento e salvamento.
    """
    df_orcado_raw, df_cc_raw = obter_dados_brutos()

    if df_orcado_raw.empty or df_cc_raw.empty:
        logger.error("Dados brutos do Orçado ou CC estão vazios. Abortando.")
        return

    # 1. PREPARAÇÃO (Agora cria a coluna ANO_FOTOGRAFIA)
    chaves_base = ['PROJETO', 'ACAO', 'UNIDADE']
    df_orcado = preparar_dados_para_validacao(df_orcado_raw, chaves_base, incluir_ano_na_chave=True)
    df_cc = preparar_dados_para_validacao(df_cc_raw, chaves_base, incluir_ano_na_chave=True)

    # 2. APLICAÇÃO DE CORREÇÕES EXISTENTES
    mapa_correcoes = carregar_mapa_correcoes()
    df_orcado_corrigido = aplicar_mapa_correcoes(df_orcado, mapa_correcoes)

    # 3. ENRIQUECIMENTO
    df_enriquecido = enriquecer_orcado_com_cc(df_orcado_corrigido, df_cc)

    # 4. TRATAMENTO DE FALHAS (se houver)
    tratar_falhas_de_enriquecimento(df_enriquecido, df_cc, args)

    # 5. PASSO FINAL: SALVAR O RESULTADO
    logger.info("Iniciando o salvamento da tabela enriquecida no servidor FINANCA...")
    
    NOME_TABELA_FINAL = "ORCADO_ENRIQUECIDO_COM_CC"
    
    try:
        engine_financa = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        
        logger.info("Organizando colunas para a tabela final...")

        # *** PASSO 2: RESTAURANDO O ANO ORIGINAL ***
        # Se a coluna 'ANO_FOTOGRAFIA' existe, use-a para restaurar o ano original.
        if 'ANO_FOTOGRAFIA' in df_enriquecido.columns:
            logger.info("Restaurando o ano original da fotografia...")
            df_enriquecido['ANO'] = df_enriquecido['ANO_FOTOGRAFIA']

        colunas_finais_ordenadas = [
            'ANO', 'MES', 'PROJETO', 'ACAO', 'UNIDADE', 'CODCCUSTO',
            'Valor_Ajustado', 'Descricao_PPA', 'Codigo_Natureza_Orcamentaria',
            'Descricao_Natureza_Orcamentaria', 'DTUNIDADE', 'DTPROJETO', 'DTACAO'
        ]
        
        df_para_salvar = df_enriquecido.reindex(columns=colunas_finais_ordenadas)

        carregar_dataframe_para_sql(df_para_salvar, NOME_TABELA_FINAL, engine_financa)
        
        logger.info(
            "SUCESSO! A tabela '%s' foi salva no banco de dados '%s'.",
            NOME_TABELA_FINAL, CONFIG.conexoes["FINANCA_SQL"].banco
        )
    except Exception:
        logger.exception("FALHA CRÍTICA AO SALVAR A TABELA FINAL NO BANCO DE DADOS.")


# (O resto do arquivo main.py permanece o mesmo)
def tratar_falhas_de_enriquecimento(
    df_enriquecido: pd.DataFrame, df_referencia_cc: pd.DataFrame, args: argparse.Namespace
) -> None:
    df_falhas = df_enriquecido[df_enriquecido['CODCCUSTO'].isnull()]
    if df_falhas.empty:
        logger.info("Etapa de verificação: Nenhuma falha de enriquecimento encontrada.")
        return
    chaves_com_falha = set(df_falhas['CHAVE_CONCAT_original'])
    logger.warning("\n--- ATENÇÃO: %d COMBINAÇÕES ÚNICAS NÃO FORAM ENRIQUECIDAS ---", len(chaves_com_falha))
    if args.modo_interativo:
        iniciar_correcao_interativa_chaves(chaves_com_falha, df_referencia_cc)
        logger.info("Processo de correção finalizado. O mapa de correções foi atualizado.")
    else:
        print("\nPara corrigir as falhas restantes, execute com a flag: python main.py --modo-interativo")

def main() -> None:
    parser = argparse.ArgumentParser(description="Robô de Enriquecimento de Dados.")
    parser.add_argument("--modo-interativo", action="store_true", help="Ativa o modo interativo.")
    args = parser.parse_args()
    logger.info("--- INICIANDO ROBÔ DE ENRIQUECIMENTO DE DADOS ---")
    try:
        run_pipeline(args)
    except Exception:
        logger.exception("--- ERRO CRÍTICO E INESPERADO NA EXECUÇÃO ---")
    finally:
        logger.info("--- FIM DA EXECUÇÃO ---")

if __name__ == "__main__":
    main()
