# processamento/extracao.py (VERSÃO REATORADA)
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

# Importações do projeto
from config.config import CONFIG
from config.database import get_conexao
from utils.utils import carregar_script_sql

logger = logging.getLogger(__name__)

# Constantes para os nomes das tabelas no cache
TABELA_ORCADO_CACHE = "orcado_nacional_raw"
TABELA_CC_CACHE = "cc_estrutura_raw"


def obter_dados_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtém os DataFrames BRUTOS do Orçado e da Estrutura de CC, otimizando
    a criação de conexões de cache.
    """
    caminho_cache: Path = CONFIG.paths.cache_db

    if not caminho_cache.exists():
        logger.warning(
            "Arquivo de cache '%s' não encontrado. Executando queries ao vivo...",
            caminho_cache.name,
        )

        df_orcado = _buscar_dados_financa_sql_raw()
        df_cc = _buscar_dados_hubdados_sql_raw()
        
        # << ALTERAÇÃO 1: Otimiza a criação da conexão para salvar o cache >>
        logger.info("Salvando dados brutos no cache local...")
        engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
        _salvar_dados_no_cache(df_orcado, df_cc, engine_cache)

    else:
        logger.info(
            "Carregando dados brutos do cache local '%s'...", caminho_cache.name
        )
        try:
            # << ALTERAÇÃO 2: Cria a conexão com o cache UMA ÚNICA VEZ >>
            engine_cache = get_conexao(CONFIG.conexoes["CacheDB"])
            
            # E a reutiliza para carregar ambas as tabelas
            df_orcado = _carregar_dados_do_cache(TABELA_ORCADO_CACHE, engine_cache)
            df_cc = _carregar_dados_do_cache(TABELA_CC_CACHE, engine_cache)
            
            logger.info("Dados brutos carregados do cache com sucesso.")
        except Exception as e:
            logger.error(
                "Erro ao ler tabelas do cache: %s. O cache pode estar corrompido.", e
            )
            logger.warning("Excluindo cache e tentando buscar dados ao vivo.")
            caminho_cache.unlink()
            # Chama a si mesma recursivamente para tentar de novo
            return obter_dados_brutos()

    return df_orcado, df_cc


def _buscar_dados_financa_sql_raw() -> pd.DataFrame:
    """
    Busca dados brutos do Orçado (Nacional) via SQL Server FINANCA.
    """
    logger.info("Buscando dados brutos do Orçado (Nacional) via SQL Server...")
    query = carregar_script_sql(CONFIG.paths.query_nacional)
    engine = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
    
    try:
        df = pd.read_sql(query, engine)
        logger.info("Dados do Orçado (SQL) carregados com sucesso (%d linhas).", len(df))
        return df
    except Exception as e:
        logger.exception("ERRO CRÍTICO AO BUSCAR DADOS DO SQL FINANCA.")
        raise e


def _buscar_dados_hubdados_sql_raw() -> pd.DataFrame:
    """Busca dados brutos da estrutura de CC."""
    logger.info("Buscando dados brutos da estrutura de CC...")
    query = carregar_script_sql(CONFIG.paths.query_cc)
    engine = get_conexao(CONFIG.conexoes["HubDados"])
    return pd.read_sql(query, engine)


# << ALTERAÇÃO 3: A função agora aceita o 'engine' como parâmetro >>
def _salvar_dados_no_cache(df_orcado: pd.DataFrame, df_cc: pd.DataFrame, engine_cache: Engine) -> None:
    """Salva os DataFrames brutos no cache SQLite."""
    df_orcado.to_sql(
        TABELA_ORCADO_CACHE, engine_cache, if_exists="replace", index=False
    )
    df_cc.to_sql(TABELA_CC_CACHE, engine_cache, if_exists="replace", index=False)
    logger.info("Cache de dados brutos criado com sucesso.")


# << ALTERAÇÃO 4: A função agora aceita o 'engine' como parâmetro >>
def _carregar_dados_do_cache(tabela: str, engine_cache: Engine) -> pd.DataFrame:
    """Carrega uma tabela específica do cache SQLite usando uma conexão existente."""
    return pd.read_sql(tabela, engine_cache)

