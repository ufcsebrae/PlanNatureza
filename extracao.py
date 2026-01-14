# extracao.py
import logging
from pathlib import Path

import pandas as pd

# Importações do projeto
from config import CONFIG
from database import get_conexao
from utils import carregar_script_sql

logger = logging.getLogger(__name__)

# Constantes para os nomes das tabelas no cache
TABELA_ORCADO_CACHE = "orcado_nacional_raw"
TABELA_CC_CACHE = "cc_estrutura_raw"


def obter_dados_brutos() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtém os DataFrames BRUTOS do Orçado e da Estrutura de CC.
    """
    caminho_cache: Path = CONFIG.paths.cache_db

    if not caminho_cache.exists():
        logger.warning(
            "Arquivo de cache '%s' não encontrado. Executando queries ao vivo...",
            caminho_cache.name,
        )

        # CORREÇÃO: Voltamos a chamar a função que busca dados do SQL Server normal.
        df_orcado = _buscar_dados_financa_sql_raw()
        df_cc = _buscar_dados_hubdados_sql_raw()
        _salvar_dados_no_cache(df_orcado, df_cc)

    else:
        logger.info(
            "Carregando dados brutos do cache local '%s'...", caminho_cache.name
        )
        try:
            df_orcado = _carregar_dados_do_cache(TABELA_ORCADO_CACHE)
            df_cc = _carregar_dados_do_cache(TABELA_CC_CACHE)
            logger.info("Dados brutos carregados do cache com sucesso.")
        except Exception as e:
            logger.error(
                "Erro ao ler tabelas do cache: %s. O cache pode estar corrompido.", e
            )
            logger.warning("Excluindo cache e tentando buscar dados ao vivo.")
            caminho_cache.unlink()
            return obter_dados_brutos()

    return df_orcado, df_cc


def _buscar_dados_financa_sql_raw() -> pd.DataFrame:
    """
    CORREÇÃO: Esta função agora é a correta e busca dados do SQL Server FINANCA.
    """
    logger.info("Buscando dados brutos do Orçado (Nacional) via SQL Server...")
    query = carregar_script_sql(CONFIG.paths.query_nacional)
    
    # Usa a configuração correta para SQL Server, conforme seu config.py
    engine_config = CONFIG.conexoes["FINANCA_SQL"]
    engine = get_conexao(engine_config)
    
    try:
        df = pd.read_sql(query, engine)
        logger.info("Dados do Orçado (SQL) carregados com sucesso (%d linhas).", len(df))
        return df
    except Exception as e:
        logger.exception("ERRO CRÍTICO AO BUSCAR DADOS DO SQL FINANCA.")
        raise e


def _buscar_dados_hubdados_sql_raw() -> pd.DataFrame:
    """Busca dados brutos da estrutura de CC (sem alterações)."""
    logger.info("Buscando dados brutos da estrutura de CC...")
    query = carregar_script_sql(CONFIG.paths.query_cc)
    engine_config = CONFIG.conexoes["HubDados"]
    engine = get_conexao(engine_config)
    return pd.read_sql(query, engine)


def _salvar_dados_no_cache(df_orcado: pd.DataFrame, df_cc: pd.DataFrame) -> None:
    """Salva os DataFrames brutos no cache SQLite (sem alterações)."""
    logger.info("Salvando dados brutos no cache local...")
    engine_cache_config = CONFIG.conexoes["CacheDB"]
    engine_cache = get_conexao(engine_cache_config)

    df_orcado.to_sql(
        TABELA_ORCADO_CACHE, engine_cache, if_exists="replace", index=False
    )
    df_cc.to_sql(TABELA_CC_CACHE, engine_cache, if_exists="replace", index=False)

    logger.info("Cache de dados brutos criado com sucesso.")


def _carregar_dados_do_cache(tabela: str) -> pd.DataFrame:
    """Carrega uma tabela específica do cache SQLite (sem alterações)."""
    engine_cache_config = CONFIG.conexoes["CacheDB"]
    engine_cache = get_conexao(engine_cache_config)
    return pd.read_sql(tabela, engine_cache)
