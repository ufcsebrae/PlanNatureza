# database.py
import logging
from typing import Union

from pyadomd import Pyadomd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

from config import DbConfig

logger = logging.getLogger(__name__)

Conexao = Union[Engine, Pyadomd]


def get_conexao(config: DbConfig) -> Conexao:
    """
    Cria e retorna um objeto de conexão de banco de dados com base na configuração.
    """
    destino_log = config.banco or config.caminho
    logger.info("Criando conexão do tipo '%s' para '%s'...", config.tipo, destino_log)

    if config.tipo == "sql":
        # Abordagem moderna e segura para criar a URL de conexão
        conn_url = URL.create(
            "mssql+pyodbc",
            query={
                "odbc_connect": (
                    f"DRIVER={{{config.driver}}};"
                    f"SERVER={config.servidor};"
                    f"DATABASE={config.banco};"
                    "Trusted_Connection=yes;"
                    "Encrypt=no;"  # Mantém a correção de compatibilidade
                )
            },
        )
        return create_engine(conn_url, fast_executemany=True)

    elif config.tipo == "olap":
        conn_str_olap = (
            f"Provider={config.provider};"
            f"Data Source={config.data_source};"
            f"Initial Catalog={config.catalog};"
            "Trusted_Connection=yes;"
        )
        try:
            conn = Pyadomd(conn_str_olap)
            conn.open()
            logger.info("Conexão OLAP aberta com sucesso.")
            return conn
        except Exception as e:
            logger.exception("Falha ao abrir conexão OLAP.")
            raise e

    elif config.tipo == "sqlite":
        # Garante que o caminho seja absoluto para evitar ambiguidades
        conn_str = f"sqlite:///{config.caminho.resolve()}"
        return create_engine(conn_str)

    else:
        raise ValueError(f"Tipo de conexão desconhecido: '{config.tipo}'.")
