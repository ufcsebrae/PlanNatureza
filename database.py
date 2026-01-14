# database.py
import logging
from typing import Union

from pyadomd import Pyadomd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Importa a classe de configuração para tipagem e acesso seguro.
from config import DbConfig

logger = logging.getLogger(__name__)

# O tipo de união representa qualquer uma das possíveis conexões que a fábrica pode retornar.
Conexao = Union[Engine, Pyadomd]


def get_conexao(config: DbConfig) -> Conexao:
    """
    Cria e retorna um objeto de conexão de banco de dados com base na configuração fornecida.
    """
    destino_log = config.banco or config.caminho
    logger.info("Criando conexão do tipo '%s' para '%s'...", config.tipo, destino_log)

    if config.tipo == "sql":
        # CORREÇÃO: Adicionado 'Encrypt=no' para compatibilidade com o Driver 18.
        conn_str = (
            "mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{{config.driver}}};"
            f"SERVER={config.servidor};"
            f"DATABASE={config.banco};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"  # Adicione esta linha
        )
        return create_engine(conn_str, fast_executemany=True)

    elif config.tipo == "olap":
        conn_str = (
            f"Provider={config.provider};"
            f"Data Source={config.data_source};"
            f"Initial Catalog={config.catalog};"
            "Trusted_Connection=yes;"
        )
        try:
            conn = Pyadomd(conn_str)
            conn.open()
            logger.info("Conexão OLAP aberta com sucesso.")
            return conn
        except Exception as e:
            logger.exception("Falha ao abrir conexão OLAP.")
            raise e

    elif config.tipo == "sqlite":
        conn_str = f"sqlite:///{config.caminho}"
        return create_engine(conn_str)

    else:
        raise ValueError(f"Tipo de conexão desconhecido: '{config.tipo}'.")
