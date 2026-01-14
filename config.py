# config.py
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Final

from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env no diretório do projeto.
load_dotenv()


@dataclass(frozen=True)
class PathsConfig:
    """Centraliza todos os caminhos de arquivos usados no projeto."""

    base_dir: Path = Path(__file__).parent
    query_nacional: Path = base_dir / "queries" / "nacional.sql"
    query_cc: Path = base_dir / "queries" / "cc.sql"
    cache_db: Path = base_dir / "cache_dados.db"
    mapa_correcoes: Path = base_dir / "mapa_correcoes.json"
    drivers: Path = base_dir / "drivers"


@dataclass(frozen=True)
class DbConfig:
    """Define a estrutura para uma configuração de banco de dados."""

    tipo: str
    driver: str | None = None
    servidor: str | None = None
    banco: str | None = None
    caminho: Path | None = None
    # Adicionado campos específicos para OLAP
    provider: str | None = None
    data_source: str | None = None
    catalog: str | None = None
    trusted_connection: bool = True


@dataclass(frozen=True)
class AppConfig:
    """Agrega todas as configurações da aplicação."""

    paths: PathsConfig
    conexoes: Dict[str, DbConfig]


def get_config() -> AppConfig:
    """
    Constrói e retorna o objeto de configuração principal da aplicação.
    Valida a presença de variáveis de ambiente essenciais.
    """
    paths = PathsConfig()

    conexoes = {
        "HubDados": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_HUB"),
            banco=os.getenv("DB_DATABASE_HUB"),
            # CORREÇÃO: Alterado para a versão 18 do driver ODBC.
            driver="ODBC Driver 18 for SQL Server",
        ),
        "FINANCA_SQL": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_FINANCA"),
            banco=os.getenv("DB_DATABASE_FINANCA"),
            # CORREÇÃO: Alterado para a versão 18 do driver ODBC.
            driver="ODBC Driver 18 for SQL Server",
        ),
        "CacheDB": DbConfig(tipo="sqlite", caminho=paths.cache_db),
        "OLAP": DbConfig(  # Inclui a configuração OLAP
            tipo="olap",
            provider=os.getenv("OLAP_PROVIDER"),
            data_source=os.getenv("OLAP_SOURCE"),
            catalog=os.getenv("OLAP_CATALOG"),
        ),
    }

    # Validação crítica para garantir que as variáveis de ambiente foram carregadas
    if not conexoes["HubDados"].servidor or not conexoes["FINANCA_SQL"].servidor:
        raise ValueError(
            "Erro crítico: Variáveis de ambiente essenciais para conexões SQL (DB_SERVER_HUB, DB_SERVER_FINANCA) "
            "não foram definidas no arquivo .env."
        )

    # Validação para OLAP (se definido)
    if conexoes["OLAP"].tipo == "olap" and (
        not conexoes["OLAP"].provider
        or not conexoes["OLAP"].data_source
        or not conexoes["OLAP"].catalog
    ):
        raise ValueError(
            "Erro crítico: Variáveis de ambiente para conexão OLAP (OLAP_PROVIDER, OLAP_SOURCE, OLAP_CATALOG) "
            "não foram completamente definidas no arquivo .env."
        )

    return AppConfig(paths=paths, conexoes=conexoes)


# Instância única de configuração para ser importada em outros módulos.
CONFIG: Final[AppConfig] = get_config()
