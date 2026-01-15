# config.py
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Final

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PathsConfig:
    """Centraliza todos os caminhos de arquivos e diretórios usados no projeto."""

    base_dir: Path = Path(__file__).resolve().parent
    
    # Diretórios de Saída
    relatorios_dir: Path = base_dir / "docs" # Aponta para a pasta 'docs'
    logs_dir: Path = base_dir / "logs"

    # Diretório de Templates e Estilos
    templates_dir: Path = base_dir / "templates"
    static_dir: Path = relatorios_dir / "static" # Pasta para CSS dentro de 'docs'

    # Arquivos de Entrada e Dados
    query_nacional: Path = base_dir / "queries" / "nacional.sql"
    query_cc: Path = base_dir / "queries" / "cc.sql"
    cache_db: Path = base_dir / "cache_dados.db"
    mapa_correcoes: Path = base_dir / "mapa_correcoes.json"
    drivers: Path = base_dir / "drivers"
    gerentes_csv: Path = base_dir / "gerentes.csv"


@dataclass(frozen=True)
class DbConfig:
    """Define a estrutura para uma configuração de banco de dados."""

    tipo: str
    driver: str | None = None
    servidor: str | None = None
    banco: str | None = None
    caminho: Path | None = None
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

    driver_sql = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    conexoes = {
        "HubDados": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_HUB"),
            banco=os.getenv("DB_DATABASE_HUB"),
            driver=driver_sql,
        ),
        "FINANCA_SQL": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_FINANCA"),
            banco=os.getenv("DB_DATABASE_FINANCA"),
            driver=driver_sql,
        ),
        "CacheDB": DbConfig(tipo="sqlite", caminho=paths.cache_db),
        "OLAP": DbConfig(
            tipo="olap",
            provider=os.getenv("OLAP_PROVIDER"),
            data_source=os.getenv("OLAP_SOURCE"),
            catalog=os.getenv("OLAP_CATALOG"),
        ),
    }

    if not conexoes["HubDados"].servidor or not conexoes["FINANCA_SQL"].servidor:
        raise ValueError(
            "Erro crítico: Variáveis de ambiente para conexões SQL (DB_SERVER_HUB, DB_SERVER_FINANCA) "
            "não foram definidas no arquivo .env."
        )

    if conexoes["OLAP"].tipo == "olap" and not all(
        [conexoes["OLAP"].provider, conexoes["OLAP"].data_source, conexoes["OLAP"].catalog]
    ):
        raise ValueError(
            "Erro crítico: Variáveis para conexão OLAP (OLAP_PROVIDER, OLAP_SOURCE, OLAP_CATALOG) "
            "não foram completamente definidas no arquivo .env."
        )

    return AppConfig(paths=paths, conexoes=conexoes)


CONFIG: Final[AppConfig] = get_config()

