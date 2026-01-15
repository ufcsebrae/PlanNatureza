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
    relatorios_dir: Path = base_dir / "relatorios"
    logs_dir: Path = base_dir / "logs"

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

    # Validações críticas...
    if not conexoes["HubDados"].servidor or not conexoes["FINANCA_SQL"].servidor:
        raise ValueError("Erro: Variáveis de ambiente para conexões SQL não definidas.")

    if conexoes["OLAP"].tipo == "olap" and not all(
        [conexoes["OLAP"].provider, conexoes["OLAP"].data_source, conexoes["OLAP"].catalog]
    ):
        raise ValueError("Erro: Variáveis de ambiente para conexão OLAP não definidas.")

    return AppConfig(paths=paths, conexoes=conexoes)


CONFIG: Final[AppConfig] = get_config()

