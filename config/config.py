# config/config.py (VERSÃO REATORADA)
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

@dataclass
class DbConfig:
    """Define a estrutura para configurações de conexão, compatível com database.py."""
    tipo: str
    servidor: Optional[str] = None
    banco: Optional[str] = None
    driver: Optional[str] = None
    provider: Optional[str] = None
    data_source: Optional[str] = None
    catalog: Optional[str] = None
    caminho: Optional[Path] = None

class Config:
    """Classe principal para centralizar as configurações do projeto."""
    def __init__(self):
        db_server_financa = os.getenv("DB_SERVER_FINANCA")
        db_database_financa = os.getenv("DB_DATABASE_FINANCA")
        db_server_hub = os.getenv("DB_SERVER_HUB")
        db_database_hub = os.getenv("DB_DATABASE_HUB")
        adomd_dll_path_str = os.getenv("ADOMD_DLL_PATH")

        if not db_server_financa or not db_server_hub:
            raise ValueError("Uma das variáveis de servidor ('DB_SERVER_FINANCA', 'DB_SERVER_HUB') não foi encontrada no .env.")

        self.base_dir = Path(__file__).resolve().parent.parent
        self.paths = self._Paths(self.base_dir)
        self.paths.cache_dir.mkdir(parents=True, exist_ok=True)

        # --- ALTERAÇÃO APLICADA ---
        # Converte o caminho da DLL de string para Path, se existir
        self.adomd_dll_path: Optional[Path] = Path(adomd_dll_path_str) if adomd_dll_path_str else None

        self.conexoes = {
            "FINANCA_SQL": DbConfig(
                tipo='sql',
                servidor=db_server_financa,
                banco=db_database_financa,
                driver="ODBC Driver 18 for SQL Server"
            ),
            "HubDados": DbConfig(
                tipo='sql',
                servidor=db_server_hub,
                banco=db_database_hub,
                driver="ODBC Driver 18 for SQL Server"
            ),
            "CacheDB": DbConfig(
                tipo='sqlite',
                caminho=self.paths.cache_db
            ),
        }

    class _Paths:
        """Classe interna que APENAS define os caminhos do projeto."""
        def __init__(self, base_dir):
            self.base_dir = base_dir
            self.config_dir = self.base_dir / "config"
            self.logs_dir = self.base_dir / "logs"
            self.docs_dir = self.base_dir / "docs"
            self.drivers = self.base_dir / "drivers"
            self.templates_dir = self.base_dir / "templates"
            self.relatorios_excel_dir = self.docs_dir / "excel"
            self.queries_dir = self.base_dir / "queries"
            self.dados_dir = self.base_dir / "dados"
            self.cache_dir = self.base_dir / "cache"
            self.cache_db = self.cache_dir / "local_cache.db"
            self.query_nacional = self.queries_dir / "nacional.sql"
            self.query_cc = self.queries_dir / "cc.sql"
            self.gerentes_csv = self.dados_dir / "gerentes.csv"
            self.unidade_csv = self.dados_dir / "UNIDADE.CSV"
            self.natureza_csv = self.dados_dir / "NATUREZA.csv"
            self.mapa_correcoes = self.dados_dir / "mapa_correcoes.json"

# --- Instância única da configuração ---
CONFIG = Config()

# ==============================================================================
#  DESIGN SYSTEM
# ==============================================================================
CORES = {
    'brand_primary': '#4F46E5', 'project_exclusive': '#3B82F6', 'project_shared': '#10B981',
    'alert_danger': '#EF4444', 'alert_warning': '#D97706', 'trend_total': '#4338CA',
}
