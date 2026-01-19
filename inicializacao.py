# inicializacao.py
import logging
import clr
from pathlib import Path

# A importação do 'config' não é mais necessária para o caminho da DLL,
# mas pode ser mantida caso outros scripts a utilizem.
from config import CONFIG

logger = logging.getLogger(__name__)


def carregar_drivers_externos() -> None:
    """
    Localiza e carrega a DLL do AdomdClient necessária para a conexão OLAP,
    utilizando o caminho absoluto da instalação do On-premises data gateway.
    """
    logger.info("Inicializando... Carregando drivers externos.")

    # CAMINHO ALTERADO: Utilizando o caminho absoluto que você especificou.
    # O uso de 'r' antes da string (raw string) previne erros com as barras invertidas '\'.
    caminho_dll = Path(r"C:\Arquivos de Programas\On-premises data gateway\Microsoft.AnalysisServices.AdomdClient.dll")
    
    try:
        if not caminho_dll.exists():
            logger.error("DLL não encontrada no caminho especificado: %s", caminho_dll)
            logger.error("Por favor, verifique se o 'On-premises data gateway' da Microsoft está instalado corretamente neste local.")
            raise FileNotFoundError(f"DLL do gateway não encontrada: {caminho_dll}")

        # Adiciona a referência à DLL para que o CLR (Common Language Runtime) a reconheça.
        clr.AddReference(str(caminho_dll))
        logger.info("Driver AdomdClient carregado com sucesso a partir da pasta do gateway.")

    except Exception as e:
        logger.critical(
            "Falha crítica ao carregar a DLL a partir de '%s'.", caminho_dll
        )
        logger.critical(
            "Verifique se o caminho está correto e se o usuário que executa o script tem permissão para acessar a pasta 'Arquivos de Programas'."
        )
        raise e
