# config/inicializacao.py (VERSÃO REATORADA)
import logging
import clr
from pathlib import Path

# Importa a instância centralizada da configuração
from .config import CONFIG

logger = logging.getLogger(__name__)


def carregar_drivers_externos() -> None:
    """
    Localiza e carrega a DLL do AdomdClient a partir do caminho definido
    na variável de ambiente 'ADOMD_DLL_PATH'.
    """
    logger.info("Inicializando... Carregando drivers externos.")

    # --- ALTERAÇÃO APLICADA ---
    # O caminho agora vem do objeto de configuração, que lê do .env
    caminho_dll = CONFIG.adomd_dll_path

    # --- NOVA VALIDAÇÃO ---
    # Verifica se o caminho foi definido no arquivo .env
    if not caminho_dll:
        logger.error("A variável de ambiente 'ADOMD_DLL_PATH' não foi definida no arquivo .env.")
        logger.error("Por favor, adicione a linha ADOMD_DLL_PATH='caminho/para/sua.dll' ao seu arquivo .env")
        raise ValueError("Caminho da DLL do AdomdClient não configurado.")

    try:
        if not caminho_dll.exists():
            logger.error("DLL não encontrada no caminho especificado: %s", caminho_dll)
            logger.error("Verifique o caminho definido em 'ADOMD_DLL_PATH' no seu arquivo .env.")
            raise FileNotFoundError(f"DLL do gateway não encontrada: {caminho_dll}")

        clr.AddReference(str(caminho_dll))
        logger.info("Driver AdomdClient carregado com sucesso de: %s", caminho_dll)

    except Exception as e:
        logger.critical("Falha crítica ao carregar a DLL a partir de '%s'.", caminho_dll)
        logger.critical("Verifique se o caminho está correto e se o usuário que executa o script tem permissões de acesso.")
        raise e
