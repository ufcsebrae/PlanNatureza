# logger_config.py
from __future__ import annotations

import logging
import sys
from typing import Final

# Importa a configuração para usar o caminho da pasta de logs
from config import CONFIG

LOG_LEVEL: Final[int] = logging.INFO


def configurar_logger(nome_arquivo_log: str) -> logging.Logger:
    """
    Configura o logger raiz para exibir mensagens no console e salvá-las em um arquivo.

    Args:
        nome_arquivo_log: O nome do arquivo onde os logs serão salvos (ex: 'pipeline.log').
    """
    formato = logging.Formatter(
        fmt="%(asctime)s - %(levelname)-8s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # Limpa handlers existentes para evitar duplicação
    if logger.hasHandlers():
        logger.handlers.clear()

    # --- 1. Console Handler (para ver no terminal) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formato)
    logger.addHandler(console_handler)

    # --- 2. File Handler (para salvar em arquivo) ---
    log_dir = CONFIG.paths.logs_dir
    log_dir.mkdir(exist_ok=True)  # Cria a pasta 'logs' se ela não existir

    caminho_log_arquivo = log_dir / nome_arquivo_log
    file_handler = logging.FileHandler(caminho_log_arquivo, mode='a', encoding='utf-8')
    file_handler.setFormatter(formato)
    logger.addHandler(file_handler)

    logger.info(f"Logger configurado. Saída também será salva em: {caminho_log_arquivo}")

    return logger
