#!/usr/bin/env python3
"""
Script para atualizar mapeamentos de escritórios e negociadores para todas as mesas suportadas.

Este script executa a atualização dos mapeamentos via API Parceiros para todas as mesas
disponíveis (btblue, 2bativos, bbmd) de forma sequencial.

Uso:
    python src/database/update_all_mesas.py [--log-level LEVEL] [--log FILE]

Exemplo:
    python src/database/update_all_mesas.py --log-level DEBUG --log logs/update_all.log
"""

import argparse
import sys
import time
from pathlib import Path

# Adiciona o diretório raiz do projeto ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loguru import logger
from dotenv import load_dotenv

from src.database.db_updater import ApiUpdater, DatabaseUpdateError

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Mesas suportadas
SUPPORTED_MESAS = ["btblue", "2bativos", "bbmd"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Atualiza mapeamentos de escritórios e negociadores para todas as mesas via API Parceiros."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de log (padrão: INFO)",
    )
    parser.add_argument("--log", type=Path, help="Arquivo de log (opcional)")
    args = parser.parse_args()

    # Configurar logging
    logger.remove()  # Remove default handler
    log_level = args.log_level.upper()

    if args.log:
        logger.add(
            args.log,
            level=log_level,
            format="{time} | {level} | {name}:{function}:{line} | {message}",
        )
    else:
        logger.add(
            sys.stdout,
            level=log_level,
            colorize=True,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
        )

    logger.info("=== Iniciando atualização de mapeamentos para todas as mesas ===")
    logger.info(f"Mesas a processar: {', '.join(SUPPORTED_MESAS)}")

    start_time = time.time()
    success_count = 0
    failed_mesas = []

    for mesa in SUPPORTED_MESAS:
        logger.info(f"=== Processando mesa: {mesa} ===")
        try:
            updater = ApiUpdater(mesa)
            updater.update_database()
            success_count += 1
            logger.info(f"Mesa '{mesa}' atualizada com sucesso.")
        except DatabaseUpdateError as e:
            logger.error(f"Erro ao atualizar mesa '{mesa}': {e}")
            failed_mesas.append(mesa)
        except Exception as e:
            logger.error(f"Erro inesperado ao atualizar mesa '{mesa}': {e}")
            failed_mesas.append(mesa)

    elapsed = time.time() - start_time
    logger.info("=== Atualização concluída ===")
    logger.info(f"Tempo total: {elapsed:.2f} segundos")
    logger.info(
        f"Mesas processadas com sucesso: {success_count}/{len(SUPPORTED_MESAS)}"
    )

    if failed_mesas:
        logger.warning(f"Mesas com falha: {', '.join(failed_mesas)}")
        return 1
    else:
        logger.success("Todas as mesas foram atualizadas com sucesso!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
