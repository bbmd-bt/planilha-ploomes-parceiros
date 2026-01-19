#!/usr/bin/env python3
"""
Script para remover negócios duplicados de um funil específico da Ploomes.

Este script identifica duplicatas baseadas no CNJ, mantendo apenas o negócio mais antigo
(baseado na data de criação) e removendo os demais.

Uso:
    python src/delete_duplicate_deals.py --pipeline-id <ID_DO_PIPELINE> --api-token <TOKEN>
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Adicionar o diretório pai ao sys.path para imports absolutos funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ploomes_client import PloomesClient


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configura o sistema de logging."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    return logging.getLogger(__name__)


def _extract_cnj_from_deal(deal: Dict) -> Optional[str]:
    """
    Extrai o CNJ de um negócio Ploomes.

    Args:
        deal: Dicionário representando o negócio

    Returns:
        CNJ extraído ou None se não encontrado
    """
    other_properties = deal.get("OtherProperties", [])
    for prop in other_properties:
        if prop.get("FieldKey") == "deal_20E8290A-809B-4CF1-9345-6B264AED7830":
            return str(prop.get("StringValue", "")).strip()
    return None


def remove_duplicate_deals(
    client: PloomesClient,
    pipeline_id: int,
    logger: logging.Logger,
    dry_run: bool = False,
) -> bool:
    """
    Remove negócios duplicados de um pipeline baseado no CNJ.

    Args:
        client: Instância do PloomesClient
        pipeline_id: ID do pipeline
        logger: Logger para mensagens

    Returns:
        True se a operação foi bem-sucedida
    """
    logger.info(
        f"Iniciando remoção de duplicatas no pipeline {pipeline_id}"
        + (" (DRY RUN)" if dry_run else "")
    )

    # Obter todos os negócios do pipeline
    deals = client.get_deals_by_pipeline(pipeline_id)
    if not deals:
        logger.warning(f"Nenhum negócio encontrado no pipeline {pipeline_id}")
        return True

    logger.info(f"Encontrados {len(deals)} negócios no pipeline {pipeline_id}")

    # Agrupar por CNJ
    cnj_groups = defaultdict(list)
    deals_without_cnj = []

    for deal in deals:
        cnj = _extract_cnj_from_deal(deal)
        if cnj:
            cnj_groups[cnj].append(deal)
        else:
            deals_without_cnj.append(deal)

    logger.info(f"Negócios sem CNJ: {len(deals_without_cnj)}")
    logger.info(f"Grupos de CNJ únicos: {len(cnj_groups)}")

    # Contar duplicatas
    duplicate_groups = {
        cnj: len(group) for cnj, group in cnj_groups.items() if len(group) > 1
    }
    logger.info(f"CNJs com duplicatas: {len(duplicate_groups)}")
    for cnj, count in duplicate_groups.items():
        logger.info(f"  CNJ {cnj}: {count} negócios")

    total_deleted = 0

    # Processar cada grupo de CNJ
    for cnj, group_deals in cnj_groups.items():
        if len(group_deals) <= 1:
            continue  # Não há duplicatas

        logger.info(f"Processando CNJ {cnj} com {len(group_deals)} negócios")

        # Ordenar por data de criação (mais antigo primeiro)
        sorted_deals = sorted(group_deals, key=lambda d: d.get("CreateDate", ""))

        # Manter o primeiro (mais antigo), deletar os demais
        deals_to_delete = sorted_deals[1:]

        for deal in deals_to_delete:
            deal_id = deal.get("Id")
            if deal_id:
                if dry_run:
                    logger.info(
                        f"[DRY RUN] Seria deletado negócio duplicado ID {deal_id} para CNJ {cnj}"
                    )
                    total_deleted += 1
                else:
                    logger.info(
                        f"Deletando negócio duplicado ID {deal_id} para CNJ {cnj}"
                    )
                    if client.delete_deal(deal_id):
                        total_deleted += 1
                    else:
                        logger.error(f"Falha ao deletar negócio ID {deal_id}")

    logger.info(
        f"Remoção de duplicatas concluída. Total de negócios deletados: {total_deleted}"
    )
    return True


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Remove negócios duplicados de um pipeline da Ploomes baseado no CNJ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

# Remover duplicatas de um pipeline (token do .env)
python src/delete_duplicate_deals.py --pipeline-id 110066857

# Com token específico
python src/delete_duplicate_deals.py --pipeline-id 110066857 --api-token "TOKEN_ESPECIFICO"
        """,
    )

    parser.add_argument(
        "--pipeline-id", type=int, required=True, help="ID do pipeline a ser processado"
    )
    parser.add_argument(
        "--api-token",
        type=str,
        help="Token da API Ploomes (opcional se definido em PLOOMES_API_TOKEN)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executar em modo simulação (não deleta nada, apenas mostra o que seria feito)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de logging (padrão: INFO)",
    )

    args = parser.parse_args()

    logger = setup_logging(args.log_level)

    api_token = args.api_token or os.getenv("PLOOMES_API_TOKEN")
    if not api_token:
        logger.error(
            "Token da API não fornecido. Use --api-token ou defina PLOOMES_API_TOKEN."
        )
        sys.exit(1)

    client = PloomesClient(api_token=api_token)

    success = remove_duplicate_deals(client, args.pipeline_id, logger, args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
