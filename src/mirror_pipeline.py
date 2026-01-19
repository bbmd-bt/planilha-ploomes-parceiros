"""
Script para criar um funil espelho a partir de um funil existente no Ploomes.

Uso:
    python src/mirror_pipeline.py --pipeline-id <ID_DO_PIPELINE> --api-token <TOKEN>

Este script clona um pipeline completo, incluindo seus estágios e negócios associados.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Adiciona o diretório src ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

from ploomes_client import PloomesClient


def setup_logging():
    """Configura o logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def validate_env():
    """Valida variáveis de ambiente necessárias."""
    api_token = os.getenv("PLOOMES_API_TOKEN")
    if not api_token:
        logging.error("Variável de ambiente PLOOMES_API_TOKEN não definida.")
        sys.exit(1)
    return api_token


def mirror_pipeline(client: PloomesClient, pipeline_id: int) -> bool:
    """
    Cria um funil espelho.

    Args:
        client: Cliente da API Ploomes
        pipeline_id: ID do pipeline original

    Returns:
        True se sucesso, False caso contrário
    """
    logging.info(f"Iniciando clonagem do pipeline {pipeline_id}")

    # 1. Obter dados do pipeline original
    original_pipeline = client.get_pipeline(pipeline_id)
    if not original_pipeline:
        logging.error(f"Pipeline {pipeline_id} não encontrado.")
        return False

    name = original_pipeline.get("Name")
    if not name:
        logging.error(f"Pipeline {pipeline_id} não possui nome válido.")
        return False

    logging.info(f"Pipeline original: {name}")

    stages = original_pipeline.get("Stages", [])
    if not stages:
        logging.error(f"Pipeline {pipeline_id} não possui estágios.")
        return False

    # Preparar dados do novo pipeline clonando atributos importantes
    new_pipeline_data = {
        "Name": f"{name} (Espelho)",
        "Active": True,
        "MayCreateQuotes": original_pipeline.get("MayCreateQuotes", True),
        "MayCreateOrders": original_pipeline.get("MayCreateOrders", False),
        "MayCreateDocuments": original_pipeline.get("MayCreateDocuments", False),
        "MayWinDeals": original_pipeline.get("MayWinDeals", False),
        "MayLoseDeals": original_pipeline.get("MayLoseDeals", False),
        "MustPassAllStages": original_pipeline.get("MustPassAllStages", False),
        "ForbiddenStageReturn": original_pipeline.get("ForbiddenStageReturn", False),
        "SingularUnitName": original_pipeline.get("SingularUnitName"),
        "PluralUnitName": original_pipeline.get("PluralUnitName"),
        "GenderId": original_pipeline.get("GenderId"),
        "Color": original_pipeline.get("Color"),
        "IconId": original_pipeline.get("IconId"),
        "EnableTableViewMode": original_pipeline.get("EnableTableViewMode", False),
        "EnableFunnelViewMode": original_pipeline.get("EnableFunnelViewMode", False),
        "WinButtonId": original_pipeline.get("WinButtonId", 1),
        "LoseButtonId": original_pipeline.get("LoseButtonId", 5),
        "WinVerbId": original_pipeline.get("WinVerbId", 1),
        "LoseVerbId": original_pipeline.get("LoseVerbId", 2),
        "ContactPageTableId": original_pipeline.get("ContactPageTableId"),
        "Stages": [
            {
                "Name": stage.get("Name", f"Estágio {stage.get('Ordination', i+1)}"),
                "Ordination": stage.get("Ordination", i + 1),
            }
            for i, stage in enumerate(stages)
        ],
    }

    # 3. Criar novo pipeline
    new_pipeline = client.create_pipeline(new_pipeline_data)
    if not new_pipeline:
        logging.error("Falha ao criar novo pipeline.")
        return False

    new_pipeline_id = new_pipeline.get("Id")
    if not new_pipeline_id:
        logging.error("Novo pipeline criado sem ID.")
        return False

    # Buscar o pipeline criado para obter os estágios com IDs
    new_pipeline_full = client.get_pipeline(new_pipeline_id)
    if not new_pipeline_full:
        logging.error("Falha ao buscar dados completos do novo pipeline.")
        return False

    new_name = new_pipeline_full.get("Name", f"{name} (Espelho)")
    logging.info(f"Novo pipeline criado: {new_name} (ID: {new_pipeline_id})")

    # 4. Criar mapeamento de estágios
    stage_mapping = {}
    original_stages = {}
    for stage in original_pipeline.get("Stages", []):
        ord = stage.get("Ordination")
        id = stage.get("Id")
        if ord is not None and id is not None:
            original_stages[ord] = id

    new_stages = {}
    for stage in new_pipeline_full.get("Stages", []):
        ord = stage.get("Ordination")
        id = stage.get("Id")
        if ord is not None and id is not None:
            new_stages[ord] = id

    for ord, old_id in original_stages.items():
        if ord in new_stages:
            stage_mapping[old_id] = new_stages[ord]
        else:
            logging.warning(
                f"Estágio com ordenação {ord} não encontrado no novo pipeline."
            )

    # Definir estágio padrão para casos não mapeados
    default_stage_id = None
    if new_stages:
        min_ord = min(new_stages.keys())
        default_stage_id = new_stages[min_ord]
        logging.info(
            f"Estágio padrão definido: {default_stage_id} (ordenação {min_ord})"
        )

    # 5. Obter negócios do pipeline original
    deals = client.get_deals_by_pipeline(pipeline_id)
    if not deals:
        logging.info("Nenhum negócio encontrado no pipeline original.")
        return True

    # 6. Replicar negócios
    success_count = 0
    for deal in deals:
        new_deal = deal.copy()
        # Remover ID para criação
        new_deal.pop("Id", None)
        # Atualizar PipelineId
        new_deal["PipelineId"] = new_pipeline_id
        # Atualizar StageId se mapeado
        old_stage_id = deal.get("StageId")
        if old_stage_id and old_stage_id in stage_mapping:
            new_deal["StageId"] = stage_mapping[old_stage_id]
        else:
            if default_stage_id:
                new_deal["StageId"] = default_stage_id
                logging.warning(
                    f"StageId {old_stage_id} não mapeado, usando estágio padrão "
                    f"{default_stage_id} para negócio {deal.get('Title', 'Sem título')}"
                )
            else:
                logging.error(
                    f"StageId {old_stage_id} não mapeado e nenhum estágio padrão "
                    f"disponível para negócio {deal.get('Title', 'Sem título')}"
                )
                continue

        # Criar negócio
        created_deal = client.create_deal(new_deal)
        if created_deal:
            success_count += 1
            logging.info(
                f"Negócio replicado: {created_deal.get('Title', 'Sem título')}"
            )
        else:
            logging.error(
                f"Falha ao replicar negócio: {deal.get('Title', 'Sem título')}"
            )

        # Pequena pausa para evitar rate limiting
        time.sleep(1)

    logging.info(
        f"Clonagem concluída. {success_count}/{len(deals)} negócios replicados com sucesso."
    )

    # Validação da integridade do novo pipeline
    validate_pipeline_integrity(client, new_pipeline_id, len(deals), success_count)

    return True


def validate_pipeline_integrity(
    client: PloomesClient, pipeline_id: int, expected_deals: int, created_deals: int
) -> None:
    """
    Valida a integridade do pipeline recém-criado.

    Args:
        client: Cliente da API Ploomes
        pipeline_id: ID do pipeline a validar
        expected_deals: Número esperado de deals
        created_deals: Número de deals criados com sucesso
    """
    logging.info("Iniciando validação da integridade do novo pipeline...")

    try:
        # Verificar se o pipeline existe e tem stages
        pipeline = client.get_pipeline(pipeline_id)
        if not pipeline:
            logging.error(f"ERRO CRÍTICO: Pipeline {pipeline_id} não foi encontrado!")
            return

        stages = pipeline.get("Stages", [])
        if not stages:
            logging.error(f"ERRO CRÍTICO: Pipeline {pipeline_id} não possui estágios!")
            return

        logging.info(f"✓ Pipeline encontrado com {len(stages)} estágios")

        # Verificar deals no pipeline
        deals_in_pipeline = client.get_deals_by_pipeline(pipeline_id)
        actual_deals = len(deals_in_pipeline)

        logging.info(f"✓ Encontrados {actual_deals} negócios no novo pipeline")

        # Validações
        if actual_deals == 0:
            logging.error("ERRO CRÍTICO: Nenhum negócio encontrado no novo pipeline!")
            logging.error(
                "Isso pode indicar que os negócios não foram criados ou estão em pipeline errado"
            )
        elif actual_deals < created_deals:
            logging.warning(
                f"AVISO: {actual_deals} negócios encontrados, mas {created_deals} foram criados com sucesso"
            )
            logging.warning(
                "Alguns negócios podem ter sido criados em pipeline errado ou deletados"
            )
        elif actual_deals > created_deals:
            logging.warning(
                f"AVISO: {actual_deals} negócios encontrados, mas apenas {created_deals} foram criados"
            )
            logging.warning("Pode haver negócios pré-existentes no pipeline")
        else:
            logging.info("✓ Número de negócios consistente com o esperado")

        # Verificar se alguns deals têm stages válidos
        deals_with_stages = sum(1 for deal in deals_in_pipeline if deal.get("StageId"))
        if deals_with_stages == 0:
            logging.error("ERRO CRÍTICO: Nenhum negócio possui StageId válido!")
            logging.error("Isso pode causar problemas de visualização no Ploomes")
        elif deals_with_stages < actual_deals:
            logging.warning(
                f"AVISO: Apenas {deals_with_stages}/{actual_deals} negócios possuem StageId válido"
            )
        else:
            logging.info("✓ Todos os negócios possuem StageId válido")

        # Verificar configurações críticas de visualização
        if not pipeline.get("EnableFunnelViewMode"):
            logging.error("ERRO CRÍTICO: EnableFunnelViewMode está desativado!")
            logging.error(
                "Isso causa tela branca ao visualizar o funil. Ative manualmente ou recrie o pipeline."
            )
        else:
            logging.info("✓ Visualização em funil ativada")

        if not pipeline.get("EnableTableViewMode"):
            logging.warning("AVISO: EnableTableViewMode está desativado")
        else:
            logging.info("✓ Visualização em tabela ativada")

        # Verificar atributos de exibição
        if not pipeline.get("SingularUnitName") or not pipeline.get("PluralUnitName"):
            logging.warning("AVISO: Nomes singulares/plurais não configurados")
        else:
            logging.info(
                f"✓ Nomes configurados: {pipeline.get('SingularUnitName')} / {pipeline.get('PluralUnitName')}"
            )

        if not pipeline.get("Color"):
            logging.warning("AVISO: Cor do pipeline não configurada")
        else:
            logging.info(f"✓ Cor configurada: {pipeline.get('Color')}")

        if not pipeline.get("IconId"):
            logging.warning("AVISO: Ícone do pipeline não configurado")
        else:
            logging.info(f"✓ Ícone configurado: ID {pipeline.get('IconId')}")

        # Verificar se MayWinDeals e MayLoseDeals estão configurados
        if not pipeline.get("MayWinDeals"):
            logging.warning(
                "AVISO: Negócios não podem ser ganhos (MayWinDeals desativado)"
            )
        if not pipeline.get("MayLoseDeals"):
            logging.warning(
                "AVISO: Negócios não podem ser perdidos (MayLoseDeals desativado)"
            )

        # Verificar se pipeline está ativo
        if not pipeline.get("Active", True):
            logging.warning("AVISO: O pipeline não está marcado como ativo")

        # Verificar se o pipeline tem nome correto
        pipeline_name = pipeline.get("Name", "")
        if not pipeline_name:
            logging.error("ERRO CRÍTICO: Pipeline não possui nome!")
        else:
            logging.info(f"✓ Pipeline nomeado como: {pipeline_name}")

        # Verificar se stages têm nomes
        stages_without_name = sum(1 for stage in stages if not stage.get("Name"))
        if stages_without_name > 0:
            logging.warning(f"AVISO: {stages_without_name} estágios não possuem nome")

        logging.info("Validação da integridade concluída.")

    except Exception as e:
        logging.error(f"Erro durante validação da integridade: {e}")


def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Clona um pipeline do Ploomes incluindo estágios e negócios."
    )
    parser.add_argument(
        "--pipeline-id", type=int, required=True, help="ID do pipeline a ser clonado"
    )
    parser.add_argument(
        "--api-token",
        type=str,
        help="Token da API Ploomes (opcional se definido em PLOOMES_API_TOKEN)",
    )

    args = parser.parse_args()

    api_token = args.api_token or os.getenv("PLOOMES_API_TOKEN")
    if not api_token:
        logging.error(
            "Token da API não fornecido. Use --api-token ou defina PLOOMES_API_TOKEN."
        )
        sys.exit(1)

    client = PloomesClient(api_token=api_token)

    success = mirror_pipeline(client, args.pipeline_id)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
