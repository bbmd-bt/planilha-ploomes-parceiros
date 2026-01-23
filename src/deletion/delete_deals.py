#!/usr/bin/env python3
"""
Script para deletar negócios na Ploomes baseado em CNJs de um arquivo Excel.

Este script lê CNJs de um arquivo Excel, busca os negócios correspondentes na Ploomes,
move-os para um estágio específico e deleta aqueles que foram movidos com sucesso.

IMPORTANTE: Antes de deletar negócios no estágio de deleção, o script verifica se cada
negócio já existe na plataforma Parceiros. Isso previne que negócios ainda não importados
sejam deletados prematuramente. Negócios que não existem em Parceiros são preservados
para serem importados no próximo ciclo.

Validação Parceiros:
- Se as credenciais PARCEIROS_API_USERNAME e PARCEIROS_API_PASSWORD estiverem configuradas,
  cada negócio será validado contra a API da Parceiros antes da deleção
- Apenas negócios que já existem em Parceiros serão deletados
- Se um negócio não existir em Parceiros, ele será preservado para importação futura

Uso:
    python src/delete_deals.py --input arquivo.xlsx --api-token TOKEN --pipeline PIPELINE
"""

import argparse
import logging
import os
import subprocess  # nosec B404
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Adicionar o diretório pai ao sys.path para imports absolutos funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.ploomes_client import PloomesClient
from src.sync.ploomes_sync import PloomesSync
from src.clients.parceiros_client import ParceirosClient


# Mapeamento de pipelines para estágios
PIPELINE_CONFIG = {
    "BT Blue Pipeline": {"target_stage_id": 110351686, "deletion_stage_id": 110351653},
    "2B Ativos Pipeline": {
        "target_stage_id": 110351791,
        "deletion_stage_id": 110351790,
    },
    "BBMD Pipeline": {"target_stage_id": 110351793, "deletion_stage_id": 110351792},
    "Pipeline de Teste": {"target_stage_id": 110353005, "deletion_stage_id": 110353004},
}

# Mapeamento de pipelines para credenciais Parceiros
PARCEIROS_CREDENTIALS = {
    "BT Blue Pipeline": {
        "username": "integracao_bbmd_prod@btcreditos.com.br",
        "password": "36uXcN{;QdN8",
    },
    "2B Ativos Pipeline": {
        "username": "integracao_2b_prod@btcreditos.com.br",
        "password": "rw)#F#009/Zd6'xf+84R",
    },
    "BBMD Pipeline": {
        "username": "integracao_bbmd_prod@btcreditos.com.br",
        "password": "36uXcN{;QdN8",
    },
    "Pipeline de Teste": {
        "username": "integracao_bbmd_prod@btcreditos.com.br",  # Usar credenciais de teste ou padrão
        "password": "36uXcN{;QdN8",
    },
}

# Mapeamento de mesas para pipelines de origem e estágios de destino
ORIGIN_PIPELINE_CONFIG = {
    "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811},
    "Mesa 2B": {"pipeline_id": 110066163, "stage_id": 110352813},
    "Mesa Yasmin": {"pipeline_id": 110066161, "stage_id": 110352810},
    "Mesa BBMD": {"pipeline_id": 110066162, "stage_id": 110352812},
    "Mesa Elson": {"pipeline_id": 110066424, "stage_id": 110352814},
}


def setup_logging(
    log_level: str = "INFO", log_file: Optional[Path] = None
) -> logging.Logger:
    """Configura o sistema de logging."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(__name__)


def validate_pipeline(pipeline_name: str) -> dict:
    """Valida e retorna a configuração do pipeline."""
    if pipeline_name not in PIPELINE_CONFIG:
        available = ", ".join(PIPELINE_CONFIG.keys())
        raise ValueError(
            f"Pipeline '{pipeline_name}' não encontrado. Pipelines disponíveis: {available}"
        )

    return PIPELINE_CONFIG[pipeline_name]


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Deleta negócios na Ploomes baseado em CNJs de arquivo Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

# Processar arquivo com pipeline BT Blue (token do .env)
python src/delete_deals.py \\
  --input "input/cnjs_erro.xlsx" \\
  --pipeline "BT Blue Pipeline" \\
  --output "output/relatorio_delecao.xlsx"

# Com token específico
python src/delete_deals.py \\
  --input "input/cnjs.xlsx" \\
  --api-token "TOKEN_ESPECIFICO" \\
  --pipeline "2B Ativos Pipeline" \\
  --log-level DEBUG \\
  --log "logs/delete_deals.log"
        """,
    )

    parser.add_argument(
        "--input",
        "-i",
        required=True,
        type=Path,
        help="Caminho para o arquivo Excel de entrada com coluna CNJ",
    )

    parser.add_argument(
        "--api-token",
        "-t",
        default=os.getenv("PLOOMES_API_TOKEN"),
        help="Token de autenticação da API Ploomes (padrão: valor do .env)",
    )

    parser.add_argument(
        "--pipeline",
        "-p",
        required=True,
        choices=list(PIPELINE_CONFIG.keys()),
        help="Nome do pipeline a ser usado",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Caminho para o arquivo Excel de relatório de saída (opcional)",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de log (padrão: INFO)",
    )

    parser.add_argument(
        "--log", type=Path, help="Caminho para o arquivo de log (opcional)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa em modo de teste (não faz alterações reais)",
    )

    args = parser.parse_args()

    # Configura logging
    logger = setup_logging(args.log_level, args.log)

    # Valida entrada
    if not args.input.exists():
        logger.error(f"Arquivo de entrada não encontrado: {args.input}")
        sys.exit(1)

    # Valida pipeline
    try:
        pipeline_config = validate_pipeline(args.pipeline)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Valida token da API
    if not args.api_token:
        logger.error(
            "Token da API Ploomes não encontrado. Configure a variável "
            "PLOOMES_API_TOKEN no arquivo .env ou passe --api-token"
        )
        sys.exit(1)

    # Define caminho de saída padrão
    if not args.output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        args.output = output_dir / f"relatorio_delecao_{timestamp}.xlsx"

    # Garante que o diretório de saída existe
    args.output.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=== Iniciando processamento de deleção de negócios ===")
    logger.info(f"Arquivo de entrada: {args.input}")
    logger.info(f"Pipeline: {args.pipeline}")
    logger.info(f"Arquivo de saída: {args.output}")
    if args.dry_run:
        logger.info("MODO DRY-RUN: Nenhuma alteração será feita")

    try:
        # Carrega CNJs do arquivo Excel
        logger.info("Carregando CNJs do arquivo Excel...")
        cnj_list, cnj_errors = PloomesSync.load_cnjs_from_excel(str(args.input))
        logger.info(f"Encontrados {len(cnj_list)} CNJs para processar")

        if cnj_errors:
            logger.info(f"Encontradas descrições de erro para {len(cnj_errors)} CNJs")

        if not cnj_list:
            logger.warning("Nenhum CNJ válido encontrado no arquivo")
            sys.exit(0)

        # Inicializa cliente Ploomes
        client = PloomesClient(args.api_token)

        # Inicializa cliente Parceiros com credenciais específicas do pipeline
        parceiros_client = None
        if args.pipeline in PARCEIROS_CREDENTIALS:
            creds = PARCEIROS_CREDENTIALS[args.pipeline]
            logger.info(
                f"Inicializando cliente da API Parceiros para pipeline {args.pipeline}..."
            )
            parceiros_client = ParceirosClient(creds["username"], creds["password"])
        else:
            logger.warning(
                f"Credenciais da API Parceiros não encontradas para o pipeline {args.pipeline}. "
                "Deleção de negócios não será validada contra Parceiros."
            )

        # Inicializa sincronizador com informações de erro
        sync = PloomesSync(
            client=client,
            target_stage_id=pipeline_config["target_stage_id"],
            deletion_stage_id=pipeline_config["deletion_stage_id"],
            origin_config=ORIGIN_PIPELINE_CONFIG,
            dry_run=args.dry_run,
            cnj_errors=cnj_errors,
            parceiros_client=parceiros_client,
        )

        # Processa CNJs
        logger.info("Iniciando processamento...")
        report = sync.process_cnj_list(cnj_list)

        # Executa validação de interações para os estágios de origem usados
        if report.origin_stages_used:
            logger.info(
                f"Executando validação de interações para {len(report.origin_stages_used)} estágio(s) de origem..."
            )
            for stage_id in report.origin_stages_used:
                logger.info(f"Validando interações para estágio {stage_id}...")
                validate_cmd = [
                    sys.executable,
                    str(Path(__file__).parent / "validate_interactions.py"),
                    "--input",
                    str(args.input),
                    "--stage-id",
                    str(stage_id),
                    "--api-token",
                    args.api_token,
                    "--log-level",
                    args.log_level,
                ]
                if args.log:
                    validate_cmd.extend(["--log", str(args.log)])

                validate_result = subprocess.run(  # nosec B603
                    validate_cmd, capture_output=True, text=True
                )
                if validate_result.returncode == 0:
                    logger.info(
                        f"Validação de interações para estágio {stage_id} executada com sucesso"
                    )
                else:
                    logger.error(
                        f"Erro na validação de interações para estágio {stage_id}: {validate_result.stderr}"
                    )
        else:
            logger.info(
                "Nenhum estágio de origem foi usado, pulando validação de interações"
            )

        # Gera relatório
        logger.info("Gerando relatório...")
        sync.generate_report_excel(report, str(args.output))

        # Exibe resumo
        logger.info("=== Processamento concluído ===")
        logger.info(f"Total processado: {report.total_processed}")
        logger.info(f"Movidos com sucesso: {report.successfully_moved}")
        logger.info(f"Deletados com sucesso: {report.successfully_deleted}")
        logger.info(f"Falhas na movimentação: {report.failed_movements}")
        logger.info(f"Deleções puladas: {report.skipped_deletions}")
        logger.info(f"Relatório salvo em: {args.output}")

        # Verifica se houve erros críticos
        if report.failed_movements > 0:
            logger.warning(
                f"Houve {report.failed_movements} falhas na movimentação de estágios"
            )
        if report.skipped_deletions > 0:
            logger.warning(
                f"{report.skipped_deletions} deleções foram puladas devido a erros"
            )

        # Deleta o arquivo de entrada ao final da execução
        try:
            os.remove(str(args.input))
            logger.info(f"Arquivo de entrada deletado: {args.input}")
        except Exception as e:
            logger.warning(f"Erro ao deletar arquivo de entrada: {e}")

        return 0

    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        if args.log_level.upper() == "DEBUG":
            import traceback

            logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
