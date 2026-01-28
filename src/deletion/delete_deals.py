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

Tratamento de erros "já existe":
- CNJs com erro contendo "já existe" são considerados casos de sucesso e serão deletados
- Eles são removidos da lista de preservação e tratados como negócios a serem deletados

Uso:
    python src/delete_deals.py --input arquivo.xlsx --api-token TOKEN --pipeline PIPELINE
"""

import argparse
import logging
import os
import shutil
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

# Mapeamento de pipelines para credenciais Parceiros (carregadas do .env)
# Variáveis de ambiente esperadas:
# PARCEIROS_BT_BLUE_USERNAME, PARCEIROS_BT_BLUE_PASSWORD
# PARCEIROS_2B_ATIVOS_USERNAME, PARCEIROS_2B_ATIVOS_PASSWORD
# PARCEIROS_BBMD_USERNAME, PARCEIROS_BBMD_PASSWORD


def get_parceiros_credentials(pipeline_name: str) -> Optional[dict]:
    """Carrega credenciais Parceiros do arquivo .env para o pipeline especificado."""
    credential_map = {
        "BT Blue Pipeline": (
            "PARCEIROS_BT_BLUE_USERNAME",
            "PARCEIROS_BT_BLUE_PASSWORD",
        ),
        "2B Ativos Pipeline": (
            "PARCEIROS_2B_ATIVOS_USERNAME",
            "PARCEIROS_2B_ATIVOS_PASSWORD",
        ),
        "BBMD Pipeline": ("PARCEIROS_BBMD_USERNAME", "PARCEIROS_BBMD_PASSWORD"),
        "Pipeline de Teste": (
            "PARCEIROS_BT_BLUE_USERNAME",
            "PARCEIROS_BT_BLUE_PASSWORD",
        ),
    }

    if pipeline_name not in credential_map:
        return None

    username_var, password_var = credential_map[pipeline_name]
    username = os.getenv(username_var)
    password = os.getenv(password_var)

    if username and password:
        return {"username": username, "password": password}
    return None


# Mapeamento de pipelines para nomes de mesas
PIPELINE_TO_MESA_MAP = {
    "BT Blue Pipeline": "btblue",
    "2B Ativos Pipeline": "2bativos",
    "BBMD Pipeline": "bbmd",
    "Pipeline de Teste": "test",
}

# Mapeamento de mesas para pipelines de origem e estágios de destino
ORIGIN_PIPELINE_CONFIG = {
    "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811},
    "Mesa 2B": {"pipeline_id": 110066163, "stage_id": 110352813},
    "Mesa Yasmin": {"pipeline_id": 110066161, "stage_id": 110352810},
    "Mesa BBMD": {"pipeline_id": 110066162, "stage_id": 110352812},
    "Mesa Elson": {"pipeline_id": 110066424, "stage_id": 110352814},
}

# Logger global (será inicializado em setup_logging)
logger = logging.getLogger(__name__)


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


def find_planilhas_for_upload(mesa_key: str) -> Optional[tuple]:
    """
    Procura pelas planilhas de sucesso e erros da data de hoje na estrutura de pastas.

    Args:
        mesa_key: Chave da mesa (btblue, 2bativos, bbmd, test)

    Returns:
        Tupla (success_file, errors_file) ou None se não encontrar
    """
    today = datetime.now().strftime("%d-%m-%Y")

    # Procurar na pasta de output
    output_dir = Path("output") / today / mesa_key
    errors_dir = Path("errors") / today / mesa_key

    if not output_dir.exists() or not errors_dir.exists():
        logger.warning(
            f"Diretórios não encontrados. Output: {output_dir.exists()}, Errors: {errors_dir.exists()}"
        )
        return None

    # Procurar por arquivos Excel
    success_files = list(output_dir.glob("*.xlsx"))
    error_files = list(errors_dir.glob("*.xlsx"))

    if not success_files or not error_files:
        logger.warning(
            f"Arquivos não encontrados. Success: {len(success_files)}, Errors: {len(error_files)}"
        )
        return None

    # Pegar o primeiro arquivo de cada pasta (geralmente há apenas um)
    return (str(success_files[0]), str(error_files[0]))


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

    # Copia a planilha de entrada para errors/hoje/mesa
    try:
        mesa_key = PIPELINE_TO_MESA_MAP.get(args.pipeline)
        if mesa_key:
            today = datetime.now().strftime("%d-%m-%Y")
            errors_copy_dir = Path("errors") / today / mesa_key
            errors_copy_dir.mkdir(parents=True, exist_ok=True)
            errors_copy_path = errors_copy_dir / args.input.name
            shutil.copy(str(args.input), str(errors_copy_path))
            logger.info(f"Planilha de entrada copiada para: {errors_copy_path}")
        else:
            logger.warning(
                f"Não foi possível mapear o pipeline {args.pipeline} para uma mesa"
            )
    except Exception as e:
        logger.warning(f"Erro ao copiar planilha de entrada: {e}")

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
        creds = get_parceiros_credentials(args.pipeline)
        if creds:
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
                    str(Path(__file__).parent.parent / "validate_interactions.py"),
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

        # Executa upload para o banco de dados (se não estiver em dry-run)
        if not args.dry_run:
            logger.info("=== Iniciando upload para o banco de dados ===")

            # Mapear pipeline para mesa key
            mesa_key = PIPELINE_TO_MESA_MAP.get(args.pipeline)
            if not mesa_key:
                logger.warning(
                    f"Não foi possível mapear o pipeline {args.pipeline} para uma mesa"
                )
            else:
                # Procurar pelas planilhas
                planilhas = find_planilhas_for_upload(mesa_key)
                if planilhas:
                    success_file, errors_file = planilhas
                    logger.info("Encontrados arquivos para upload:")
                    logger.info(f"  - Sucesso: {success_file}")
                    logger.info(f"  - Erros: {errors_file}")

                    # Executar upload_leads_history
                    upload_cmd = [
                        sys.executable,
                        str(
                            Path(__file__).parent.parent
                            / "upload"
                            / "upload_leads_history.py"
                        ),
                        "--success",
                        success_file,
                        "--errors",
                        errors_file,
                        "--mesa",
                        mesa_key,
                        "--log-level",
                        args.log_level,
                    ]

                    if args.log:
                        upload_cmd.extend(["--log", str(args.log)])

                    try:
                        upload_result = subprocess.run(  # nosec B603
                            upload_cmd, capture_output=True, text=True
                        )
                        if upload_result.returncode == 0:
                            logger.info(
                                "Upload para o banco de dados executado com sucesso"
                            )
                        else:
                            logger.error(
                                f"Erro no upload para o banco de dados: {upload_result.stderr}"
                            )
                    except Exception as e:
                        logger.error(f"Erro ao executar script de upload: {e}")
                else:
                    logger.warning(
                        f"Não foi possível encontrar os arquivos de upload para a mesa {mesa_key} na data de hoje"
                    )
        else:
            logger.info("Modo DRY-RUN ativo - pulando upload para o banco de dados")

        return 0

    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        if args.log_level.upper() == "DEBUG":
            import traceback

            logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
