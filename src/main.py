import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import shutil
from dotenv import load_dotenv
from loguru import logger

from transformer import PlanilhaTransformer
from ploomes_client import PloomesClient

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()
from db_updater import DatabaseUpdater, DatabaseUpdateError

# Adiciona o diretório src ao path para imports
sys.path.insert(0, str(Path(__file__).parent))


def main() -> int:
    # Define o diretório base do projeto (um nível acima de src)
    base_dir = Path(__file__).parent.parent

    parser = argparse.ArgumentParser(
        description="Transforma planilha de parceiros para padrão Ploomes."
    )
    parser.add_argument(
        "--input",
        default=base_dir / "input" / "entrada.xlsx",
        type=Path,
        help="Arquivo de entrada (xlsx)",
    )
    parser.add_argument(
        "--mesa",
        required=True,
        help="Nome da mesa referente aos leads (obrigatório)",
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help="Arquivo de saída (xlsx). Se não informado, será criado automaticamente.",
    )
    parser.add_argument(
        "--log", default=None, type=Path, help="Arquivo de log de erros"
    )
    parser.add_argument(
        "--log-level", default="INFO", help="Nível de log (DEBUG, INFO, WARNING, ERROR)"
    )
    parser.add_argument(
        "--update-db",
        action="store_true",
        help="Atualiza mapeamentos de escritórios e negociadores do banco de dados",
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("PLOOMES_API_TOKEN"),
        help="Token da API Ploomes (padrão: PLOOMES_API_TOKEN do .env)",
    )
    parser.add_argument(
        "--deletion-stage-id",
        type=int,
        help="ID do estágio de deleção na Ploomes",
    )
    args = parser.parse_args()

    # Configure loguru
    logger.remove()  # Remove default handler
    log_level = args.log_level.upper()
    if args.log:
        logger.add(
            args.log,
            level=log_level,
            format="{time} | {level} | {name}:{function}:{line} | {message}",
        )
    elif (base_dir / "logs").exists():
        log_file = (
            base_dir
            / "logs"
            / f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        logger.add(
            log_file,
            level=log_level,
            format="{time} | {level} | {name}:{function}:{line} | {message}",
        )
    logger.add(
        sys.stdout,
        level=log_level,
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        ),
    )

    # Atualiza banco de dados se solicitado
    if args.update_db:
        try:
            updater = DatabaseUpdater()
            updater.update_database()
        except DatabaseUpdateError as e:
            logger.error(f"Erro na atualização do banco de dados: {e}")
            sys.exit(1)

    input_path = args.input
    mesa = args.mesa

    hoje = datetime.now().strftime("%d-%m-%Y")

    # Define caminho de saída padrão se não informado
    if args.output:
        output_path = args.output
    else:
        output_dir = base_dir / "output" / hoje / mesa
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "importacao_parceiros.xlsx"

    # Garante que o diretório de saída existe
    output_path.parent.mkdir(parents=True, exist_ok=True)

    log_path = (
        args.log
        or base_dir
        / "logs"
        / f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    if args.log is None and (base_dir / "logs").exists():
        (base_dir / "logs").mkdir(exist_ok=True)

    if not input_path.exists():
        logger.error(f"Arquivo de entrada não encontrado: {input_path}")
        sys.exit(1)

    # Copia a planilha de entrada para input/hoje/mesa
    input_copy_dir = base_dir / "input" / hoje / mesa
    input_copy_dir.mkdir(parents=True, exist_ok=True)
    input_copy_path = input_copy_dir / input_path.name
    shutil.copy(input_path, input_copy_path)
    logger.info(f"Planilha de entrada copiada para: {input_copy_path}")

    logger.info(f"Lendo planilha de entrada: {input_path}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        logger.error(f"Erro ao ler planilha: {e}")
        sys.exit(1)

    # Inicializar cliente Ploomes se token fornecido
    ploomes_client = None
    if args.api_token and args.deletion_stage_id:
        ploomes_client = PloomesClient(args.api_token)

    transformer = PlanilhaTransformer(
        ploomes_client=ploomes_client, deletion_stage_id=args.deletion_stage_id
    )
    df_out = transformer.transform(df)

    logger.info(f"Salvando planilha de saída: {output_path}")
    try:
        df_out.to_excel(output_path, index=False)
    except Exception as e:
        logger.error(f"Erro ao salvar planilha: {e}")
        sys.exit(1)

    logger.info(f"Salvando log de erros: {log_path}")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(transformer.get_error_report())
    except Exception as e:
        logger.error(f"Erro ao salvar log: {e}")
        sys.exit(1)

    logger.info("Processamento concluído.")
    logger.info(f"Linhas processadas: {len(df)}")
    logger.info(f"Linhas com erro: {len(transformer.errors)}")
    if transformer.errors:
        logger.warning(f"Veja detalhes no log: {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
