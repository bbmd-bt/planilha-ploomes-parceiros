import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

# Adiciona o diretório src ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

from transformer import PlanilhaTransformer
from db_updater import DatabaseUpdater, DatabaseUpdateError


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
    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log:
        handlers.append(logging.FileHandler(args.log))
    elif (base_dir / "logs").exists():
        log_file = (
            base_dir
            / "logs"
            / f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    logger = logging.getLogger(__name__)

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

    # Define caminho de saída padrão se não informado
    if args.output:
        output_path = args.output
    else:
        hoje = datetime.now().strftime("%d-%m-%Y")
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

    logger.info(f"Lendo planilha de entrada: {input_path}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        logger.error(f"Erro ao ler planilha: {e}")
        sys.exit(1)

    transformer = PlanilhaTransformer()
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
