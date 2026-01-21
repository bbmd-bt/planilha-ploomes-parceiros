#!/usr/bin/env python3
"""
Script para fazer upload do histórico de leads para o banco de dados PostgreSQL.

Este script lê duas planilhas Excel (uma com todos os leads e outra com leads com erro),
processa os dados e insere/atualiza registros na tabela leads_parceiros_upload_history.

Uso:
    python src/upload_leads_history.py --success planilha_todos.xlsx --errors planilha_erros.xlsx --mesa "Nome da Mesa"
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Adicionar o diretório pai ao sys.path para imports absolutos funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger


class DatabaseConnection:
    """Gerencia a conexão com o banco de dados PostgreSQL."""

    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self):
        """Estabelece conexão com o banco de dados."""
        # Tentar DATABASE_URL primeiro, depois componentes separados
        database_url = os.getenv("DATABASE_URL")

        if database_url:
            self.connection = psycopg2.connect(database_url)
        else:
            # Componentes separados
            db_config = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": os.getenv("DB_PORT", "5432"),
                "database": os.getenv("DB_NAME"),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASSWORD"),
            }

            # Verificar se todos os componentes necessários estão presentes
            missing = [k for k, v in db_config.items() if not v]
            if missing:
                raise ValueError(
                    f"Variáveis de ambiente faltando: {', '.join(missing)}"
                )

            self.connection = psycopg2.connect(**db_config)

        # Configurar para não fazer commit automático
        self.connection.autocommit = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
            self.connection.close()

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Executa uma query SQL."""
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def execute_many(self, query: str, data: List[Tuple]):
        """Executa uma query com múltiplos parâmetros."""
        with self.connection.cursor() as cursor:
            execute_values(cursor, query, data)


class LeadsHistoryUploader:
    """Classe responsável por fazer upload do histórico de leads."""

    def __init__(
        self, success_file: str, errors_file: str, mesa: str, dry_run: bool = False
    ):
        self.success_file = success_file
        self.errors_file = errors_file
        self.mesa = mesa
        self.dry_run = dry_run

    def load_success_leads(self) -> pd.DataFrame:
        """Carrega a planilha com todos os leads."""
        try:
            df = pd.read_excel(self.success_file)
            logger.info(f"Carregados {len(df)} leads da planilha de sucesso")

            # Validar colunas obrigatórias (aceitar tanto "Negociador" quanto "Responsável")
            negotiator_col = None
            if "Negociador" in df.columns:
                negotiator_col = "Negociador"
            elif "Responsável" in df.columns:
                negotiator_col = "Responsável"
            else:
                raise ValueError(
                    "Coluna 'Negociador' ou 'Responsável' não encontrada na planilha de sucesso"
                )

            if "CNJ" not in df.columns:
                raise ValueError("Coluna 'CNJ' não encontrada na planilha de sucesso")

            # Renomear coluna para padronizar
            if negotiator_col == "Responsável":
                df = df.rename(columns={"Responsável": "Negociador"})

            return df
        except Exception as e:
            raise ValueError(f"Erro ao ler planilha de sucesso: {e}")

    def load_error_leads(self) -> pd.DataFrame:
        """Carrega a planilha com leads que falharam."""
        try:
            df = pd.read_excel(self.errors_file)
            logger.info(f"Carregados {len(df)} leads com erro da planilha de erros")

            # Validar colunas obrigatórias (aceitar tanto "Negociador" quanto "Responsável")
            negotiator_col = None
            if "Negociador" in df.columns:
                negotiator_col = "Negociador"
            elif "Responsável" in df.columns:
                negotiator_col = "Responsável"
            else:
                raise ValueError(
                    "Coluna 'Negociador' ou 'Responsável' não encontrada na planilha de erros"
                )

            required_cols = ["CNJ", negotiator_col, "Erro"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(
                    f"Colunas obrigatórias faltando na planilha de erros: {missing_cols}"
                )

            # Renomear coluna para padronizar
            if negotiator_col == "Responsável":
                df = df.rename(columns={"Responsável": "Negociador"})

            return df
        except Exception as e:
            raise ValueError(f"Erro ao ler planilha de erros: {e}")

    def process_leads(self) -> Tuple[List[Tuple], List[Tuple]]:
        """
        Processa os leads e retorna tuplas para inserção.

        Returns:
            Tupla com (success_records, error_records)
        """
        # Carregar dados
        success_df = self.load_success_leads()
        error_df = self.load_error_leads()

        # Obter CNJs com erro
        error_cnjs = set(error_df["CNJ"].dropna().astype(str).str.strip())

        success_records = []
        error_records = []

        # Processar leads bem-sucedidos (presentes em sucesso mas não em erro)
        for _, row in success_df.iterrows():
            cnj = str(row.get("CNJ", "")).strip()
            negociador = str(row.get("Negociador", "")).strip()
            escritorio = str(row.get("Escritório", "")).strip()

            if not cnj or not negociador:
                logger.warning(
                    f"Pulando linha com CNJ ou Negociador vazio: CNJ='{cnj}', Negociador='{negociador}'"
                )
                continue

            # Se CNJ não está na lista de erros, é um lead bem-sucedido
            if cnj not in error_cnjs:
                success_records.append(
                    (cnj, negociador, self.mesa, False, None, escritorio or None)
                )

        # Processar leads com erro
        for _, row in error_df.iterrows():
            cnj = str(row.get("CNJ", "")).strip()
            negociador = str(row.get("Negociador", "")).strip()
            error_message = str(row.get("Erro", "")).strip()
            escritorio = str(row.get("Escritório", "")).strip()

            if not cnj or not negociador:
                logger.warning(
                    f"Pulando linha com CNJ ou Negociador vazio: CNJ='{cnj}', Negociador='{negociador}'"
                )
                continue

            error_records.append(
                (
                    cnj,
                    negociador,
                    self.mesa,
                    True,
                    error_message or None,
                    escritorio or None,
                )
            )

        logger.info(
            f"Processados {len(success_records)} leads bem-sucedidos e {len(error_records)} leads com erro"
        )

        return success_records, error_records

    def upload_to_database(
        self, success_records: List[Tuple], error_records: List[Tuple]
    ):
        """Faz upload dos registros para o banco de dados."""
        all_records = success_records + error_records

        if not all_records:
            logger.warning("Nenhum registro para inserir")
            return

        # Query de UPSERT usando execute_values
        # O formato é: (col1, col2, col3, ...) VALUES %s
        upsert_query = """
            INSERT INTO leads_parceiros_upload_history (cnj, negociador, mesa, error, error_message, escritorio)
            VALUES %s
            ON CONFLICT (cnj)
            DO UPDATE SET
                negociador = EXCLUDED.negociador,
                mesa = EXCLUDED.mesa,
                error = EXCLUDED.error,
                error_message = EXCLUDED.error_message,
                escritorio = EXCLUDED.escritorio
        """

        try:
            with DatabaseConnection() as db:
                db.execute_many(upsert_query, all_records)
                logger.info(
                    f"Inseridos/atualizados {len(all_records)} registros no banco de dados"
                )

        except Exception as e:
            logger.error(f"Erro ao fazer upload para o banco de dados: {e}")
            raise

    def run(self):
        """Executa o processo completo de upload."""
        logger.info("=== Iniciando upload do histórico de leads ===")
        logger.info(f"Planilha de sucesso: {self.success_file}")
        logger.info(f"Planilha de erros: {self.errors_file}")
        logger.info(f"Mesa: {self.mesa}")
        if self.dry_run:
            logger.info(
                "Modo DRY-RUN ativado - dados serão processados mas não enviados ao banco"
            )

        try:
            # Processar leads
            success_records, error_records = self.process_leads()

            if self.dry_run:
                # Apenas mostrar resumo sem fazer upload
                total_processed = len(success_records) + len(error_records)
                logger.info("=== MODO DRY-RUN - Dados processados (não enviados) ===")
                logger.info(f"Total processado: {total_processed}")
                logger.info(f"Leads bem-sucedidos: {len(success_records)}")
                logger.info(f"Leads com erro: {len(error_records)}")
                if success_records:
                    logger.info(f"Exemplo de lead bem-sucedido: {success_records[0]}")
                if error_records:
                    logger.info(f"Exemplo de lead com erro: {error_records[0]}")
            else:
                # Fazer upload
                self.upload_to_database(success_records, error_records)

                # Resumo
                total_processed = len(success_records) + len(error_records)
                logger.info("=== Upload concluído com sucesso ===")
                logger.info(f"Total processado: {total_processed}")
                logger.info(f"Leads bem-sucedidos: {len(success_records)}")
                logger.info(f"Leads com erro: {len(error_records)}")

        except Exception as e:
            logger.error(f"Erro durante o processamento: {e}")
            raise


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


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Faz upload do histórico de leads para o banco de dados PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

# Upload básico
python src/upload_leads_history.py \\
  --success "input/todos_leads.xlsx" \\
  --errors "input/leads_com_erro.xlsx" \\
  --mesa "Mesa JPA"

# Com logging detalhado
python src/upload_leads_history.py \\
  --success "input/todos.xlsx" \\
  --errors "input/erros.xlsx" \\
  --mesa "Mesa 2B" \\
  --log-level DEBUG \\
  --log "logs/upload_history.log"
        """,
    )

    parser.add_argument(
        "--success",
        "-s",
        required=True,
        type=Path,
        help="Caminho para a planilha Excel com todos os leads",
    )

    parser.add_argument(
        "--errors",
        "-e",
        required=True,
        type=Path,
        help="Caminho para a planilha Excel com leads que falharam",
    )

    parser.add_argument(
        "--mesa",
        "-m",
        required=True,
        help="Nome da mesa referente aos leads",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa em modo dry-run (processa dados mas não envia ao banco)",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de log (padrão: INFO)",
    )

    parser.add_argument(
        "--log",
        type=Path,
        help="Caminho para o arquivo de log (opcional)",
    )

    args = parser.parse_args()

    # Configurar logging
    setup_logging(args.log_level, args.log)

    # Validar entradas
    if not args.success.exists():
        logger.error(f"Arquivo de sucesso não encontrado: {args.success}")
        sys.exit(1)

    if not args.errors.exists():
        logger.error(f"Arquivo de erros não encontrado: {args.errors}")
        sys.exit(1)

    # Validar conexão com banco (exceto em dry-run)
    if not args.dry_run:
        try:
            with DatabaseConnection():
                pass
        except Exception as e:
            logger.error(f"Erro na conexão com o banco de dados: {e}")
            logger.error(
                "Verifique as variáveis de ambiente DATABASE_URL ou DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"
            )
            sys.exit(1)

    try:
        # Executar upload
        uploader = LeadsHistoryUploader(
            success_file=str(args.success),
            errors_file=str(args.errors),
            mesa=args.mesa,
            dry_run=args.dry_run,
        )
        uploader.run()

        return 0

    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        if args.log_level.upper() == "DEBUG":
            import traceback

            logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
