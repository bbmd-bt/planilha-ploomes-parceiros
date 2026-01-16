import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from pathlib import Path
import time

from loguru import logger
from dotenv import load_dotenv


class DatabaseUpdateError(Exception):
    pass


class DatabaseUpdater:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        self.base_dir = Path(__file__).parent.parent

    def connect(self):
        logger.debug("Tentando conectar ao banco de dados.")
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.info("Conectado ao banco de dados com sucesso.")
            return conn
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise DatabaseUpdateError(f"Erro ao conectar ao banco de dados: {e}")

    def fetch_data(self, conn, batch_size=1000):
        logger.debug("Executando query para recuperar dados do lead_snapshot em lotes.")
        offset = 0
        total_rows = 0
        while True:
            query = """
            SELECT payload
            FROM lead_snapshot
            WHERE payload IS NOT NULL
            LIMIT %s OFFSET %s
            """
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, (batch_size, offset))
                    rows = cursor.fetchall()
                if not rows:
                    break
                total_rows += len(rows)
                yield rows
                offset += batch_size
            except Exception as e:
                logger.error(f"Erro ao executar query em lote: {e}")
                raise DatabaseUpdateError(f"Erro ao executar query: {e}")
        logger.info(f"Recuperados {total_rows} registros do banco de dados.")

    def parse_payloads_batch(self, rows):
        offices = set()
        negotiators = set()
        processed = 0
        skipped_invalid_type = 0
        skipped_missing_office = 0
        skipped_missing_negotiator = 0
        skipped_json_error = 0
        for row in rows:
            try:
                if isinstance(row["payload"], str):
                    payload = json.loads(row["payload"])
                elif isinstance(row["payload"], dict):
                    payload = row["payload"]
                else:
                    skipped_invalid_type += 1
                    continue
                # Assuming payload has 'escritorio_responsavel' and 'negociador' fields
                has_office = False
                if "escritorio_responsavel" in payload:
                    office_name = payload["escritorio_responsavel"]
                    if isinstance(office_name, str) and office_name.strip():
                        offices.add(office_name.strip())
                        has_office = True
                if not has_office:
                    skipped_missing_office += 1
                has_negotiator = False
                if "negociador" in payload:
                    neg_name = payload["negociador"]
                    if isinstance(neg_name, str) and neg_name.strip():
                        negotiators.add(neg_name.strip())
                        has_negotiator = True
                if not has_negotiator:
                    skipped_missing_negotiator += 1
                processed += 1
            except (json.JSONDecodeError, TypeError):
                skipped_json_error += 1
                continue
        return (
            offices,
            negotiators,
            processed,
            skipped_invalid_type,
            skipped_missing_office,
            skipped_missing_negotiator,
            skipped_json_error,
        )

    def update_json_files(self, offices, negotiators):
        logger.debug("Iniciando atualização dos arquivos JSON.")
        # Update utils/escritorios.json
        offices_file = self.base_dir / "utils" / "escritorios.json"
        try:
            data = {"escritorios": offices, "total": len(offices)}
            with open(offices_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(
                f"Arquivo {offices_file} atualizado com {len(offices)} escritórios."
            )
        except Exception as e:
            logger.error(f"Erro ao salvar escritórios: {e}")
            raise DatabaseUpdateError(f"Erro ao salvar escritórios: {e}")

        # Update utils/negociadores.json
        neg_file = self.base_dir / "utils" / "negociadores.json"
        try:
            with open(neg_file, "w", encoding="utf-8") as f:
                json.dump(negotiators, f, ensure_ascii=False, indent=2)
            logger.info(
                f"Arquivo {neg_file} atualizado com {len(negotiators)} negociadores."
            )
        except Exception as e:
            logger.error(f"Erro ao salvar negociadores: {e}")
            raise DatabaseUpdateError(f"Erro ao salvar negociadores: {e}")

    def update_database(self):
        start_time = time.time()
        logger.info("Iniciando atualização do banco de dados.")
        conn = None
        try:
            conn = self.connect()
            all_offices = set()
            all_negotiators = set()
            total_processed = 0
            total_skipped_invalid_type = 0
            total_skipped_missing_office = 0
            total_skipped_missing_negotiator = 0
            total_skipped_json_error = 0
            for batch in self.fetch_data(conn):
                (
                    offices,
                    negotiators,
                    processed,
                    skipped_invalid_type,
                    skipped_missing_office,
                    skipped_missing_negotiator,
                    skipped_json_error,
                ) = self.parse_payloads_batch(batch)
                all_offices.update(offices)
                all_negotiators.update(negotiators)
                total_processed += processed
                total_skipped_invalid_type += skipped_invalid_type
                total_skipped_missing_office += skipped_missing_office
                total_skipped_missing_negotiator += skipped_missing_negotiator
                total_skipped_json_error += skipped_json_error
            total_skipped = (
                total_skipped_invalid_type
                + total_skipped_missing_office
                + total_skipped_missing_negotiator
                + total_skipped_json_error
            )
            logger.info(
                f"Parsing total concluído: {total_processed} processados, {total_skipped} pulados "
                f"(tipo inválido: {total_skipped_invalid_type}, sem escritório: {total_skipped_missing_office}, "
                f"sem negociador: {total_skipped_missing_negotiator}, erro JSON: {total_skipped_json_error}). "
                f"Escritórios únicos: {len(all_offices)}, Negociadores únicos: {len(all_negotiators)}."
            )
            self.update_json_files(
                dict.fromkeys(all_offices), dict.fromkeys(all_negotiators)
            )
            elapsed = time.time() - start_time
            logger.info(
                f"Atualização do banco de dados concluída com sucesso em {elapsed:.2f} segundos."
            )
        except DatabaseUpdateError:
            raise
        except Exception as e:
            logger.error(f"Erro inesperado durante atualização: {e}")
            raise DatabaseUpdateError(f"Erro inesperado durante atualização: {e}")
        finally:
            if conn:
                conn.close()
                logger.debug("Conexão com o banco de dados fechada.")
