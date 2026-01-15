import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from pathlib import Path
from typing import Dict, List
import logging
from dotenv import load_dotenv


class DatabaseUpdateError(Exception):
    pass


class DatabaseUpdater:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.base_dir = Path(__file__).parent.parent
        self.logger = logging.getLogger(__name__)

    def connect(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            self.logger.info("Conectado ao banco de dados com sucesso.")
            return conn
        except Exception as e:
            raise DatabaseUpdateError(f"Erro ao conectar ao banco de dados: {e}")

    def fetch_data(self, conn):
        query = """
        SELECT payload
        FROM lead_snapshot
        WHERE payload IS NOT NULL
        """
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
            self.logger.info(f"Recuperados {len(rows)} registros do banco de dados.")
            return rows
        except Exception as e:
            raise DatabaseUpdateError(f"Erro ao executar query: {e}")

    def parse_payloads(self, rows):
        offices = {}
        negotiators = {}
        for row in rows:
            try:
                if isinstance(row['payload'], str):
                    payload = json.loads(row['payload'])
                elif isinstance(row['payload'], dict):
                    payload = row['payload']
                else:
                    self.logger.warning(f"Payload não é string nem dict: {type(row['payload'])}")
                    continue
                # Assuming payload has 'escritorio_responsavel' and 'negociador' fields
                if 'escritorio_responsavel' in payload:
                    office_name = payload['escritorio_responsavel']
                    if isinstance(office_name, str) and office_name.strip():
                        offices[office_name.strip()] = office_name.strip()
                else:
                    self.logger.warning(f"Payload sem 'escritorio_responsavel': {list(payload.keys())}")
                if 'negociador' in payload:
                    neg_name = payload['negociador']
                    if isinstance(neg_name, str) and neg_name.strip():
                        negotiators[neg_name.strip()] = neg_name.strip()
                else:
                    self.logger.warning(f"Payload sem 'negociador': {list(payload.keys())}")
            except (json.JSONDecodeError, TypeError):
                self.logger.warning(f"Payload inválido: {row['payload']}")
                continue
        return offices, negotiators

    def update_json_files(self, offices, negotiators):
        # Update utils/escritorios.json
        offices_file = self.base_dir / 'utils' / 'escritorios.json'
        try:
            data = {"escritorios": offices, "total": len(offices)}
            with open(offices_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Arquivo {offices_file} atualizado com {len(offices)} escritórios.")
        except Exception as e:
            raise DatabaseUpdateError(f"Erro ao salvar escritórios: {e}")

        # Update utils/negociadores.json
        neg_file = self.base_dir / 'utils' / 'negociadores.json'
        try:
            with open(neg_file, 'w', encoding='utf-8') as f:
                json.dump(negotiators, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Arquivo {neg_file} atualizado com {len(negotiators)} negociadores.")
        except Exception as e:
            raise DatabaseUpdateError(f"Erro ao salvar negociadores: {e}")

    def update_database(self):
        conn = None
        try:
            conn = self.connect()
            rows = self.fetch_data(conn)
            offices, negotiators = self.parse_payloads(rows)
            self.update_json_files(offices, negotiators)
            self.logger.info("Atualização do banco de dados concluída com sucesso.")
        except DatabaseUpdateError:
            raise
        except Exception as e:
            raise DatabaseUpdateError(f"Erro inesperado durante atualização: {e}")
        finally:
            if conn:
                conn.close()
                self.logger.info("Conexão com o banco de dados fechada.")
