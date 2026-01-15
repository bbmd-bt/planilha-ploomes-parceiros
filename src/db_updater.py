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
                payload = json.loads(row['payload'])
                # Assuming payload has 'office' and 'negotiator' fields
                if 'office' in payload:
                    office_data = payload['office']
                    if isinstance(office_data, dict):
                        office_id = office_data.get('id')
                        office_name = office_data.get('name')
                        if office_id and office_name:
                            offices[str(office_id)] = office_name
                if 'negotiator' in payload:
                    neg_data = payload['negotiator']
                    if isinstance(neg_data, dict):
                        neg_id = neg_data.get('id')
                        neg_name = neg_data.get('name')
                        if neg_id and neg_name:
                            negotiators[str(neg_id)] = neg_name
            except json.JSONDecodeError:
                self.logger.warning(f"Payload inválido: {row['payload']}")
                continue
        return offices, negotiators

    def update_json_files(self, offices, negotiators):
        # Update utils/escritorios.json
        offices_file = self.base_dir / 'utils' / 'escritorios.json'
        try:
            with open(offices_file, 'w', encoding='utf-8') as f:
                json.dump(offices, f, ensure_ascii=False, indent=2)
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