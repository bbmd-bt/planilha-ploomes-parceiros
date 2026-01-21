"""
Testes para o script de upload do histórico de leads.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import tempfile
import os

from src.upload_leads_history import LeadsHistoryUploader, DatabaseConnection


class TestDatabaseConnection:
    """Testes para DatabaseConnection."""

    @patch.dict(
        os.environ,
        {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass",
        },
    )
    @patch("psycopg2.connect")
    def test_connect_with_separate_components(self, mock_connect):
        """Testa conexão usando componentes separados."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        with DatabaseConnection() as db:
            assert db.connection == mock_connection

        mock_connect.assert_called_once_with(
            host="localhost",
            port="5432",
            database="test_db",
            user="test_user",
            password="test_pass",
        )

    @patch.dict(
        os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost:5432/db"}
    )
    @patch("psycopg2.connect")
    def test_connect_with_database_url(self, mock_connect):
        """Testa conexão usando DATABASE_URL."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        with DatabaseConnection() as db:
            assert db.connection == mock_connection

        mock_connect.assert_called_once_with("postgresql://user:pass@localhost:5432/db")

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_environment_variables(self):
        """Testa erro quando variáveis de ambiente estão faltando."""
        with pytest.raises(ValueError, match="Variáveis de ambiente faltando"):
            DatabaseConnection()


class TestLeadsHistoryUploader:
    """Testes para LeadsHistoryUploader."""

    def create_test_excel(self, data: list, filename: str) -> str:
        """Cria um arquivo Excel temporário para teste."""
        df = pd.DataFrame(data)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        df.to_excel(temp_path, index=False)
        return temp_path

    def test_load_success_leads_valid(self):
        """Testa carregamento de planilha de sucesso válida."""
        data = [
            {"CNJ": "12345678901234567890", "Negociador": "João Silva"},
            {"CNJ": "09876543210987654321", "Negociador": "Maria Santos"},
        ]
        file_path = self.create_test_excel(data, "success_test.xlsx")

        try:
            uploader = LeadsHistoryUploader(file_path, "dummy_errors.xlsx", "Test Mesa")
            df = uploader.load_success_leads()

            assert len(df) == 2
            assert list(df.columns) == ["CNJ", "Negociador"]
            assert str(df.iloc[0]["CNJ"]) == "12345678901234567890"
            assert df.iloc[0]["Negociador"] == "João Silva"
        finally:
            os.remove(file_path)

    def test_load_success_leads_missing_columns(self):
        """Testa erro quando colunas obrigatórias estão faltando."""
        data = [{"Nome": "João Silva"}]  # Sem CNJ e Negociador
        file_path = self.create_test_excel(data, "invalid_success.xlsx")

        try:
            uploader = LeadsHistoryUploader(file_path, "dummy_errors.xlsx", "Test Mesa")
            with pytest.raises(ValueError, match="Erro ao ler planilha de sucesso"):
                uploader.load_success_leads()
        finally:
            os.remove(file_path)

    def test_load_error_leads_valid(self):
        """Testa carregamento de planilha de erros válida."""
        data = [
            {
                "CNJ": "12345678901234567890",
                "Negociador": "João Silva",
                "Erro": "Erro de validação",
            }
        ]
        file_path = self.create_test_excel(data, "errors_test.xlsx")

        try:
            uploader = LeadsHistoryUploader(
                "dummy_success.xlsx", file_path, "Test Mesa"
            )
            df = uploader.load_error_leads()

            assert len(df) == 1
            assert list(df.columns) == ["CNJ", "Negociador", "Erro"]
            assert df.iloc[0]["Erro"] == "Erro de validação"
        finally:
            os.remove(file_path)

    def test_process_leads(self):
        """Testa processamento completo dos leads."""
        # Criar planilha de sucesso
        success_data = [
            {"CNJ": "11111111111111111111", "Negociador": "João Silva"},
            {"CNJ": "22222222222222222222", "Negociador": "Maria Santos"},
            {
                "CNJ": "33333333333333333333",
                "Negociador": "Pedro Costa",
            },  # Este terá erro
        ]
        success_file = self.create_test_excel(success_data, "success_process.xlsx")

        # Criar planilha de erros
        error_data = [
            {
                "CNJ": "33333333333333333333",
                "Negociador": "Pedro Costa",
                "Erro": "CNJ inválido",
            }
        ]
        error_file = self.create_test_excel(error_data, "errors_process.xlsx")

        try:
            uploader = LeadsHistoryUploader(success_file, error_file, "Test Mesa")
            success_records, error_records = uploader.process_leads()

            # Verificar leads bem-sucedidos (2 registros)
            assert len(success_records) == 2
            assert success_records[0] == (
                "11111111111111111111",
                "João Silva",
                "Test Mesa",
                False,
                None,
            )
            assert success_records[1] == (
                "22222222222222222222",
                "Maria Santos",
                "Test Mesa",
                False,
                None,
            )

            # Verificar leads com erro (1 registro)
            assert len(error_records) == 1
            assert error_records[0] == (
                "33333333333333333333",
                "Pedro Costa",
                "Test Mesa",
                True,
                "CNJ inválido",
            )

        finally:
            os.remove(success_file)
            os.remove(error_file)

    @patch("src.upload_leads_history.DatabaseConnection")
    def test_upload_to_database(self, mock_db_class):
        """Testa upload para o banco de dados."""
        mock_db = MagicMock()
        mock_db_class.return_value.__enter__.return_value = mock_db

        uploader = LeadsHistoryUploader(
            "dummy_success.xlsx", "dummy_errors.xlsx", "Test Mesa"
        )

        success_records = [("111", "João", "Test Mesa", False, None)]
        error_records = [("222", "Maria", "Test Mesa", True, "Erro teste")]

        uploader.upload_to_database(success_records, error_records)

        # Verificar que execute_many foi chamado
        mock_db.execute_many.assert_called_once()

        # Verificar os argumentos
        call_args = mock_db.execute_many.call_args
        query = call_args[0][0]
        data = call_args[0][1]

        assert "INSERT INTO leads_parceiros_upload_history" in query
        assert "ON CONFLICT (cnj)" in query
        assert len(data) == 2
