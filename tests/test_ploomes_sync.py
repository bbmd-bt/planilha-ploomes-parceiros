"""
Testes para o módulo de sincronização Ploomes.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from ploomes_sync import PloomesSync, ProcessingResult, SyncReport
from ploomes_client import PloomesClient


class TestPloomesSync:
    """Testes para PloomesSync."""

    @pytest.fixture
    def mock_client(self):
        """Fixture que retorna um cliente mockado."""
        return Mock(spec=PloomesClient)

    @pytest.fixture
    def sync(self, mock_client):
        """Fixture que retorna uma instância do sincronizador."""
        return PloomesSync(mock_client, target_stage_id=999, deletion_stage_id=888)

    def test_load_cnjs_from_excel_success(self):
        """Testa carregamento de CNJs de arquivo Excel."""
        # Cria DataFrame de teste
        data = {
            "CNJ": ["12345678901234567890", "09876543210987654321", ""]
        }
        df = pd.DataFrame(data)

        # Salva em arquivo temporário
        with patch('pandas.read_excel') as mock_read:
            mock_read.return_value = df

            cnjs = PloomesSync.load_cnjs_from_excel("fake.xlsx")

            assert len(cnjs) == 2
            assert "12345678901234567890" in cnjs
            assert "09876543210987654321" in cnjs

    def test_load_cnjs_from_excel_no_cnj_column(self):
        """Testa erro quando não há coluna CNJ."""
        data = {"Nome": ["João", "Maria"]}
        df = pd.DataFrame(data)

        with patch('pandas.read_excel') as mock_read:
            mock_read.return_value = df

            with pytest.raises(ValueError, match="Coluna 'CNJ' não encontrada"):
                PloomesSync.load_cnjs_from_excel("fake.xlsx")

    def test_process_single_cnj_success(self, sync, mock_client):
        """Testa processamento bem-sucedido de um CNJ."""
        # Configura mocks - negócio no estágio de deleção
        mock_client.search_deals_by_cnj.return_value = [{"Id": 123, "StageId": 888}]
        mock_client.delete_deal.return_value = True

        result = sync._process_single_cnj("12345678901234567890")

        assert result.cnj == "12345678901234567890"
        assert result.deal_id == 123
        assert result.deleted_successfully is True
        assert result.error_message is None

        # Verifica chamadas
        mock_client.search_deals_by_cnj.assert_called_once_with("12345678901234567890")
        mock_client.delete_deal.assert_called_once_with(123)
        mock_client.update_deal_stage.assert_not_called()

    def test_process_single_cnj_not_found(self, sync, mock_client):
        """Testa processamento quando negócio não é encontrado."""
        mock_client.search_deals_by_cnj.return_value = []

        result = sync._process_single_cnj("99999999999999999999")

        assert result.cnj == "99999999999999999999"
        assert result.deal_id is None
        assert result.moved_successfully is False
        assert result.deleted_successfully is False
        assert result.error_message == "Negócio não encontrado"

    def test_process_single_cnj_not_in_deletion_stage(self, sync, mock_client):
        """Testa processamento quando negócio não está no estágio de deleção."""
        mock_client.search_deals_by_cnj.return_value = [{"Id": 123, "StageId": 456}]

        result = sync._process_single_cnj("12345678901234567890")

        assert result.deal_id == 123
        assert result.deleted_successfully is False
        assert "não está no estágio de deleção" in result.error_message

        # Verifica que delete não foi chamado
        mock_client.delete_deal.assert_not_called()

    def test_process_single_cnj_multiple_deals(self, sync, mock_client):
        """Testa processamento quando múltiplos negócios são encontrados (deve usar o segundo)."""
        mock_client.search_deals_by_cnj.return_value = [
            {"Id": 123, "StageId": 456},
            {"Id": 789, "StageId": 888}  # No estágio de deleção
        ]
        mock_client.delete_deal.return_value = True

        result = sync._process_single_cnj("12345678901234567890")

        assert result.cnj == "12345678901234567890"
        assert result.deal_id == 789  # Deve usar o segundo negócio
        assert result.deleted_successfully is True
        assert result.error_message is None

        # Verifica chamadas com o ID do segundo negócio
        mock_client.delete_deal.assert_called_once_with(789)

    def test_process_cnj_list(self, sync, mock_client):
        """Testa processamento de lista de CNJs."""
        # Configura mocks para dois CNJs: um no estágio errado, outro não encontrado
        mock_client.search_deals_by_cnj.side_effect = [
            [{"Id": 123, "StageId": 456}],  # Primeiro CNJ encontrado mas não no estágio de deleção
            []  # Segundo CNJ não encontrado
        ]

        cnj_list = ["12345678901234567890", "99999999999999999999"]
        report = sync.process_cnj_list(cnj_list)

        assert report.total_processed == 2
        assert report.successfully_deleted == 0
        assert report.failed_movements == 0
        assert report.skipped_deletions == 1
        assert len(report.results) == 2

    @patch('ploomes_sync.pd.DataFrame.to_excel')
    @patch('ploomes_sync.pd.ExcelWriter')
    def test_generate_report_excel(self, mock_writer, mock_to_excel, sync):
        """Testa geração de relatório Excel."""
        # Cria relatório de teste
        results = [
            ProcessingResult("12345678901234567890", 123, True, True, None),
            ProcessingResult("99999999999999999999", None, False, False, "Não encontrado")
        ]
        report = SyncReport(
            total_processed=2,
            successfully_moved=1,
            successfully_deleted=1,
            failed_movements=1,
            skipped_deletions=0,
            results=results
        )

        sync.generate_report_excel(report, "output/test.xlsx")

        # Verifica que ExcelWriter foi chamado
        mock_writer.assert_called_once_with("output/test.xlsx", engine='openpyxl')

        # Verifica que to_excel foi chamado duas vezes (uma para cada aba)
        assert mock_to_excel.call_count == 2
