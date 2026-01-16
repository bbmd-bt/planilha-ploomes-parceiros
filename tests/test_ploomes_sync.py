"""
Testes para o módulo de sincronização Ploomes.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from src.ploomes_sync import PloomesSync, SyncReport
from src.ploomes_client import PloomesClient


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
        data = {"CNJ": ["12345678901234567890", "09876543210987654321", ""]}
        df = pd.DataFrame(data)

        # Salva em arquivo temporário
        with patch("pandas.read_excel") as mock_read:
            mock_read.return_value = df

            cnjs = PloomesSync.load_cnjs_from_excel("fake.xlsx")

            assert len(cnjs) == 2
            assert "12345678901234567890" in cnjs
            assert "09876543210987654321" in cnjs

    def test_load_cnjs_from_excel_no_cnj_column(self):
        """Testa erro quando não há coluna CNJ."""
        data = {"Nome": ["João", "Maria"]}
        df = pd.DataFrame(data)

        with patch("pandas.read_excel") as mock_read:
            mock_read.return_value = df

            with pytest.raises(ValueError, match="Coluna 'CNJ' não encontrada"):
                PloomesSync.load_cnjs_from_excel("fake.xlsx")

    def test_process_cnj_list(self, sync, mock_client):
        """Testa processamento de lista de CNJs para limpeza de negócios antigos."""
        # Mock para buscar deals antigos no estágio de deleção
        mock_client.search_deals_by_stage.return_value = [
            {
                "Id": 123,
                "StageId": 888,
                "OtherProperties": [
                    {
                        "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                        "StringValue": "11111111111111111",
                    }
                ],
            },
            {
                "Id": 456,
                "StageId": 888,
                "OtherProperties": [
                    {
                        "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                        "StringValue": "22222222222222222",
                    }
                ],
            },
        ]

        # Mock para search_deals_by_cnj - retorna o deal correspondente
        def mock_search_deals_by_cnj(cnj):
            if cnj == "11111111111111111":
                return [
                    {
                        "Id": 123,
                        "StageId": 888,
                        "OtherProperties": [
                            {
                                "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                                "StringValue": "11111111111111111",
                            }
                        ],
                    }
                ]
            return []

        mock_client.search_deals_by_cnj.side_effect = mock_search_deals_by_cnj
        mock_client.delete_deal.side_effect = [True, True]

        cnj_list = ["11111111111111111"]  # Apenas o primeiro deve ser preservado
        report = sync.process_cnj_list(cnj_list)

        assert report.total_processed == 1
        assert report.successfully_deleted == 1  # Apenas o segundo foi deletado
        assert report.skipped_deletions == 0

        # Verifica que apenas o deal 456 foi deletado
        mock_client.delete_deal.assert_called_once_with(456)

    @patch("src.ploomes_sync.pd.DataFrame.to_excel")
    @patch("src.ploomes_sync.pd.ExcelWriter")
    def test_generate_report_excel(self, mock_writer, mock_to_excel, sync):
        """Testa geração de relatório Excel."""
        # Cria relatório de teste
        report = SyncReport(
            total_processed=5, successfully_deleted=3, skipped_deletions=1
        )

        sync.generate_report_excel(report, "output/test.xlsx")

        # Verifica que ExcelWriter foi chamado
        mock_writer.assert_called_once_with("output/test.xlsx", engine="openpyxl")

        # Verifica que to_excel foi chamado
        assert mock_to_excel.call_count == 1
