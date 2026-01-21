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
        data = {
            "CNJ": ["12345678901234567890", "09876543210987654321", ""],
            "Erro": ["Erro 1", "Erro 2", ""],
        }
        df = pd.DataFrame(data)

        # Salva em arquivo temporário
        with patch("pandas.read_excel") as mock_read:
            mock_read.return_value = df

            cnjs, cnj_errors = PloomesSync.load_cnjs_from_excel("fake.xlsx")

            assert len(cnjs) == 2
            assert "12345678901234567890" in cnjs
            assert "09876543210987654321" in cnjs
            assert cnj_errors.get("12345678901234567890") == "Erro 1"
            assert cnj_errors.get("09876543210987654321") == "Erro 2"

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
        target_stage_deals = [
            {
                "Id": 123,
                "StageId": 999,
                "OtherProperties": [
                    {
                        "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                        "StringValue": "11111111111111111",
                    }
                ],
            },
            {
                "Id": 456,
                "StageId": 999,
                "OtherProperties": [
                    {
                        "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                        "StringValue": "22222222222222222",
                    }
                ],
            },
        ]

        deletion_stage_deals = [
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

        # Mock para search_deals_by_cnj - retorna o deal correspondente
        def mock_search_deals_by_cnj(cnj):
            if cnj == "11111111111111111":
                return [deletion_stage_deals[0]]
            return []

        # Mock para search_deals_by_stage - retorna deals com base no estágio
        def mock_search_deals_by_stage(stage_id):
            if stage_id == 999:  # target_stage
                return target_stage_deals
            elif stage_id == 888:  # deletion_stage
                return deletion_stage_deals
            return []

        mock_client.search_deals_by_cnj.side_effect = mock_search_deals_by_cnj
        mock_client.search_deals_by_stage.side_effect = mock_search_deals_by_stage
        mock_client.update_deal_stage.return_value = True
        mock_client.delete_deal.side_effect = [
            True,
            True,
            True,
        ]  # 2 no target_stage + 1 preservado

        cnj_list = ["11111111111111111"]
        report = sync.process_cnj_list(cnj_list)

        assert report.total_processed == 1
        assert report.successfully_moved == 1  # Deal foi movido para target_stage
        # O target_stage é limpo automaticamente (2 deals deletados)
        # successfully_deleted conta apenas deleções no deletion_stage após preservação
        assert report.skipped_deletions == 0

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

    def test_move_origin_deal_with_error_description(self, sync, mock_client):
        """Testa movimento de deal de origem com descrição de erro (Interaction Record)."""
        # Setup
        deal = {
            "Id": 123,
            "OriginDealId": 456,
            "OtherProperties": [
                {
                    "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                    "StringValue": "12345678901234567890",
                }
            ],
        }

        origin_deal = {"Id": 456, "PipelineId": 110065217}

        # Configurar mocks
        mock_client.get_deal_by_id.return_value = origin_deal
        mock_client.create_interaction_record.return_value = (
            789  # ID da interaction criada
        )
        mock_client.update_deal_last_interaction_record.return_value = True
        mock_client.update_deal_stage.return_value = True

        # Configurar configuração de origem
        sync.origin_config = {
            "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811}
        }

        # Executar
        error_msg = "Campo CNJ inválido"
        sync._move_origin_deal(deal, error_description=error_msg)

        # Verificar chamadas
        mock_client.get_deal_by_id.assert_called_once_with(456)
        mock_client.create_interaction_record.assert_called_once_with(456, error_msg)
        mock_client.update_deal_last_interaction_record.assert_called_once_with(
            456, 789
        )
        mock_client.update_deal_stage.assert_called_once_with(456, 110352811)

    def test_sync_with_cnj_errors(self, mock_client):
        """Testa sincronizador com mapeamento de erros de CNJs."""
        # Criar sincronizador com erros
        cnj_errors = {
            "12345678901234567890": "Erro de validação",
            "09876543210987654321": "CNJ duplicado",
        }
        sync = PloomesSync(
            mock_client,
            target_stage_id=999,
            deletion_stage_id=888,
            cnj_errors=cnj_errors,
        )

        # Verificar que os erros foram armazenados
        assert sync.cnj_errors["12345678901234567890"] == "Erro de validação"
        assert sync.cnj_errors["09876543210987654321"] == "CNJ duplicado"

    def test_move_origin_deal_already_in_correct_stage_without_error(
        self, sync, mock_client
    ):
        """Testa movimento de deal já no estágio correto sem descrição de erro."""
        # Setup
        deal = {
            "Id": 123,
            "OriginDealId": 456,
            "OtherProperties": [
                {
                    "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                    "StringValue": "12345678901234567890",
                }
            ],
        }

        origin_deal = {"Id": 456, "PipelineId": 110065217, "StageId": 110352811}

        # Configurar mocks
        mock_client.get_deal_by_id.return_value = origin_deal
        mock_client.update_deal_stage.return_value = True

        # Configurar configuração de origem
        sync.origin_config = {
            "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811}
        }

        # Executar
        sync._move_origin_deal(deal)

        # Verificar que não foi chamado update_deal_stage
        mock_client.update_deal_stage.assert_not_called()
        mock_client.create_interaction_record.assert_not_called()

    def test_move_origin_deal_already_in_correct_stage_with_error_and_no_interaction(
        self, sync, mock_client
    ):
        """Testa movimento de deal já no estágio correto com erro mas sem Interaction Record."""
        # Setup
        deal = {
            "Id": 123,
            "OriginDealId": 456,
            "OtherProperties": [
                {
                    "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                    "StringValue": "12345678901234567890",
                }
            ],
        }

        origin_deal = {
            "Id": 456,
            "PipelineId": 110065217,
            "StageId": 110352811,
            "LastInteractionRecordId": None,  # Sem interaction record
        }

        # Configurar mocks
        mock_client.get_deal_by_id.return_value = origin_deal
        mock_client.create_interaction_record.return_value = 789
        mock_client.update_deal_last_interaction_record.return_value = True
        mock_client.update_deal_stage.return_value = True

        # Configurar configuração de origem
        sync.origin_config = {
            "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811}
        }

        # Executar
        error_msg = "Erro de validação"
        sync._move_origin_deal(deal, error_description=error_msg)

        # Verificar chamadas
        mock_client.get_deal_by_id.assert_called_once_with(456)
        mock_client.create_interaction_record.assert_called_once_with(456, error_msg)
        mock_client.update_deal_last_interaction_record.assert_called_once_with(
            456, 789
        )
        # Não deve mover já que está no estágio correto
        mock_client.update_deal_stage.assert_not_called()

    def test_move_origin_deal_already_in_correct_stage_with_error_and_existing_interaction(
        self, sync, mock_client
    ):
        """Testa movimento de deal já no estágio correto com erro e Interaction Record existente."""
        # Setup
        deal = {
            "Id": 123,
            "OriginDealId": 456,
            "OtherProperties": [
                {
                    "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                    "StringValue": "12345678901234567890",
                }
            ],
        }

        origin_deal = {
            "Id": 456,
            "PipelineId": 110065217,
            "StageId": 110352811,
            "LastInteractionRecordId": 789,  # Já possui interaction record
        }

        # Configurar mocks
        mock_client.get_deal_by_id.return_value = origin_deal
        mock_client.update_deal_stage.return_value = True

        # Configurar configuração de origem
        sync.origin_config = {
            "Mesa JPA": {"pipeline_id": 110065217, "stage_id": 110352811}
        }

        # Executar
        error_msg = "Erro de validação"
        sync._move_origin_deal(deal, error_description=error_msg)

        # Verificar chamadas
        mock_client.get_deal_by_id.assert_called_once_with(456)
        # Não deve criar novo interaction record
        mock_client.create_interaction_record.assert_not_called()
        # Não deve fazer move já que está no estágio correto
        mock_client.update_deal_stage.assert_not_called()

    def test_delete_all_deals_in_target_stage_success(self, sync, mock_client):
        """Testa deleção de negócios no estágio alvo."""
        # Setup
        deals = [
            {"Id": 1},
            {"Id": 2},
            {"Id": 3},
        ]
        mock_client.search_deals_by_stage.return_value = deals
        mock_client.delete_deal.return_value = True

        # Executar
        result = sync._delete_all_deals_in_target_stage()

        # Verificações
        assert result["deleted"] == 3
        assert result["skipped"] == 0
        mock_client.search_deals_by_stage.assert_called_once_with(999)
        assert mock_client.delete_deal.call_count == 3

    def test_delete_all_deals_in_target_stage_with_failures(self, sync, mock_client):
        """Testa deleção com falhas parciais."""
        # Setup
        deals = [
            {"Id": 1},
            {"Id": 2},
            {"Id": 3},
        ]
        mock_client.search_deals_by_stage.return_value = deals
        # 1º sucesso, 2º falha, 3º sucesso
        mock_client.delete_deal.side_effect = [True, False, True]

        # Executar
        result = sync._delete_all_deals_in_target_stage()

        # Verificações
        assert result["deleted"] == 2
        assert result["skipped"] == 1

    def test_delete_all_deals_in_target_stage_empty(self, sync, mock_client):
        """Testa deleção quando estágio alvo está vazio."""
        # Setup
        mock_client.search_deals_by_stage.return_value = []

        # Executar
        result = sync._delete_all_deals_in_target_stage()

        # Verificações
        assert result["deleted"] == 0
        assert result["skipped"] == 0
        mock_client.delete_deal.assert_not_called()

    def test_delete_all_deals_in_target_stage_dry_run(self, mock_client):
        """Testa deleção em modo dry-run."""
        # Setup com dry_run=True
        sync = PloomesSync(
            mock_client, target_stage_id=999, deletion_stage_id=888, dry_run=True
        )
        deals = [
            {"Id": 1},
            {"Id": 2},
        ]
        mock_client.search_deals_by_stage.return_value = deals

        # Executar
        result = sync._delete_all_deals_in_target_stage()

        # Verificações
        assert result["deleted"] == 2  # Contados como "deletados" em dry-run
        assert result["skipped"] == 0
        # Não deve chamar delete_deal em dry-run
        mock_client.delete_deal.assert_not_called()
