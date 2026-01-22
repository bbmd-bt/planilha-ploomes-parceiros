"""
Testes para o módulo de validação de Interaction Records.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock
import pandas as pd
import tempfile

# Importar as classes do módulo
from src.validate_interactions import (
    InteractionValidator,
    InteractionValidationResult,
    InteractionValidationReport,
)


class TestInteractionValidator:
    """Testes para a classe InteractionValidator."""

    def test_load_cnj_errors_from_excel_success(self):
        """Testa carregamento bem-sucedido de CNJs e erros do Excel."""
        # Criar arquivo temporário com dados de teste
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            data = {
                "CNJ": ["1234567-89.0123.4.56.7890", "9876543-21.9876.5.43.2109"],
                "Erro": ["Erro 1", "Erro 2"],
            }
            df = pd.DataFrame(data)
            df.to_excel(tmp.name, index=False)

            # Testar carregamento
            result = InteractionValidator.load_cnj_errors_from_excel(tmp.name)

            assert len(result) == 2
            assert result["1234567-89.0123.4.56.7890"] == "Erro 1"
            assert result["9876543-21.9876.5.43.2109"] == "Erro 2"

            # Limpar
            Path(tmp.name).unlink()

    def test_load_cnj_errors_missing_cnj_column(self):
        """Testa erro quando coluna CNJ está faltando."""
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            data = {"Erro": ["Erro 1", "Erro 2"]}
            df = pd.DataFrame(data)
            df.to_excel(tmp.name, index=False)

            with pytest.raises(ValueError, match="Coluna 'CNJ' não encontrada"):
                InteractionValidator.load_cnj_errors_from_excel(tmp.name)

            Path(tmp.name).unlink()

    def test_extract_cnj_from_deal_via_other_properties(self):
        """Testa extração de CNJ via OtherProperties."""
        deal = {
            "OtherProperties": [
                {
                    "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                    "StringValue": "1234567-89.0123.4.56.7890",
                }
            ]
        }

        result = InteractionValidator._extract_cnj_from_deal(deal)
        assert result == "1234567-89.0123.4.56.7890"

    def test_extract_cnj_from_deal_via_title(self):
        """Testa extração de CNJ via título do deal."""
        deal = {
            "Title": "Deal 1234567-89.0123.4.56.7890",
            "OtherProperties": [],
        }

        result = InteractionValidator._extract_cnj_from_deal(deal)
        assert result == "1234567-89.0123.4.56.7890"

    def test_extract_cnj_from_deal_not_found(self):
        """Testa quando CNJ não é encontrado no deal."""
        deal = {
            "Title": "Deal sem CNJ",
            "OtherProperties": [],
        }

        result = InteractionValidator._extract_cnj_from_deal(deal)
        assert result is None

    def test_interaction_exists_for_error_true(self):
        """Testa verificação de interaction existente."""
        mock_client = Mock()
        mock_client.get_deal_by_id.return_value = {
            "Id": 123,
            "LastInteractionRecordId": 456,
        }

        validator = InteractionValidator(mock_client, {})
        result = validator._interaction_exists_for_error(123, "Erro teste")

        assert result is True
        mock_client.get_deal_by_id.assert_called_once_with(123)

    def test_interaction_exists_for_error_false(self):
        """Testa quando interaction não existe."""
        mock_client = Mock()
        mock_client.get_deal_by_id.return_value = {
            "Id": 123,
            "LastInteractionRecordId": None,
        }

        validator = InteractionValidator(mock_client, {})
        result = validator._interaction_exists_for_error(123, "Erro teste")

        assert result is False

    def test_interaction_exists_for_error_empty_description(self):
        """Testa com descrição de erro vazia."""
        mock_client = Mock()
        validator = InteractionValidator(mock_client, {})
        result = validator._interaction_exists_for_error(123, "")

        assert result is False
        mock_client.get_deal_by_id.assert_not_called()

    def test_validate_interactions_in_stage_no_deals(self):
        """Testa validação quando nenhum deal é encontrado."""
        mock_client = Mock()
        mock_client.search_deals_by_stage.return_value = []

        validator = InteractionValidator(mock_client, {})
        report = validator.validate_interactions_in_stage(110351653)

        assert report.total_deals == 0
        assert len(report.results) == 0

    def test_validate_interactions_in_stage_with_deals(self):
        """Testa validação com deals encontrados."""
        mock_client = Mock()
        mock_client.search_deals_by_stage.return_value = [
            {
                "Id": 123,
                "Title": "Deal 1234567-89.0123.4.56.7890",
                "OtherProperties": [],
            },
            {
                "Id": 456,
                "Title": "Deal 9876543-21.9876.5.43.2109",
                "OtherProperties": [],
            },
        ]
        mock_client.get_deal_by_id.return_value = None

        cnj_errors = {
            "1234567-89.0123.4.56.7890": "Erro 1",
            "9876543-21.9876.5.43.2109": "Erro 2",
        }

        validator = InteractionValidator(mock_client, cnj_errors)
        report = validator.validate_interactions_in_stage(110351653)

        assert report.total_deals == 2

    def test_validate_single_deal_success(self):
        """Testa validação e criação de interaction bem-sucedida."""
        mock_client = Mock()
        mock_client.get_deal_by_id.return_value = None  # No existing interaction
        mock_client.create_interaction_record.return_value = 789
        mock_client.update_deal_last_interaction_record.return_value = True

        deal = {
            "Id": 123,
            "Title": "Deal 1234567-89.0123.4.56.7890",
            "OtherProperties": [],
        }

        cnj_errors = {"1234567-89.0123.4.56.7890": "Erro teste"}

        validator = InteractionValidator(mock_client, cnj_errors)
        result = validator._validate_single_deal(deal)

        assert result.deal_id == 123
        assert result.cnj == "1234567-89.0123.4.56.7890"
        assert result.interaction_created is True
        assert result.last_interaction_updated is True
        assert result.error_message is None

    def test_validate_single_deal_no_cnj_error(self):
        """Testa quando deal não tem erro associado."""
        mock_client = Mock()

        deal = {
            "Id": 123,
            "Title": "Deal sem CNJ",
            "OtherProperties": [],
        }

        validator = InteractionValidator(mock_client, {})
        result = validator._validate_single_deal(deal)

        assert result.deal_id == 123
        assert result.cnj is None
        assert result.interaction_created is False
        assert result.error_message is None

    def test_generate_report_excel(self):
        """Testa geração de relatório Excel."""
        mock_client = Mock()

        validator = InteractionValidator(mock_client, {})

        report = InteractionValidationReport(
            total_deals=2,
            deals_with_interaction=1,
            deals_without_interaction=1,
            interactions_created=1,
            last_interaction_updated=1,
            errors=0,
        )

        report.results = [
            InteractionValidationResult(
                deal_id=123,
                cnj="1234567-89.0123.4.56.7890",
                had_interaction=False,
                interaction_created=True,
                last_interaction_updated=True,
            ),
            InteractionValidationResult(
                deal_id=456,
                cnj="9876543-21.9876.5.43.2109",
                had_interaction=True,
                interaction_created=False,
                last_interaction_updated=False,
            ),
        ]

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            validator.generate_report_excel(report, tmp.name)

            # Verificar que arquivo foi criado
            assert Path(tmp.name).exists()

            # Limpar
            Path(tmp.name).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
