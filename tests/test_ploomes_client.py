"""
Testes para o cliente da API Ploomes.
"""

import pytest
from unittest.mock import Mock, patch
from src.ploomes_client import PloomesClient, PloomesAPIError


class TestPloomesClient:
    """Testes para PloomesClient."""

    @pytest.fixture
    def client(self):
        """Fixture que retorna uma instância do cliente."""
        return PloomesClient("fake_token")

    def test_init(self, client):
        """Testa inicialização do cliente."""
        assert client.api_token == "fake_token"
        assert client.base_url == "https://api2.ploomes.com"
        assert client.timeout == 30

    @patch("src.ploomes_client.requests.Session.request")
    def test_search_deals_by_cnj_success(self, mock_request, client):
        """Testa busca de negócios por CNJ com sucesso."""
        # Mock da resposta
        mock_response = Mock()
        mock_response.json.return_value = {"value": [{"Id": 123, "StageId": 456}]}
        mock_request.return_value = mock_response

        # Executa busca
        result = client.search_deals_by_cnj("12345678901234567890")

        # Verifica resultado
        assert len(result) == 1
        assert result[0]["Id"] == 123

        # Verifica chamada da API
        mock_request.assert_called_once()
        args = mock_request.call_args
        assert args[0][0] == "GET"
        assert "Deals?$filter" in args[0][1]

    @patch("src.ploomes_client.requests.Session.request")
    def test_search_deals_by_cnj_not_found(self, mock_request, client):
        """Testa busca de negócios por CNJ quando não encontrado."""
        # Mock da resposta vazia
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        mock_request.return_value = mock_response

        result = client.search_deals_by_cnj("99999999999999999999")

        assert result == []

    @patch("src.ploomes_client.requests.Session.request")
    def test_update_deal_stage_success(self, mock_request, client):
        """Testa atualização de estágio com sucesso."""
        # Mock da resposta do PATCH
        mock_response = Mock()

        mock_request.return_value = mock_response

        result = client.update_deal_stage(123, 999)

        assert result is True

        # Verifica chamada do PATCH
        assert mock_request.call_count == 1

    @patch("src.ploomes_client.requests.Session.request")
    def test_update_deal_stage_success_value_wrapper(self, mock_request, client):
        """Testa atualização de estágio quando a API retorna o negócio dentro de 'value'."""
        mock_response = Mock()

        mock_request.return_value = mock_response

        result = client.update_deal_stage(123, 999)

        assert result is True
        assert mock_request.call_count == 1

    @patch("src.ploomes_client.requests.Session.request")
    def test_update_deal_stage_failure(self, mock_request, client):
        """Testa falha na atualização de estágio."""
        # Mock de erro na requisição
        mock_request.side_effect = PloomesAPIError("API Error")

        result = client.update_deal_stage(123, 999)

        assert result is False

    @patch("src.ploomes_client.requests.Session.request")
    def test_delete_deal_success(self, mock_request, client):
        """Testa deleção de negócio com sucesso."""
        mock_response = Mock()
        mock_request.return_value = mock_response

        result = client.delete_deal(123)

        assert result is True

        # Verifica chamada DELETE
        args = mock_request.call_args
        assert args[0][0] == "DELETE"
        assert "Deals(123)" in args[0][1]

    @patch("src.ploomes_client.requests.Session.request")
    def test_delete_deal_failure(self, mock_request, client):
        """Testa falha na deleção de negócio."""
        mock_request.side_effect = PloomesAPIError("API Error")

        result = client.delete_deal(123)

        assert result is False
