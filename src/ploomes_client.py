"""
Cliente HTTP para integração com a API Ploomes.

Este módulo fornece uma interface para interagir com a API do Ploomes,
incluindo busca de negócios por CNJ, alteração de estágios e deleção.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

import requests


class PloomesAPIError(Exception):
    """Exceção para erros da API Ploomes."""
    pass


class PloomesClient:
    """
    Cliente para integração com a API Ploomes.

    Args:
        api_token: Token de autenticação da API
        base_url: URL base da API (padrão: https://api2.ploomes.com)
        timeout: Timeout para requisições HTTP (padrão: 30 segundos)
    """

    def __init__(self, api_token: str, base_url: str = "https://api2.ploomes.com", timeout: int = 30):
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        # Configura headers padrão
        self.session.headers.update({
            "User-Key": api_token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

        self.logger = logging.getLogger(__name__)

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Faz uma requisição HTTP para a API.

        Args:
            method: Método HTTP (GET, POST, PATCH, DELETE)
            endpoint: Endpoint da API (sem barra inicial)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Response object

        Raises:
            PloomesAPIError: Se a requisição falhar
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        kwargs.setdefault('timeout', self.timeout)

        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro na requisição {method} {url}: {e}")
            raise PloomesAPIError(f"Falha na requisição: {e}")

    def search_deals_by_cnj(self, cnj: str) -> List[Dict]:
        """
        Busca negócios por CNJ.

        Args:
            cnj: Número CNJ para busca

        Returns:
            Lista de dicionários representando os negócios encontrados
        """

        # Endpoint para buscar negócios com filtro por CNJ
        endpoint = f"Deals?$filter=OtherProperties/any(op: op/FieldKey eq 'deal_20E8290A-809B-4CF1-9345-6B264AED7830' and op/StringValue eq '{cnj}')"

        try:
            response = self._make_request("GET", endpoint)
            data = response.json()
            return data.get("value", [])
        except PloomesAPIError:
            return []

    def get_deal_by_id(self, deal_id: int) -> Optional[Dict]:
        """
        Busca um negócio específico por ID.

        Args:
            deal_id: ID do negócio

        Returns:
            Dicionário com dados do negócio ou None se não encontrado
        """
        try:
            response = self._make_request("GET", f"Deals({deal_id})")
            data = response.json()

            # A API pode retornar um objeto direto ou dentro de "value"
            if isinstance(data, dict) and "value" in data:
                items = data.get("value") or []
                if items:
                    return items[0]
                return None

            return data
        except PloomesAPIError:
            return None

    def update_deal_stage(self, deal_id: int, stage_id: int) -> bool:
        """
        Atualiza o estágio de um negócio.

        Args:
            deal_id: ID do negócio
            stage_id: ID do novo estágio

        Returns:
            True se a atualização foi bem-sucedida
        """
        payload = {
            "StageId": stage_id
        }

        try:
            self._make_request("PATCH", f"Deals({deal_id})", json=payload)
            self.logger.info(f"Negócio {deal_id} movido para estágio {stage_id}")

            # Verifica se a mudança foi realmente aplicada
            deal = self.get_deal_by_id(deal_id)
            if deal and deal.get("StageId") == stage_id:
                return True
            else:
                self.logger.warning(f"Falha na verificação: negócio {deal_id} não está no estágio esperado {stage_id}")
                return False

        except PloomesAPIError as e:
            self.logger.error(f"Erro ao mover negócio {deal_id} para estágio {stage_id}: {e}")
            return False

    def delete_deal(self, deal_id: int) -> bool:
        """
        Deleta um negócio.

        Args:
            deal_id: ID do negócio a ser deletado

        Returns:
            True se a deleção foi bem-sucedida
        """
        try:
            self._make_request("DELETE", f"Deals({deal_id})")
            self.logger.info(f"Negócio {deal_id} deletado com sucesso")
            return True
        except PloomesAPIError as e:
            self.logger.error(f"Erro ao deletar negócio {deal_id}: {e}")
            return False

    def get_pipeline_stages(self, pipeline_id: int) -> List[Dict]:
        """
        Busca os estágios de um pipeline específico.

        Args:
            pipeline_id: ID do pipeline

        Returns:
            Lista de estágios do pipeline
        """
        try:
            response = self._make_request("GET", f"Stages?$filter=PipelineId eq {pipeline_id}")
            data = response.json()
            return data.get("value", [])
        except PloomesAPIError:
            return []
