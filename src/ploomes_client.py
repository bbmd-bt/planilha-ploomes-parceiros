"""
Cliente HTTP para integração com a API Ploomes.

Este módulo fornece uma interface para interagir com a API do Ploomes,
incluindo busca de negócios por CNJ, alteração de estágios e deleção.
"""

import logging
from typing import Dict, List, Optional

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

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api2.ploomes.com",
        timeout: int = 30,
    ):
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        # Configura headers padrão
        self.session.headers.update(
            {
                "User-Key": api_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

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
        kwargs.setdefault("timeout", self.timeout)

        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            # Log detalhado para debug, mas sem expor informações sensíveis
            self.logger.error(f"Erro na requisição {method} para endpoint: {endpoint}")
            # Não logar a URL completa ou headers que podem conter tokens
            raise PloomesAPIError("Falha na requisição para a API Ploomes")

    def search_deals_by_cnj(self, cnj: str) -> List[Dict]:
        """
        Busca negócios por CNJ.

        Args:
            cnj: Número CNJ para busca

        Returns:
            Lista de dicionários representando os negócios encontrados
        """
        # Validação básica do CNJ para prevenir injeções
        if not cnj or not isinstance(cnj, str):
            self.logger.warning("CNJ inválido fornecido")
            return []

        # Sanitiza o CNJ removendo caracteres não numéricos
        cnj_clean = "".join(filter(str.isdigit, cnj))
        if len(cnj_clean) != 20:  # CNJ brasileiro tem 20 dígitos
            self.logger.warning(f"CNJ com formato inválido: {cnj}")
            return []

        # Endpoint para buscar negócios com filtro por CNJ
        field_key = "deal_20E8290A-809B-4CF1-9345-6B264AED7830"
        filter_str = f"OtherProperties/any(op: op/FieldKey eq '{field_key}' and op/StringValue eq '{cnj_clean}')"
        endpoint = f"Deals?$filter={filter_str}"

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
        # Validação do deal_id
        if not isinstance(deal_id, int) or deal_id <= 0:
            self.logger.warning(f"ID de negócio inválido: {deal_id}")
            return None
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
        # Validação dos IDs
        if not isinstance(deal_id, int) or deal_id <= 0:
            self.logger.warning(f"ID de negócio inválido: {deal_id}")
            return False
        if not isinstance(stage_id, int) or stage_id <= 0:
            self.logger.warning(f"ID de estágio inválido: {stage_id}")
            return False

        payload = {"StageId": stage_id}

        try:
            response = self._make_request("PATCH", f"Deals({deal_id})", json=payload)
            updated_deal = response.json()

            # Trata resposta similar ao get_deal_by_id
            if isinstance(updated_deal, dict) and "value" in updated_deal:
                items = updated_deal.get("value") or []
                if items:
                    updated_deal = items[0]
                else:
                    updated_deal = None

            if updated_deal and str(updated_deal.get("StageId")) == str(stage_id):
                self.logger.info(
                    f"Negócio {deal_id} movido com sucesso para estágio {stage_id}"
                )
                return True
            else:
                self.logger.warning(
                    f"PATCH não atualizou o estágio corretamente para negócio {deal_id}"
                )
                return False

        except PloomesAPIError as e:
            self.logger.error(
                f"Erro ao mover negócio {deal_id} para estágio {stage_id}: {e}"
            )
            return False

    def delete_deal(self, deal_id: int) -> bool:
        """
        Deleta um negócio.

        Args:
            deal_id: ID do negócio a ser deletado

        Returns:
            True se a deleção foi bem-sucedida
        """
        # Validação do deal_id
        if not isinstance(deal_id, int) or deal_id <= 0:
            self.logger.warning(f"ID de negócio inválido: {deal_id}")
            return False
        try:
            self._make_request("DELETE", f"Deals({deal_id})")
            self.logger.info(f"Negócio {deal_id} deletado com sucesso")
            return True
        except PloomesAPIError as e:
            self.logger.error(f"Erro ao deletar negócio {deal_id}: {e}")
            return False

    def search_deals_by_stage(
        self, stage_id: int, created_before_datetime: Optional[str] = None
    ) -> List[Dict]:
        """
        Busca todos os negócios em um estágio específico.

        Args:
            stage_id: ID do estágio
            created_before_datetime: Data/hora máxima de criação (formato YYYY-MM-DDTHH:MM:SS), opcional

        Returns:
            Lista de dicionários representando os negócios encontrados
        """
        # Validação do stage_id
        if not isinstance(stage_id, int) or stage_id <= 0:
            self.logger.warning(f"ID de estágio inválido: {stage_id}")
            return []
        endpoint = f"Deals?$filter=StageId eq {stage_id}"

        try:
            response = self._make_request("GET", endpoint)
            data = response.json()
            deals = data.get("value", [])

            # Filtrar localmente por data/hora se especificado
            if created_before_datetime and deals:
                from datetime import datetime

                cutoff_datetime = datetime.strptime(
                    created_before_datetime, "%Y-%m-%dT%H:%M:%S"
                )
                filtered_deals = []

                for deal in deals:
                    created_date_str = deal.get("CreateDate") or deal.get("CreatedDate")
                    if created_date_str:
                        try:
                            # Parse ISO format date
                            created_date = datetime.fromisoformat(
                                created_date_str.replace("Z", "+00:00")
                            )
                            # Remove timezone info for comparison
                            created_date = created_date.replace(tzinfo=None)

                            if created_date < cutoff_datetime:
                                filtered_deals.append(deal)
                        except (ValueError, AttributeError):
                            # Se falhar ao parsear a data, incluir o deal
                            filtered_deals.append(deal)
                    else:
                        # Se não tiver data de criação, incluir o deal
                        filtered_deals.append(deal)

                deals = filtered_deals

            self.logger.info(
                f"Encontrados {len(deals)} negócios no estágio {stage_id}"
                + (
                    f" criados antes de {created_before_datetime}"
                    if created_before_datetime
                    else ""
                )
            )
            return deals
        except PloomesAPIError:
            return []
