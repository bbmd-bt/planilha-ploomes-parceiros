"""
Cliente HTTP para integração com a API Ploomes.

Este módulo fornece uma interface para interagir com a API do Ploomes,
incluindo busca de negócios por CNJ, alteração de estágios e deleção.
"""

import logging
import time
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

        # Rate limiting: 120 requisições por minuto = 1 req a cada 0.5 segundos
        self.min_request_interval = 0.5
        self.last_request_time = 0

    def _apply_rate_limit(self):
        """Aplica rate limiting para respeitar o limite de 120 requisições por minuto."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Faz uma requisição HTTP para a API com retry automático e rate limiting.

        Args:
            method: Método HTTP (GET, POST, PATCH, DELETE)
            endpoint: Endpoint da API (sem barra inicial)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Response object

        Raises:
            PloomesAPIError: Se a requisição falhar após retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        kwargs.setdefault("timeout", self.timeout)

        max_retries = 3
        backoff_factor = 1.0

        for attempt in range(max_retries):
            try:
                # Aplicar rate limiting antes de cada requisição
                self._apply_rate_limit()

                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                status_code = getattr(e.response, "status_code", None)

                # Se for erro 429 (rate limit), retry com backoff
                if status_code == 429 and attempt < max_retries - 1:
                    wait_time = backoff_factor * (2**attempt)
                    self.logger.warning(
                        f"Rate limit (429) atingido. Aguardando {wait_time:.1f}s "
                        f"antes de retry {attempt + 1}/{max_retries}..."
                    )
                    time.sleep(wait_time)
                    # Aumentar o intervalo mínimo entre requisições
                    self.min_request_interval = min(
                        self.min_request_interval * 1.5, 2.0
                    )
                    continue

                # Para outros erros HTTP, falhar imediatamente
                self.logger.error(
                    f"Erro na requisição {method} para endpoint: {endpoint}"
                )
                self.logger.error(f"Status code: {status_code}")
                if hasattr(e.response, "text") and e.response is not None:
                    self.logger.error(f"Response text: {e.response.text[:500]}")
                raise PloomesAPIError(
                    f"Falha na requisição para a API Ploomes (HTTP {status_code})"
                )

            except requests.exceptions.RequestException as e:
                # Para outros tipos de erro, também falhar
                self.logger.error(
                    f"Erro na requisição {method} para endpoint: {endpoint}"
                )
                self.logger.error(f"Erro: {str(e)}")
                raise PloomesAPIError("Falha na requisição para a API Ploomes")

        raise PloomesAPIError("Falha na requisição após múltiplas tentativas")

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

        # Normaliza o CNJ para o formato padrão NNNNNNN-DD.AAAA.J.TR.OOOO
        from src.normalizers import normalize_cnj

        cnj_normalized = normalize_cnj(cnj)
        if not cnj_normalized:
            self.logger.warning(f"CNJ com formato inválido: {cnj}")
            return []

        # Endpoint para buscar negócios com filtro por CNJ
        field_key = "deal_20E8290A-809B-4CF1-9345-6B264AED7830"
        filter_str = f"OtherProperties/any(op: op/FieldKey eq '{field_key}' and op/StringValue eq '{cnj_normalized}')"
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
            response = self._make_request(
                "GET", f"Deals?$filter=Id eq {deal_id}&$expand=OtherProperties"
            )
            data = response.json()

            # A API pode retornar um objeto direto ou dentro de "value"
            if isinstance(data, dict) and "value" in data:
                items = data.get("value") or []
                if items:
                    deal = items[0]
                    if deal.get("Id") == deal_id:
                        return deal
                    else:
                        self.logger.warning(
                            f"API returned wrong deal: requested {deal_id}, got {deal.get('Id')}"
                        )
                        return None
                return None

            # Retorno direto
            if isinstance(data, dict) and data.get("Id") == deal_id:
                return data
            else:
                got_id = data.get("Id") if isinstance(data, dict) else "non-dict"
                self.logger.warning(
                    f"API returned wrong deal: requested {deal_id}, got {got_id}"
                )
                return None
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
            self._make_request("PATCH", f"Deals({deal_id})", json=payload)
            self.logger.info(
                f"Negócio {deal_id} movido com sucesso para estágio {stage_id}"
            )
            return True

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
        endpoint = (
            "Deals?$select=Id,StageId,Title,CreateDate,OriginDealId,PipelineId,OtherProperties"
            f"&$expand=OtherProperties&$filter=StageId eq {stage_id}"
        )

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

    def get_pipeline(self, pipeline_id: int) -> Optional[Dict]:
        """
        Busca um pipeline específico por ID, incluindo estágios.

        Args:
            pipeline_id: ID do pipeline

        Returns:
            Dicionário com dados do pipeline ou None se não encontrado
        """
        if not isinstance(pipeline_id, int) or pipeline_id <= 0:
            self.logger.warning(f"ID de pipeline inválido: {pipeline_id}")
            return None
        try:
            response = self._make_request(
                "GET", f"Deals@Pipelines?$expand=Stages&$filter=Id+eq+{pipeline_id}"
            )
            data = response.json()
            # A API retorna um objeto com "value" contendo a lista
            if isinstance(data, dict) and "value" in data:
                items = data.get("value") or []
                if items:
                    return items[0]
                return None
            return data
        except PloomesAPIError:
            return None

    def create_pipeline(self, pipeline_data: Dict) -> Optional[Dict]:
        """
        Cria um novo pipeline.

        Args:
            pipeline_data: Dados do pipeline a ser criado

        Returns:
            Dicionário com dados do pipeline criado ou None se falhar
        """
        try:
            response = self._make_request("POST", "Deals@Pipelines", json=pipeline_data)
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

    def get_deals_by_pipeline(self, pipeline_id: int) -> List[Dict]:
        """
        Busca todos os negócios em um pipeline específico.

        Args:
            pipeline_id: ID do pipeline

        Returns:
            Lista de dicionários com dados dos negócios
        """
        if not isinstance(pipeline_id, int) or pipeline_id <= 0:
            self.logger.warning(f"ID de pipeline inválido: {pipeline_id}")
            return []
        deals = []
        skip = 0
        top = 100  # Limite por página
        while True:
            try:
                response = self._make_request(
                    "GET",
                    f"Deals?$filter=PipelineId eq {pipeline_id}&$expand=OtherProperties&$top={top}&$skip={skip}",
                )
                data = response.json()
                page_deals = data.get("value", [])
                deals.extend(page_deals)
                if len(page_deals) < top:
                    break
                skip += top
            except PloomesAPIError:
                break
        self.logger.info(f"Encontrados {len(deals)} negócios no pipeline {pipeline_id}")
        return deals

    def create_deal(self, deal_data: Dict) -> Optional[Dict]:
        """
        Cria um novo negócio.

        Args:
            deal_data: Dados do negócio a ser criado

        Returns:
            Dicionário com dados do negócio criado ou None se falhar
        """
        try:
            response = self._make_request("POST", "Deals", json=deal_data)
            data = response.json()
            return data
        except PloomesAPIError:
            return None
