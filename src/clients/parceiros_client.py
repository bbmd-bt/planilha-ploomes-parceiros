"""
Cliente HTTP para integração com a API Parceiros.

Este módulo fornece uma interface para interagir com a API do Parceiros,
incluindo autenticação via token e busca de leads/pendências.
"""

import requests
from typing import Any, Dict, List, Optional
from loguru import logger


class ParceirosAPIError(Exception):
    """Exceção para erros da API Parceiros."""

    pass


class ParceirosClient:
    """
    Cliente para integração com a API Parceiros.

    Responsável por autenticação e busca de leads na plataforma Parceiros.

    Args:
        username: Usuário para autenticação (email)
        password: Senha para autenticação
        base_url: URL base da API (padrão: https://uar8quj870.execute-api.us-east-1.amazonaws.com/prod)
        timeout: Timeout para requisições HTTP (padrão: 30 segundos)
    """

    PAGE_SIZE = 10  # Tamanho fixo das páginas da API

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str = "https://uar8quj870.execute-api.us-east-1.amazonaws.com/prod",
        timeout: int = 30,
    ):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.token: Optional[str] = None

    def authenticate(self) -> None:
        """
        Realiza autenticação na API Parceiros e obtém o token.

        Raises:
            ParceirosAPIError: Se houver erro na autenticação ou requisição
        """
        try:
            login_url = f"{self.base_url}/login"
            payload = {"usuario": self.username, "senha": self.password}

            response = self.session.post(login_url, json=payload, timeout=self.timeout)

            if response.status_code != 200:
                error_msg = f"Erro na autenticação Parceiros: {response.status_code}"
                if response.status_code == 502:
                    error_msg += (
                        " - Internal Server Error. Isso pode indicar que o servidor da API "
                        "Parceiros está temporariamente indisponível. Tente novamente em alguns "
                        "minutos. Se o problema persistir, verifique as credenciais ou contate o suporte."
                    )
                elif response.status_code == 401:
                    error_msg += (
                        " - Unauthorized. Verifique se as credenciais estão corretas."
                    )
                elif response.status_code == 403:
                    error_msg += " - Forbidden. Verifique se a conta tem permissões para acessar a API."
                else:
                    error_msg += f" - {response.text}"
                logger.error(f"{error_msg} (URL: {login_url})")
                raise ParceirosAPIError(error_msg)

            data = response.json()
            self.token = data.get("token")

            if not self.token:
                logger.error("Token não recebido na resposta de autenticação")
                raise ParceirosAPIError(
                    "Token não recebido na resposta de autenticação"
                )

            logger.info("Autenticação Parceiros bem-sucedida")

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao conectar na API Parceiros: {e}")
            raise ParceirosAPIError(f"Erro de conexão: {e}")

    def get_all_leads(self) -> List[Dict]:
        """
        Obtém todos os leads/pendências da API Parceiros.

        Realiza paginação automaticamente para obter todos os resultados.
        Para grandes volumes, pode levar tempo. Recomenda-se usar com cuidado.

        Returns:
            Lista com todos os leads encontrados

        Raises:
            ParceirosAPIError: Se houver erro na requisição ou autenticação
        """
        if not self.token:
            raise ParceirosAPIError("Não autenticado. Execute authenticate() primeiro.")

        all_leads = []
        page = 1
        max_pages = 100  # Limitar a 100 páginas (1000 leads) para não sobrecarregar

        try:
            while page <= max_pages:
                leads_url = f"{self.base_url}/pendencia"
                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                }

                params = {"pagina": page, "tamanho_pagina": self.PAGE_SIZE}

                response = self.session.get(
                    leads_url, headers=headers, params=params, timeout=self.timeout
                )

                if response.status_code != 200:
                    logger.error(
                        f"Erro ao obter leads (página {page}): {response.status_code} - {response.text}"
                    )
                    break

                data = response.json()
                # A resposta pode ter 'body' wrapping ou não, tentar ambas as formas
                if "body" in data:
                    body = data.get("body", {})
                else:
                    body = data

                resultado = body.get("resultado", [])

                if not resultado:
                    break

                all_leads.extend(resultado)

                # Verificar se há mais páginas
                informacao = body.get("informacao", {})
                total_paginas = informacao.get("total_paginas", 1)

                if page >= total_paginas:
                    break

                page += 1

            logger.info(
                f"Total de leads obtidos: {len(all_leads)} (limite: {max_pages} páginas)"
            )
            return all_leads

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter leads: {e}")
            raise ParceirosAPIError(f"Erro de conexão: {e}")

    def get_leads_page(self, page: int) -> List[Dict]:
        """
        Obtém uma página específica de leads da API Parceiros.

        Args:
            page: Número da página (1-indexed)

        Returns:
            Lista com os leads da página

        Raises:
            ParceirosAPIError: Se houver erro na requisição ou autenticação
        """
        if not self.token:
            raise ParceirosAPIError("Não autenticado. Execute authenticate() primeiro.")

        try:
            leads_url = f"{self.base_url}/pendencia"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }

            params = {"numero_pagina": page, "tamanho_pagina": self.PAGE_SIZE}

            response = self.session.get(
                leads_url, headers=headers, params=params, timeout=self.timeout
            )

            if response.status_code != 200:
                error_msg = (
                    f"Erro ao obter leads (página {page}): {response.status_code}"
                )
                if response.status_code == 401:
                    error_msg += " - Unauthorized. O token pode ter expirado. Tente autenticar novamente."
                elif response.status_code == 403:
                    error_msg += " - Forbidden. Verifique se a conta tem permissões para acessar os dados."
                elif response.status_code == 404:
                    error_msg += " - Not Found. O endpoint pode ter mudado."
                elif response.status_code == 429:
                    error_msg += " - Too Many Requests. Aguarde alguns minutos antes de tentar novamente."
                elif response.status_code == 500:
                    error_msg += (
                        " - Internal Server Error. Problema no servidor da API."
                    )
                elif response.status_code == 502:
                    error_msg += " - Bad Gateway. O servidor da API pode estar temporariamente indisponível."
                elif response.status_code == 503:
                    error_msg += " - Service Unavailable. O serviço da API pode estar em manutenção."
                else:
                    error_msg += f" - {response.text}"
                logger.error(f"{error_msg} (URL: {leads_url})")
                return []

            data = response.json()
            # A resposta pode ter 'body' wrapping ou não, tentar ambas as formas
            if "body" in data:
                body = data.get("body", {})
            else:
                body = data

            resultado = body.get("resultado", [])
            return resultado

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter página {page}: {e}")
            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter leads: {e}")
            raise ParceirosAPIError(f"Erro ao obter leads: {e}")

    def get_leads_by_cnj(self, cnj: str) -> List[Dict]:
        """
        Obtém leads filtrados por CNJ diretamente da API.

        Args:
            cnj: CNJ a ser procurado

        Returns:
            Lista com leads encontrados (pode estar vazia se não existir)

        Raises:
            ParceirosAPIError: Se houver erro na requisição ou autenticação
        """
        if not self.token:
            raise ParceirosAPIError("Não autenticado. Execute authenticate() primeiro.")

        try:
            leads_url = f"{self.base_url}/pendencia"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }

            params: Dict[str, Any] = {"cnj": cnj, "tamanho_pagina": self.PAGE_SIZE}  # type: ignore[assignment]

            response = self.session.get(  # type: ignore
                leads_url, headers=headers, params=params, timeout=self.timeout  # type: ignore[arg-type]
            )  # type: ignore[arg-type]

            if response.status_code != 200:
                logger.error(
                    f"Erro ao obter leads por CNJ ({cnj}): {response.status_code} - {response.text}"
                )
                return []

            data = response.json()
            # A resposta pode ter 'body' wrapping ou não, tentar ambas as formas
            if "body" in data:
                body = data.get("body", {})
            else:
                body = data

            resultado = body.get("resultado", [])

            logger.info(f"Leads encontrados para CNJ {cnj}: {len(resultado)}")
            return resultado

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao obter leads por CNJ: {e}")
            raise ParceirosAPIError(f"Erro ao obter leads por CNJ: {e}")

    def lead_exists_by_cnj(self, cnj: str, leads: Optional[List[Dict]] = None) -> bool:
        """
        Verifica se um lead existe na Parceiros pelo CNJ.

        Args:
            cnj: CNJ a ser procurado
            leads: Lista de leads (ignorada, mantida para compatibilidade)

        Returns:
            True se o lead existe, False caso contrário
        """
        try:
            leads = self.get_leads_by_cnj(cnj)
            return len(leads) > 0

        except Exception as e:
            logger.error(f"Erro ao verificar se lead existe: {e}")
            return False

    def get_lead_by_cnj(
        self, cnj: str, leads: Optional[List[Dict]] = None
    ) -> Optional[Dict]:
        """
        Obtém um lead específico pelo CNJ.

        Args:
            cnj: CNJ a ser procurado
            leads: Lista de leads (ignorada, mantida para compatibilidade)

        Returns:
            Dicionário com dados do lead ou None se não encontrado
        """
        try:
            leads = self.get_leads_by_cnj(cnj)
            return leads[0] if leads else None

        except Exception as e:
            logger.error(f"Erro ao obter lead por CNJ: {e}")
            return None
