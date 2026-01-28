import os
import json
from pathlib import Path
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from dotenv import load_dotenv
from src.clients.parceiros_client import ParceirosClient, ParceirosAPIError
from tqdm import tqdm


class DatabaseUpdateError(Exception):
    pass


def sanitize_string(value):
    """Sanitize string to prevent injection or malicious inputs."""
    if not isinstance(value, str):
        return ""
    # Allow only alphanumeric, spaces, hyphens, and underscores
    return re.sub(r"[^a-zA-Z0-9\s\-_]", "", value).strip()


# Mapeamento de mesa para pipeline para credenciais Parceiros
MESA_TO_PIPELINE = {
    "btblue": "BT Blue Pipeline",
    "2bativos": "2B Ativos Pipeline",
    "bbmd": "BBMD Pipeline",
}


class ApiUpdater:
    def __init__(self, mesa: str):
        load_dotenv()
        self.mesa = mesa.lower()
        if self.mesa not in MESA_TO_PIPELINE:
            raise ValueError(
                f"Mesa '{mesa}' não suportada. Mesas disponíveis: {', '.join(MESA_TO_PIPELINE.keys())}"
            )

        self.base_dir = Path(__file__).parent.parent.parent  # Vai até a raiz do projeto

    def update_json_files(self, offices, negotiators):
        logger.debug(
            f"Iniciando atualização dos arquivos JSON para mesa '{self.mesa}'."
        )

        # Criar nomes de arquivo com sufixo da mesa
        offices_filename = f"escritorios_{self.mesa}.json"
        negotiators_filename = f"negociadores_{self.mesa}.json"

        # Update escritorios file
        offices_file = self.base_dir / "utils" / offices_filename
        try:
            # Convert offices set to dict for JSON serialization
            offices_dict = {office: office for office in offices}
            data = {"escritorios": offices_dict, "total": len(offices)}
            with open(offices_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(
                f"Arquivo {offices_file} atualizado com {len(offices)} escritórios."
            )
        except Exception as e:
            logger.error(f"Erro ao salvar escritórios: {e}")
            raise DatabaseUpdateError(f"Erro ao salvar escritórios: {e}")

        # Update negociadores file
        neg_file = self.base_dir / "utils" / negotiators_filename
        try:
            # Convert negotiators set to dict for JSON serialization
            negotiators_dict = {neg: neg for neg in negotiators}
            data = {"negociadores": negotiators_dict, "total": len(negotiators)}
            with open(neg_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(
                f"Arquivo {neg_file} atualizado com {len(negotiators)} negociadores."
            )
        except Exception as e:
            logger.error(f"Erro ao salvar negociadores: {e}")
            raise DatabaseUpdateError(f"Erro ao salvar negociadores: {e}")

    def update_database(self):
        start_time = time.time()
        logger.info(f"Iniciando atualização dos mapeamentos para mesa '{self.mesa}'.")

        # Fazer uma única busca na API para obter leads
        all_leads = self.get_all_leads_from_api()

        # Processar leads para extrair escritórios e negociadores
        all_offices, all_negotiators = self.process_leads_for_mappings(all_leads)

        # Salvar dados nos arquivos JSON por mesa
        self.update_json_files(all_offices, all_negotiators)

        elapsed = time.time() - start_time
        logger.info(
            f"Atualização concluída com sucesso em {elapsed:.2f} segundos. "
            f"Escritórios únicos: {len(all_offices)}, Negociadores únicos: {len(all_negotiators)}."
        )

    def get_all_leads_from_api(self) -> list:
        """Obtém todos os leads da API Parceiros em uma única busca usando paralelização.

        A API Parceiros retorna leads em páginas de 10 itens cada.
        Este método usa paralelização para melhorar a performance.
        """
        logger.info(
            f"Iniciando busca paralela de leads na API Parceiros para mesa '{self.mesa}'."
        )

        # Obter credenciais
        pipeline = MESA_TO_PIPELINE[self.mesa]
        credential_map = {
            "BT Blue Pipeline": (
                "PARCEIROS_BT_BLUE_USERNAME",
                "PARCEIROS_BT_BLUE_PASSWORD",
            ),
            "2B Ativos Pipeline": (
                "PARCEIROS_2B_ATIVOS_USERNAME",
                "PARCEIROS_2B_ATIVOS_PASSWORD",
            ),
            "BBMD Pipeline": ("PARCEIROS_BBMD_USERNAME", "PARCEIROS_BBMD_PASSWORD"),
        }

        username_var, password_var = credential_map[pipeline]
        username = os.getenv(username_var)
        password = os.getenv(password_var)

        if not username or not password:
            raise DatabaseUpdateError(
                f"Credenciais não encontradas para mesa '{self.mesa}'. "
                f"Verifique as variáveis de ambiente {username_var} e {password_var}."
            )

        try:
            client = ParceirosClient(username, password)
            client.authenticate()

            # Obter informações de paginação
            logger.info("Obtendo informações de paginação...")
            leads_url = f"{client.base_url}/pendencia"
            headers = {
                "Authorization": f"Bearer {client.token}",
                "Content-Type": "application/json",
            }
            params = {"numero_pagina": 1, "tamanho_pagina": client.PAGE_SIZE}

            response = client.session.get(
                leads_url, headers=headers, params=params, timeout=client.timeout
            )
            if response.status_code != 200:
                raise DatabaseUpdateError(
                    f"Erro ao obter informações de paginação: {response.status_code}"
                )

            pagination_data = response.json()
            if "informacao" not in pagination_data:
                raise DatabaseUpdateError(
                    "Informações de paginação não encontradas na resposta da API"
                )

            info = pagination_data["informacao"]
            total_pages = info.get("total_paginas")
            total_items = info.get("quantidade_itens")

            if not total_pages:
                raise DatabaseUpdateError(
                    "Número total de páginas não informado pela API"
                )

            logger.info(
                f"API retornou {total_pages} páginas totais com {total_items} itens"
            )

            # Estratégia de paralelização
            max_workers = min(10, total_pages)  # Máximo 10 threads simultâneas
            batch_size = 50  # Processar em lotes de 50 páginas

            all_leads = []
            processed_pages = 1  # Página 1 já foi processada
            start_time = time.time()

            logger.info(
                f"Iniciando captura paralela com {max_workers} threads simultâneas..."
            )
            logger.info(f"Total estimado: {total_pages} páginas ({total_items} leads)")

            with tqdm(
                desc="Capturando leads",
                unit="página",
                initial=1,
                total=total_pages,
                leave=False,
                smoothing=0.1,
            ) as pbar:

                # Processar em lotes para não sobrecarregar a memória
                for batch_start in range(2, total_pages + 1, batch_size):
                    batch_end = min(batch_start + batch_size, total_pages + 1)
                    pages_to_fetch = list(range(batch_start, batch_end))

                    logger.debug(
                        f"Processando lote: páginas {batch_start} a {batch_end-1}"
                    )

                    # Usar ThreadPoolExecutor para paralelização
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submeter todas as requisições do lote
                        future_to_page = {
                            executor.submit(client.get_leads_page, page): page
                            for page in pages_to_fetch
                        }

                        # Processar resultados à medida que chegam
                        completed_in_batch = 0
                        for future in as_completed(future_to_page):
                            page = future_to_page[future]
                            try:
                                leads_batch = future.result()
                                if leads_batch:
                                    all_leads.extend(leads_batch)
                                else:
                                    logger.warning(f"Página {page} retornou vazia")

                                completed_in_batch += 1
                                processed_pages += 1

                                # Atualizar progresso apenas quando completar um lote ou periodicamente
                                if completed_in_batch % max(
                                    1, len(pages_to_fetch) // 10
                                ) == 0 or completed_in_batch == len(pages_to_fetch):
                                    pbar.n = processed_pages
                                    pbar.set_description(
                                        f"Capturando leads (página {processed_pages}/{total_pages})"
                                    )
                                    pbar.refresh()

                            except Exception as e:
                                logger.error(f"Erro ao processar página {page}: {e}")
                                completed_in_batch += 1
                                processed_pages += 1

                                if completed_in_batch % max(
                                    1, len(pages_to_fetch) // 10
                                ) == 0 or completed_in_batch == len(pages_to_fetch):
                                    pbar.n = processed_pages
                                    pbar.set_description(
                                        f"Capturando leads (página {processed_pages}/{total_pages})"
                                    )
                                    pbar.refresh()

            # Calcular estatísticas de tempo
            end_time = time.time()
            total_time = end_time - start_time
            avg_time_per_page = (
                total_time / (total_pages - 1) if total_pages > 1 else total_time
            )

            logger.info(f"Total de leads capturados: {len(all_leads)}")
            logger.info(
                f"Tempo total: {total_time:.2f}s | Média por página: {avg_time_per_page:.2f}s"
            )
            logger.info(f"Performance: {len(all_leads)/total_time:.1f} leads/segundo")

            return all_leads

        except ParceirosAPIError as e:
            logger.error(f"Erro na API Parceiros: {e}")
            raise DatabaseUpdateError(f"Erro ao obter leads da API Parceiros: {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao obter leads: {e}")
            raise DatabaseUpdateError(f"Erro inesperado ao obter leads: {e}")

    def process_leads_for_mappings(self, all_leads: list) -> tuple[set, set]:
        """Processa os leads para extrair escritórios e negociadores.

        Args:
            all_leads: Lista de leads obtidos da API

        Returns:
            Tupla com (escritorios_set, negociadores_set)
        """
        logger.info("Processando leads para extrair mapeamentos...")

        offices = set()
        negotiators = set()

        # Barra de progresso combinada para processamento
        with tqdm(
            total=len(all_leads),
            desc="Processando mapeamentos",
            unit="lead",
            leave=False,
        ) as pbar:
            for lead in all_leads:
                # Extrair escritório
                office = lead.get("escritorio_responsavel")
                if office and isinstance(office, str) and office.strip():
                    offices.add(office.strip())

                # Extrair negociador
                negotiator = lead.get("negociador")
                if negotiator and isinstance(negotiator, str) and negotiator.strip():
                    negotiators.add(sanitize_string(negotiator.strip()))

                pbar.update(1)

        logger.info(
            f"Processamento concluído: {len(offices)} escritórios e {len(negotiators)} negociadores únicos."
        )
        return offices, negotiators
