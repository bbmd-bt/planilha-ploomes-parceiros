"""
Módulo de sincronização para processamento de negócios Ploomes.

Este módulo contém a lógica para processar CNJs de um arquivo Excel,
mover negócios para estágios específicos e deletar aqueles que falharam.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import re

# Adicionar o diretório pai ao sys.path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from loguru import logger

from src.ploomes_client import PloomesClient


@dataclass
class ProcessingResult:
    """Resultado do processamento de um CNJ."""

    cnj: str
    deal_id: Optional[int] = None
    moved_successfully: bool = False
    deleted_successfully: bool = False
    error_message: Optional[str] = None


@dataclass
class SyncReport:
    """Relatório consolidado do processamento."""

    total_processed: int = 0
    successfully_moved: int = 0
    successfully_deleted: int = 0
    failed_movements: int = 0
    skipped_deletions: int = 0
    results: List[ProcessingResult] = field(default_factory=list)


class PloomesSync:
    """
    Classe responsável pela sincronização de negócios na Ploomes.

    Args:
        client: Instância do cliente Ploomes
        target_stage_id: ID do estágio para onde mover os negócios
        deletion_stage_id: ID do estágio onde a deleção deve ocorrer
    """

    def __init__(
        self,
        client: PloomesClient,
        target_stage_id: int,
        deletion_stage_id: int,
        origin_config: Optional[Dict[str, Dict[str, int]]] = None,
        dry_run: bool = False,
        max_workers: int = 1,
    ):
        self.client = client
        self.target_stage_id = target_stage_id
        self.deletion_stage_id = deletion_stage_id
        self.origin_config = origin_config or {}
        self.dry_run = dry_run
        self.max_workers = max_workers

    def process_cnj_list(self, cnj_list: List[str]) -> SyncReport:
        """
        Processa uma lista de CNJs seguindo as regras de negócio.

        Regras:
        1. Carregar CNJs do arquivo (esses negócios devem ser preservados/movidos para target_stage)
        2. Buscar os negócios correspondentes aos CNJs
        3. Mover os negócios encontrados para o target_stage

        Args:
            cnj_list: Lista de CNJs a mover para target_stage

        Returns:
            Relatório consolidado do processamento
        """
        report = SyncReport()
        report.total_processed = len(cnj_list)

        self.cnj_list = set(cnj_list)  # Store for later use in deletion

        # Verificar se o estágio de deleção está vazio
        deletion_stage_empty = False
        try:
            deletion_deals = self.client.search_deals_by_stage(self.deletion_stage_id)
            if not deletion_deals:
                deletion_stage_empty = True
                logger.info(f"Estágio de deleção {self.deletion_stage_id} está vazio.")
        except Exception as e:
            logger.error(f"Erro ao verificar estágio de deleção: {e}")
            deletion_stage_empty = True  # Treat as empty to skip

        # Processar CNJs apenas se o estágio de deleção não estiver vazio
        if not deletion_stage_empty:
            # Usar paralelização para processar CNJs
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submeter tarefas
                future_to_cnj = {
                    executor.submit(self._process_single_cnj, cnj): cnj
                    for cnj in cnj_list
                }

                # Coletar resultados
                for future in as_completed(future_to_cnj):
                    cnj = future_to_cnj[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Erro ao processar CNJ {cnj}: {e}")
                        results.append(ProcessingResult(cnj=cnj, error_message=str(e)))

            # Agregar resultados
            for result in results:
                if result.moved_successfully:
                    report.successfully_moved += 1
                else:
                    report.failed_movements += 1
                report.results.append(result)
        else:
            logger.info(
                "Pulando processamento de CNJs pois estágio de deleção está vazio."
            )
            results = []  # No results

        # Mover deals de origem para todos os negócios no target_stage

        # Mover deals de origem para todos os negócios no target_stage
        try:
            all_target_deals = self.client.search_deals_by_stage(self.target_stage_id)
            if all_target_deals:
                logger.info(
                    f"Movendo deals de origem para {len(all_target_deals)} negócios no target_stage"
                )
                for deal in all_target_deals:
                    self._move_origin_deal(deal)
        except Exception as e:
            logger.error(
                f"Erro ao mover deals de origem para negócios no target_stage: {e}"
            )

        # Agora, deletar negócios no estágio de deleção que não estão na lista de CNJs preservados
        if not deletion_stage_empty:
            deleted_count = 0
            skipped_deletions = 0

            try:
                all_deletion_deals = self.client.search_deals_by_stage(
                    self.deletion_stage_id
                )
                if all_deletion_deals:
                    logger.info(
                        f"Encontrados {len(all_deletion_deals)} negócios no estágio de deleção para verificar deleção"
                    )
                    for deal in all_deletion_deals:
                        deal_id = deal.get("Id")

                        if not deal_id:
                            skipped_deletions += 1
                            logger.warning("Negócio sem ID encontrado, pulando")
                            continue

                        cnj_label = self._extract_cnj_from_deal(deal) or "sem CNJ"

                        # Skip deletion if CNJ is in the preserved list
                        if cnj_label in self.cnj_list:
                            logger.info(
                                f"{cnj_label}: negócio {deal_id} preservado (CNJ na lista), pulando deleção"
                            )
                            continue

                        if self.dry_run:
                            logger.info(
                                f"[DRY-RUN] {cnj_label}: negócio {deal_id} seria deletado"
                            )
                            deleted_count += 1
                        elif self.client.delete_deal(deal_id):
                            deleted_count += 1
                            logger.info(f"{cnj_label}: negócio {deal_id} deletado")
                        else:
                            skipped_deletions += 1
                            logger.error(
                                f"{cnj_label}: falha ao deletar negócio {deal_id}"
                            )
            except Exception as e:
                logger.error(f"Erro ao deletar negócios no estágio de deleção: {e}")

            report.successfully_deleted = deleted_count
            report.skipped_deletions = skipped_deletions
        else:
            logger.info("Pulando deleção pois estágio de deleção está vazio.")
            report.successfully_deleted = 0
            report.skipped_deletions = 0

        logger.info(
            f"Processamento concluído: {report.successfully_moved} movidos, {report.failed_movements} falhas, "
            f"{deleted_count} deletados, {skipped_deletions} pulados"
        )
        return report

    def _process_single_cnj(self, cnj: str) -> ProcessingResult:
        """
        Processa um único CNJ.

        Args:
            cnj: CNJ a ser processado

        Returns:
            Resultado do processamento
        """
        result = ProcessingResult(cnj=cnj)
        logger.info(f"Processando CNJ: {cnj}")
        try:
            # Buscar negócio por CNJ
            deals = self.client.search_deals_by_cnj(cnj)

            if not deals:
                result.error_message = "Negócio não encontrado"
                logger.warning(f"CNJ {cnj}: negócio não encontrado")
                return result

            # Filtrar apenas negócios no estágio de deleção
            deletion_stage_deals = [
                d for d in deals if str(d.get("StageId")) == str(self.deletion_stage_id)
            ]

            if not deletion_stage_deals:
                result.error_message = f"Nenhum negócio encontrado no estágio de deleção ({self.deletion_stage_id})"
                logger.info(f"CNJ {cnj}: {result.error_message}, pulando")
                return result

            moved_any = False
            for deal in deletion_stage_deals:
                deal_id = deal.get("Id")
                current_stage = deal.get("StageId")

                if not deal_id:
                    logger.warning(f"CNJ {cnj}: negócio sem ID encontrado, pulando")
                    continue

                logger.info(
                    f"CNJ {cnj}: processando negócio no estágio de deleção (ID: {deal_id}, Stage: {current_stage})"
                )

                # Mover para o target_stage se não estiver já lá
                if str(current_stage) == str(self.target_stage_id):
                    logger.info(f"CNJ {cnj}: negócio {deal_id} já está no target_stage")
                else:
                    move_success = False
                    if self.dry_run:
                        logger.info(
                            f"[DRY-RUN] CNJ {cnj}: negócio {deal_id} seria movido para "
                            f"target_stage {self.target_stage_id}"
                        )
                        move_success = True
                    elif self.client.update_deal_stage(deal_id, self.target_stage_id):
                        logger.info(
                            f"CNJ {cnj}: negócio {deal_id} movido para target_stage {self.target_stage_id}"
                        )
                        move_success = True
                    else:
                        logger.error(f"CNJ {cnj}: falha ao mover negócio {deal_id}")

                    if move_success:
                        moved_any = True

            result.moved_successfully = moved_any

        except Exception as e:
            result.error_message = f"Erro inesperado: {e}"
            logger.error(f"CNJ {cnj}: erro inesperado - {e}")

        return result

    def _extract_cnj_from_deal(self, deal: Dict) -> Optional[str]:
        """
        Extrai o CNJ de um negócio Ploomes.

        Args:
            deal: Dicionário representando o negócio

        Returns:
            CNJ extraído ou None se não encontrado
        """
        other_properties = deal.get("OtherProperties", [])
        for prop in other_properties:
            if prop.get("FieldKey") == "deal_20E8290A-809B-4CF1-9345-6B264AED7830":
                value = str(prop.get("StringValue", "")).strip()
                if value:
                    return value

        # Tentativa de extrair CNJ do título, se presente
        title = str(deal.get("Title", ""))
        if title:
            match = re.search(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b", title)
            if match:
                return match.group(0)

        return None

    def _extract_origin_deal_id_from_deal(self, deal: Dict) -> Optional[int]:
        """
        Extrai o OriginDealId de um negócio Ploomes.

        Args:
            deal: Dicionário representando o negócio

        Returns:
            OriginDealId extraído ou None se não encontrado
        """
        # First, check if it's a direct field
        origin_deal_id = deal.get("OriginDealId")
        if origin_deal_id:
            return int(origin_deal_id)

        return None

    def _move_origin_deal(self, deal: Dict) -> None:
        """
        Move o deal de origem para o estágio configurado.

        Args:
            deal: Dicionário representando o negócio que foi movido para target_stage
        """
        deal_id = deal.get("Id")
        origin_deal_id = self._extract_origin_deal_id_from_deal(deal)

        logger.debug(f"Deal {deal_id}: OriginDealId={origin_deal_id}")

        if not origin_deal_id:
            logger.debug(f"Deal {deal_id}: OriginDealId não encontrado")
            return

        # Buscar o deal de origem para obter seu pipeline
        origin_deal = self.client.get_deal_by_id(origin_deal_id)

        if not origin_deal:
            logger.warning(f"Deal de origem {origin_deal_id} não encontrado")
            return

        origin_pipeline_id = origin_deal.get("PipelineId")
        logger.debug(
            f"Deal de origem {origin_deal_id} encontrado: "
            f"PipelineId={origin_pipeline_id}"
        )

        # Buscar configuração baseada no pipeline de origem
        origin_config = None
        mesa_name = None
        for mesa, config in self.origin_config.items():
            if config["pipeline_id"] == origin_pipeline_id:
                origin_config = config
                mesa_name = mesa
                logger.debug(
                    f"Configuração encontrada para pipeline {origin_pipeline_id} (mesa '{mesa}'): "
                    f"stage_id={config['stage_id']}"
                )
                break

        if not origin_config:
            logger.debug(
                f"Pipeline {origin_pipeline_id} do deal de origem não está na configuração"
            )
            return

        origin_stage_id = origin_config["stage_id"]

        if self.dry_run:
            logger.info(
                f"[DRY-RUN] Movendo deal de origem {origin_deal_id} "
                f"para estágio {origin_stage_id} no pipeline {origin_pipeline_id} (mesa '{mesa_name}')"
            )
        elif self.client.update_deal_stage(origin_deal_id, origin_stage_id):
            logger.info(
                f"Deal de origem {origin_deal_id} movido para estágio {origin_stage_id} (mesa '{mesa_name}')"
            )
        else:
            logger.error(f"Falha ao mover deal de origem {origin_deal_id}")

    def _extract_mesa_from_deal(self, deal: Dict) -> Optional[str]:
        """
        Extrai a Mesa de um negócio Ploomes.

        Args:
            deal: Dicionário representando o negócio

        Returns:
            Mesa extraída ou None se não encontrado
        """
        other_properties = deal.get("OtherProperties", [])
        for prop in other_properties:
            if prop.get("FieldKey") == "deal_6FB5087A-22DA-42E1-A993-D85C6BAECEA3":
                value = str(prop.get("StringValue", "")).strip()
                if value:
                    return value

        return None

    def _move_cnj_to_deletion_stage(self, cnj: str) -> ProcessingResult:
        """
        Move um negócio de um CNJ para o estágio de deleção.

        Args:
            cnj: CNJ a ser processado

        Returns:
            Resultado do processamento
        """
        result = ProcessingResult(cnj=cnj)
        logger.info(f"Processando CNJ: {cnj}")

        try:
            # 1. Buscar negócio por CNJ
            deals = self.client.search_deals_by_cnj(cnj)

            if not deals:
                result.error_message = "Negócio não encontrado"
                logger.warning(f"CNJ {cnj}: negócio não encontrado")
                return result

            if len(deals) == 1:
                deal = deals[0]
            else:
                # Procurar por deals no estágio de deleção
                deletion_stage_deals = [
                    d
                    for d in deals
                    if str(d.get("StageId")) == str(self.deletion_stage_id)
                ]
                if deletion_stage_deals:
                    if len(deletion_stage_deals) == 1:
                        deal = deletion_stage_deals[0]
                        logger.info(
                            f"CNJ {cnj}: múltiplos negócios encontrados, usando o que está no "
                            f"estágio de deleção (ID: {deal.get('Id')})"
                        )
                    else:
                        deal = deletion_stage_deals[0]
                        logger.warning(
                            f"CNJ {cnj}: múltiplos negócios no estágio de deleção "
                            f"({len(deletion_stage_deals)}), usando o primeiro (ID: {deal.get('Id')})"
                        )
                else:
                    result.error_message = (
                        f"Múltiplos negócios encontrados ({len(deals)}), mas nenhum está no "
                        f"estágio de deleção ({self.deletion_stage_id})"
                    )
                    logger.warning(f"CNJ {cnj}: {result.error_message}")
                    return result

            deal_id = deal.get("Id")
            current_stage = deal.get("StageId")

            if not deal_id:
                result.error_message = "ID do negócio não encontrado"
                return result

            result.deal_id = deal_id
            logger.info(
                f"CNJ {cnj}: negócio encontrado (ID: {deal_id}, Stage: {current_stage})"
            )

            # 2. Mover para o estágio de deleção se não estiver já lá
            if str(current_stage) == str(self.deletion_stage_id):
                result.moved_successfully = True
                logger.info(
                    f"CNJ {cnj}: negócio {deal_id} já está no estágio de deleção"
                )
            else:
                if self.client.update_deal_stage(deal_id, self.deletion_stage_id):
                    result.moved_successfully = True
                    logger.info(
                        f"CNJ {cnj}: negócio {deal_id} movido para estágio de deleção"
                    )
                else:
                    result.error_message = (
                        "Falha ao mover negócio para estágio de deleção"
                    )
                    logger.error(f"CNJ {cnj}: falha ao mover negócio {deal_id}")

        except Exception as e:
            result.error_message = f"Erro inesperado: {str(e)}"
            logger.error(f"CNJ {cnj}: erro inesperado - {e}")

        return result

    def _delete_all_deals_in_deletion_stage(self) -> Dict[str, int]:
        """
        Deleta todos os negócios que estão no estágio de deleção.

        Returns:
            Dicionário com contadores de deletados e pulados
        """
        deleted_count = 0
        skipped_count = 0

        try:
            # Buscar todos os negócios no estágio de deleção
            deals = self.client.search_deals_by_stage(self.deletion_stage_id)

            if not deals:
                logger.info("Nenhum negócio encontrado no estágio de deleção")
                return {"deleted_count": 0, "skipped_count": 0}

            logger.info(
                f"Encontrados {len(deals)} negócios no estágio de deleção para deletar"
            )

            for deal in deals:
                deal_id = deal.get("Id")
                if deal_id:
                    if self.client.delete_deal(deal_id):
                        deleted_count += 1
                    else:
                        skipped_count += 1
                        logger.warning(f"Falha ao deletar negócio {deal_id}")
                else:
                    skipped_count += 1
                    logger.warning(f"Negócio sem ID encontrado: {deal}")

        except Exception as e:
            logger.error(f"Erro ao deletar negócios no estágio de deleção: {e}")
            skipped_count += len(deals) if "deals" in locals() else 0

        logger.info(
            f"Deleção concluída: {deleted_count} deletados, {skipped_count} pulados"
        )
        return {"deleted_count": deleted_count, "skipped_count": skipped_count}

    @staticmethod
    def load_cnjs_from_excel(file_path: str) -> List[str]:
        """
        Carrega lista de CNJs de um arquivo Excel.

        Args:
            file_path: Caminho para o arquivo Excel

        Returns:
            Lista de CNJs válidos
        """
        try:
            df = pd.read_excel(file_path)

            # Verifica se existe coluna CNJ
            if "CNJ" not in df.columns:
                raise ValueError("Coluna 'CNJ' não encontrada no arquivo Excel")

            # Extrai CNJs não vazios
            cnjs = []
            for value in df["CNJ"].dropna():
                cnj_str = str(value).strip()
                if cnj_str:
                    cnjs.append(cnj_str)

            return cnjs

        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo Excel: {e}")

    def generate_report_excel(self, report: SyncReport, output_path: str) -> None:
        """
        Gera relatório Excel com os resultados do processamento.

        Args:
            report: Relatório a ser exportado
            output_path: Caminho onde salvar o relatório
        """
        # Cria DataFrame com estatísticas
        stats_data = {
            "Métrica": [
                "CNJs Carregados",
                "Deletados com Sucesso",
                "Falhas na Deleção",
            ],
            "Valor": [
                report.total_processed,
                report.successfully_deleted,
                report.skipped_deletions,
            ],
        }
        df_stats = pd.DataFrame(stats_data)

        # Salva em Excel
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df_stats.to_excel(writer, sheet_name="Estatísticas", index=False)

        logger.info(f"Relatório salvo em: {output_path}")
