"""
Módulo de sincronização para processamento de negócios Ploomes.

Este módulo contém a lógica para processar CNJs de um arquivo Excel,
mover negócios para estágios específicos e deletar aqueles que falharam.
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Adicionar o diretório pai ao sys.path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

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
        dry_run: bool = False,
    ):
        self.client = client
        self.target_stage_id = target_stage_id
        self.deletion_stage_id = deletion_stage_id
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

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

        moved_count = 0
        failed_moves = 0

        for cnj in cnj_list:
            self.logger.info(f"Processando CNJ: {cnj}")
            try:
                # Buscar negócio por CNJ
                deals = self.client.search_deals_by_cnj(cnj)

                if not deals:
                    self.logger.warning(f"CNJ {cnj}: negócio não encontrado")
                    continue

                # Filtrar apenas negócios no estágio de deleção
                deletion_stage_deals = [
                    d
                    for d in deals
                    if str(d.get("StageId")) == str(self.deletion_stage_id)
                ]

                if not deletion_stage_deals:
                    self.logger.info(
                        f"CNJ {cnj}: nenhum negócio encontrado no estágio de deleção ({self.deletion_stage_id}), pulando"
                    )
                    continue

                if len(deletion_stage_deals) == 1:
                    deal = deletion_stage_deals[0]
                else:
                    deal = deletion_stage_deals[0]
                    self.logger.warning(
                        f"CNJ {cnj}: múltiplos negócios no estágio de deleção ({len(deletion_stage_deals)}), usando o primeiro (ID: {deal.get('Id')})"
                    )

                deal_id = deal.get("Id")
                current_stage = deal.get("StageId")

                if not deal_id:
                    self.logger.error(f"CNJ {cnj}: ID do negócio não encontrado")
                    failed_moves += 1
                    continue

                self.logger.info(
                    f"CNJ {cnj}: negócio encontrado no estágio de deleção (ID: {deal_id}, Stage: {current_stage})"
                )

                # Mover para o target_stage se não estiver já lá
                if str(current_stage) == str(self.target_stage_id):
                    self.logger.info(
                        f"CNJ {cnj}: negócio {deal_id} já está no target_stage"
                    )
                    moved_count += 1  # Consider as moved
                else:
                    if self.dry_run:
                        self.logger.info(
                            f"[DRY-RUN] CNJ {cnj}: negócio {deal_id} seria movido para target_stage {self.target_stage_id}"
                        )
                        moved_count += 1  # In dry-run, assume success
                    elif self.client.update_deal_stage(deal_id, self.target_stage_id):
                        moved_count += 1
                        self.logger.info(
                            f"CNJ {cnj}: negócio {deal_id} movido para target_stage {self.target_stage_id}"
                        )
                    else:
                        failed_moves += 1
                        self.logger.error(
                            f"CNJ {cnj}: falha ao mover negócio {deal_id}"
                        )

            except Exception as e:
                failed_moves += 1
                self.logger.error(f"CNJ {cnj}: erro inesperado - {e}")

        # Agora, deletar negócios no estágio de deleção que não estão na lista de CNJs preservados
        deleted_count = 0
        skipped_deletions = 0

        try:
            all_deletion_deals = self.client.search_deals_by_stage(
                self.deletion_stage_id
            )
            if all_deletion_deals:
                self.logger.info(
                    f"Encontrados {len(all_deletion_deals)} negócios no estágio de deleção para verificar deleção"
                )
                for deal in all_deletion_deals:
                    deal_cnj = self._extract_cnj_from_deal(deal)
                    if deal_cnj and deal_cnj not in cnj_list:
                        deal_id = deal.get("Id")
                        if deal_id:
                            if self.dry_run:
                                self.logger.info(
                                    f"[DRY-RUN] CNJ {deal_cnj}: negócio {deal_id} seria deletado"
                                )
                                deleted_count += 1
                            elif self.client.delete_deal(deal_id):
                                deleted_count += 1
                                self.logger.info(
                                    f"CNJ {deal_cnj}: negócio {deal_id} deletado"
                                )
                            else:
                                skipped_deletions += 1
                                self.logger.error(
                                    f"CNJ {deal_cnj}: falha ao deletar negócio {deal_id}"
                                )
                        else:
                            skipped_deletions += 1
                            self.logger.warning(
                                f"CNJ {deal_cnj}: negócio sem ID encontrado"
                            )
                    elif not deal_cnj:
                        self.logger.warning(
                            f"Negócio {deal.get('Id')} sem CNJ encontrado, pulando"
                        )
        except Exception as e:
            self.logger.error(f"Erro ao deletar negócios no estágio de deleção: {e}")

        report.successfully_moved = moved_count
        report.failed_movements = failed_moves
        report.successfully_deleted = deleted_count
        report.skipped_deletions = skipped_deletions

        self.logger.info(
            f"Processamento concluído: {moved_count} movidos, {failed_moves} falhas, {deleted_count} deletados, {skipped_deletions} pulados"
        )
        return report

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
                return str(prop.get("StringValue", "")).strip()
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
        self.logger.info(f"Processando CNJ: {cnj}")

        try:
            # 1. Buscar negócio por CNJ
            deals = self.client.search_deals_by_cnj(cnj)

            if not deals:
                result.error_message = "Negócio não encontrado"
                self.logger.warning(f"CNJ {cnj}: negócio não encontrado")
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
                        self.logger.info(
                            f"CNJ {cnj}: múltiplos negócios encontrados, usando o que está no estágio de deleção (ID: {deal.get('Id')})"
                        )
                    else:
                        deal = deletion_stage_deals[0]
                        self.logger.warning(
                            f"CNJ {cnj}: múltiplos negócios no estágio de deleção ({len(deletion_stage_deals)}), usando o primeiro (ID: {deal.get('Id')})"
                        )
                else:
                    result.error_message = f"Múltiplos negócios encontrados ({len(deals)}), mas nenhum está no estágio de deleção ({self.deletion_stage_id})"
                    self.logger.warning(f"CNJ {cnj}: {result.error_message}")
                    return result

            deal_id = deal.get("Id")
            current_stage = deal.get("StageId")

            if not deal_id:
                result.error_message = "ID do negócio não encontrado"
                return result

            result.deal_id = deal_id
            self.logger.info(
                f"CNJ {cnj}: negócio encontrado (ID: {deal_id}, Stage: {current_stage})"
            )

            # 2. Mover para o estágio de deleção se não estiver já lá
            if str(current_stage) == str(self.deletion_stage_id):
                result.moved_successfully = True
                self.logger.info(
                    f"CNJ {cnj}: negócio {deal_id} já está no estágio de deleção"
                )
            else:
                if self.client.update_deal_stage(deal_id, self.deletion_stage_id):
                    result.moved_successfully = True
                    self.logger.info(
                        f"CNJ {cnj}: negócio {deal_id} movido para estágio de deleção"
                    )
                else:
                    result.error_message = (
                        "Falha ao mover negócio para estágio de deleção"
                    )
                    self.logger.error(f"CNJ {cnj}: falha ao mover negócio {deal_id}")

        except Exception as e:
            result.error_message = f"Erro inesperado: {str(e)}"
            self.logger.error(f"CNJ {cnj}: erro inesperado - {e}")

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
                self.logger.info("Nenhum negócio encontrado no estágio de deleção")
                return {"deleted_count": 0, "skipped_count": 0}

            self.logger.info(
                f"Encontrados {len(deals)} negócios no estágio de deleção para deletar"
            )

            for deal in deals:
                deal_id = deal.get("Id")
                if deal_id:
                    if self.client.delete_deal(deal_id):
                        deleted_count += 1
                    else:
                        skipped_count += 1
                        self.logger.warning(f"Falha ao deletar negócio {deal_id}")
                else:
                    skipped_count += 1
                    self.logger.warning(f"Negócio sem ID encontrado: {deal}")

        except Exception as e:
            self.logger.error(f"Erro ao deletar negócios no estágio de deleção: {e}")
            skipped_count += len(deals) if "deals" in locals() else 0

        self.logger.info(
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

        self.logger.info(f"Relatório salvo em: {output_path}")
