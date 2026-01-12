"""
Módulo de sincronização para processamento de negócios Ploomes.

Este módulo contém a lógica para processar CNJs de um arquivo Excel,
mover negócios para estágios específicos e deletar aqueles que falharam.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from ploomes_client import PloomesClient


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
    results: List[ProcessingResult] = None

    def __post_init__(self):
        if self.results is None:
            self.results = []


class PloomesSync:
    """
    Classe responsável pela sincronização de negócios na Ploomes.

    Args:
        client: Instância do cliente Ploomes
        target_stage_id: ID do estágio para onde mover os negócios
        deletion_stage_id: ID do estágio onde a deleção deve ocorrer
    """

    def __init__(self, client: PloomesClient, target_stage_id: int, deletion_stage_id: int):
        self.client = client
        self.target_stage_id = target_stage_id
        self.deletion_stage_id = deletion_stage_id
        self.logger = logging.getLogger(__name__)

    def process_cnj_list(self, cnj_list: List[str]) -> SyncReport:
        """
        Processa uma lista de CNJs seguindo as regras de negócio.

        Regras:
        1. Para cada CNJ, buscar negócio na Ploomes
        2. Mover negócio para target_stage_id
        3. Se movimento falhar, pular deleção
        4. Se movimento for bem-sucedido, deletar o negócio

        Args:
            cnj_list: Lista de CNJs para processar

        Returns:
            Relatório consolidado do processamento
        """
        report = SyncReport()

        for cnj in cnj_list:
            result = self._process_single_cnj(cnj)
            report.results.append(result)
            report.total_processed += 1

            if result.moved_successfully:
                report.successfully_moved += 1
                if result.deleted_successfully:
                    report.successfully_deleted += 1
            else:
                report.failed_movements += 1
                if result.error_message and "não encontrado" not in result.error_message.lower():
                    report.skipped_deletions += 1

        self.logger.info(f"Processamento concluído: {report.total_processed} CNJs processados")
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
        self.logger.info(f"Processando CNJ: {cnj}")

        try:
            # 1. Buscar negócio por CNJ
            deals = self.client.search_deals_by_cnj(cnj)

            if not deals:
                result.error_message = "Negócio não encontrado"
                self.logger.warning(f"CNJ {cnj}: negócio não encontrado")
                return result

            if len(deals) > 1:
                result.error_message = f"Múltiplos negócios encontrados ({len(deals)})"
                self.logger.warning(f"CNJ {cnj}: múltiplos negócios encontrados, pulando")
                return result

            deal = deals[0]
            deal_id = deal.get("Id")
            current_stage = deal.get("StageId")

            if not deal_id:
                result.error_message = "ID do negócio não encontrado"
                return result

            result.deal_id = deal_id
            self.logger.info(f"CNJ {cnj}: negócio encontrado (ID: {deal_id}, Stage: {current_stage})")

            # 2. Mover para estágio alvo
            if not self.client.update_deal_stage(deal_id, self.target_stage_id):
                result.error_message = "Falha ao mover para estágio alvo"
                self.logger.error(f"CNJ {cnj}: falha ao mover negócio {deal_id} para estágio {self.target_stage_id}")
                return result

            result.moved_successfully = True
            self.logger.info(f"CNJ {cnj}: negócio {deal_id} movido com sucesso para estágio {self.target_stage_id}")

            # 3. Deletar o negócio (apenas se movimento foi bem-sucedido)
            if self.client.delete_deal(deal_id):
                result.deleted_successfully = True
                self.logger.info(f"CNJ {cnj}: negócio {deal_id} deletado com sucesso")
            else:
                result.error_message = "Falha na deleção após movimento bem-sucedido"
                self.logger.error(f"CNJ {cnj}: falha ao deletar negócio {deal_id}")

        except Exception as e:
            result.error_message = f"Erro inesperado: {str(e)}"
            self.logger.error(f"CNJ {cnj}: erro inesperado - {e}")

        return result

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
        # Cria DataFrame com resultados detalhados
        data = []
        for result in report.results:
            data.append({
                "CNJ": result.cnj,
                "Deal ID": result.deal_id or "",
                "Movido com Sucesso": "Sim" if result.moved_successfully else "Não",
                "Deletado com Sucesso": "Sim" if result.deleted_successfully else "Não",
                "Erro": result.error_message or ""
            })

        df_results = pd.DataFrame(data)

        # Cria DataFrame com estatísticas
        stats_data = {
            "Métrica": [
                "Total Processado",
                "Movidos com Sucesso",
                "Deletados com Sucesso",
                "Falhas na Movimentação",
                "Deleções Puladas"
            ],
            "Valor": [
                report.total_processed,
                report.successfully_moved,
                report.successfully_deleted,
                report.failed_movements,
                report.skipped_deletions
            ]
        }
        df_stats = pd.DataFrame(stats_data)

        # Salva em Excel com múltiplas abas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_results.to_excel(writer, sheet_name='Resultados Detalhados', index=False)
            df_stats.to_excel(writer, sheet_name='Estatísticas', index=False)

        self.logger.info(f"Relatório salvo em: {output_path}")
