"""
Script para validar e atualizar Interaction Records de negócios em um estágio específico.

Este script:
1. Carrega CNJs e descrições de erro de um arquivo Excel
2. Busca negócios em um estágio específico
3. Valida se cada negócio possui a interaction record correto
4. Se não tiver, cria a interaction e atualiza lastinteractionid
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from src.clients.ploomes_client import PloomesClient


# Carregar variáveis de ambiente
load_dotenv()


@dataclass
class InteractionValidationResult:
    """Resultado da validação de uma interação."""

    deal_id: int
    cnj: Optional[str] = None
    had_correct_interaction: bool = False
    had_wrong_interaction: bool = False
    interaction_created: bool = False
    last_interaction_updated: bool = False
    error_message: Optional[str] = None


@dataclass
class InteractionValidationReport:
    """Relatório consolidado da validação de interactions."""

    total_deals: int = 0
    deals_with_correct_interaction: int = 0
    deals_with_wrong_interaction: int = 0
    deals_without_interaction: int = 0
    interactions_created: int = 0
    last_interaction_updated: int = 0
    errors: int = 0
    results: List[InteractionValidationResult] = field(default_factory=list)


class InteractionValidator:
    """
    Classe responsável pela validação e atualização de Interaction Records.

    Args:
        client: Instância do cliente Ploomes
        cnj_errors: Mapeamento de CNJ para descrição de erro
    """

    def __init__(
        self, client: PloomesClient, cnj_errors: Optional[Dict[str, str]] = None
    ):
        self.client = client
        self.cnj_errors = cnj_errors or {}
        self.logger = logger

    @staticmethod
    def load_cnj_errors_from_excel(file_path: str) -> Dict[str, str]:
        """
        Carrega CNJs e descrições de erros de um arquivo Excel.

        Espera colunas: 'CNJ' e 'Erro' (ou 'Error')

        Args:
            file_path: Caminho para o arquivo Excel

        Returns:
            Dicionário mapeando CNJ para descrição de erro
        """
        try:
            df = pd.read_excel(file_path)

            # Verificar colunas obrigatórias
            if "CNJ" not in df.columns:
                raise ValueError("Coluna 'CNJ' não encontrada no arquivo Excel")

            # Determinar qual coluna tem a descrição de erro
            error_column = None
            for col in ["Erro", "Error", "Description", "Descrição"]:
                if col in df.columns:
                    error_column = col
                    break

            if not error_column:
                logger.warning(
                    "Nenhuma coluna de erro encontrada (procurou: Erro, Error, Description, Descrição)"
                )
                error_column = None

            cnj_errors = {}
            for _, row in df.iterrows():
                cnj_value = row.get("CNJ")
                if pd.isna(cnj_value):
                    continue

                cnj_str = str(cnj_value).strip()
                if cnj_str:
                    error_desc = ""
                    if error_column:
                        error_value = row.get(error_column)
                        if pd.notna(error_value):
                            error_desc = str(error_value).strip()

                    cnj_errors[cnj_str] = error_desc

            logger.info(f"Carregados {len(cnj_errors)} CNJs com erros do arquivo Excel")
            return cnj_errors

        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo Excel: {e}")

    @staticmethod
    def _extract_cnj_from_deal(deal: Dict) -> Optional[str]:
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

    def _check_interaction_status(
        self, deal_id: int, error_desc: str
    ) -> Tuple[bool, bool]:
        """
        Verifica o status da interaction para um negócio.

        Args:
            deal_id: ID do negócio
            error_desc: Descrição do erro a buscar

        Returns:
            Tupla (has_correct_interaction, has_wrong_interaction)
        """
        if not error_desc:
            return False, False

        try:
            # Buscar deal para obter LastInteractionRecordId
            deal = self.client.get_deal_by_id(deal_id)
            if not deal:
                return False, False

            # Verificar LastInteractionRecordId
            last_interaction_id = deal.get("LastInteractionRecordId")
            if not last_interaction_id:
                return False, False

            # Buscar o conteúdo da interaction record
            interaction_record = self.client.get_interaction_record_by_id(
                last_interaction_id
            )
            if not interaction_record:
                logger.debug(f"Interaction Record {last_interaction_id} não encontrado")
                return False, False

            # Comparar o conteúdo da interaction com a descrição do erro
            interaction_content = interaction_record.get("Content", "").strip()
            error_desc_stripped = error_desc.strip()

            if interaction_content == error_desc_stripped:
                logger.debug(
                    f"Deal {deal_id} já possui Interaction Record correto "
                    f"(ID: {last_interaction_id}, conteúdo corresponde)"
                )
                return True, False
            else:
                logger.debug(
                    f"Deal {deal_id} possui Interaction Record (ID: {last_interaction_id}), "
                    f"mas conteúdo não corresponde. Conteúdo atual: '{interaction_content[:100]}...', "
                    f"Esperado: '{error_desc_stripped[:100]}...'"
                )
                return False, True

        except Exception as e:
            logger.warning(f"Erro ao verificar interaction para deal {deal_id}: {e}")
            return False, False

    def validate_interactions_in_stage(
        self, stage_id: int
    ) -> InteractionValidationReport:
        """
        Valida e atualiza interactions de todos os negócios em um estágio.

        Args:
            stage_id: ID do estágio a validar

        Returns:
            Relatório consolidado da validação
        """
        report = InteractionValidationReport()

        try:
            # Buscar todos os negócios no estágio
            deals = self.client.search_deals_by_stage(stage_id)

            if not deals:
                logger.info(f"Nenhum negócio encontrado no estágio {stage_id}")
                return report

            report.total_deals = len(deals)
            logger.info(f"Encontrados {len(deals)} negócios no estágio {stage_id}")

            for deal in deals:
                result = self._validate_single_deal(deal)
                report.results.append(result)

                # Atualizar contadores
                if result.error_message:
                    report.errors += 1
                elif result.had_correct_interaction:
                    report.deals_with_correct_interaction += 1
                elif result.had_wrong_interaction:
                    report.deals_with_wrong_interaction += 1
                else:
                    report.deals_without_interaction += 1
                    if result.interaction_created:
                        report.interactions_created += 1
                    if result.last_interaction_updated:
                        report.last_interaction_updated += 1

        except Exception as e:
            logger.error(f"Erro ao validar interactions no estágio {stage_id}: {e}")

        return report

    def _validate_single_deal(self, deal: Dict) -> InteractionValidationResult:
        """
        Valida e atualiza interaction de um único negócio.

        Args:
            deal: Dicionário do negócio

        Returns:
            Resultado da validação
        """
        deal_id = deal.get("Id")

        if not deal_id:
            result = InteractionValidationResult(
                deal_id=0
            )  # Use 0 as placeholder for invalid deals
            result.error_message = "ID do negócio não encontrado"
            logger.warning(f"Negócio sem ID encontrado: {deal}")
            return result

        result = InteractionValidationResult(deal_id=deal_id)

        try:
            # Extrair CNJ
            cnj = self._extract_cnj_from_deal(deal)
            result.cnj = cnj

            # Obter descrição de erro para este CNJ
            error_desc = self.cnj_errors.get(cnj, "") if cnj else ""

            if not error_desc:
                logger.debug(
                    f"Deal {deal_id} (CNJ: {cnj}): Nenhuma descrição de erro encontrada"
                )
                return result

            # Verificar status da interaction
            has_correct, has_wrong = self._check_interaction_status(deal_id, error_desc)

            if has_correct:
                result.had_correct_interaction = True
                logger.info(
                    f"Deal {deal_id} (CNJ: {cnj}): Já possui interaction record correto"
                )
                return result

            if has_wrong:
                result.had_wrong_interaction = True
                logger.info(
                    f"Deal {deal_id} (CNJ: {cnj}): Possui interaction record com conteúdo incorreto, será atualizado"
                )
                # Continua para criar nova interaction

            # Criar interaction record
            interaction_id = self.client.create_interaction_record(deal_id, error_desc)

            if not interaction_id:
                result.error_message = "Falha ao criar interaction record"
                logger.error(
                    f"Deal {deal_id} (CNJ: {cnj}): Falha ao criar interaction record"
                )
                return result

            result.interaction_created = True
            logger.info(
                f"Deal {deal_id} (CNJ: {cnj}): Interaction record criado com ID {interaction_id}"
            )

            # Atualizar lastinteractionid
            if self.client.update_deal_last_interaction_record(deal_id, interaction_id):
                result.last_interaction_updated = True
                logger.info(
                    f"Deal {deal_id} (CNJ: {cnj}): LastInteractionRecordId atualizado para {interaction_id}"
                )
            else:
                result.error_message = "Falha ao atualizar lastinteractionid"
                logger.error(
                    f"Deal {deal_id} (CNJ: {cnj}): Falha ao atualizar lastinteractionid"
                )

        except Exception as e:
            result.error_message = f"Erro inesperado: {str(e)}"
            logger.error(f"Deal {deal_id}: Erro inesperado - {e}")

        return result

    def generate_report_excel(
        self, report: InteractionValidationReport, output_path: str
    ) -> None:
        """
        Gera relatório Excel com os resultados da validação.

        Args:
            report: Relatório a ser exportado
            output_path: Caminho onde salvar o relatório
        """
        try:
            # Criar DataFrame com detalhes de cada deal
            results_data = {
                "Deal ID": [r.deal_id for r in report.results],
                "CNJ": [r.cnj or "N/A" for r in report.results],
                "Tinha Interaction Correta": [
                    "Sim" if r.had_correct_interaction else "Não"
                    for r in report.results
                ],
                "Tinha Interaction Incorreta": [
                    "Sim" if r.had_wrong_interaction else "Não" for r in report.results
                ],
                "Interaction Criada": [
                    "Sim" if r.interaction_created else "Não" for r in report.results
                ],
                "LastInteractionId Atualizado": [
                    "Sim" if r.last_interaction_updated else "Não"
                    for r in report.results
                ],
                "Erro": [r.error_message or "OK" for r in report.results],
            }
            df_results = pd.DataFrame(results_data)

            # Criar DataFrame com estatísticas
            stats_data = {
                "Métrica": [
                    "Total de Negócios",
                    "Com Interaction Correta",
                    "Com Interaction Incorreta",
                    "Sem Interaction",
                    "Interactions Criadas",
                    "LastInteractionId Atualizados",
                    "Erros",
                ],
                "Valor": [
                    report.total_deals,
                    report.deals_with_correct_interaction,
                    report.deals_with_wrong_interaction,
                    report.deals_without_interaction,
                    report.interactions_created,
                    report.last_interaction_updated,
                    report.errors,
                ],
            }
            df_stats = pd.DataFrame(stats_data)

            # Salvar em Excel com múltiplas abas
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                df_stats.to_excel(writer, sheet_name="Estatísticas", index=False)
                df_results.to_excel(writer, sheet_name="Detalhes", index=False)

            logger.info(f"Relatório salvo em: {output_path}")

        except Exception as e:
            logger.error(f"Erro ao gerar relatório Excel: {e}")


def main() -> int:
    """Função principal do script."""
    base_dir = Path(__file__).parent.parent

    parser = argparse.ArgumentParser(
        description="Valida e atualiza Interaction Records de negócios em um estágio."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Arquivo Excel com CNJs e descrições de erro",
    )
    parser.add_argument(
        "--stage-id",
        type=int,
        required=True,
        help="ID do estágio a validar na Ploomes",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Arquivo de saída com relatório (xlsx). Se não informado, será criado automaticamente.",
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("PLOOMES_API_TOKEN"),
        help="Token da API Ploomes (padrão: PLOOMES_API_TOKEN do .env)",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Arquivo de log. Se não informado, será criado em logs/",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nível de log (DEBUG, INFO, WARNING, ERROR)",
    )

    args = parser.parse_args()

    # Validar token
    if not args.api_token:
        logger.error("Token PLOOMES_API_TOKEN não encontrado em variáveis de ambiente")
        return 1

    # Validar arquivo de input
    if not args.input.exists():
        logger.error(f"Arquivo de input não encontrado: {args.input}")
        return 1

    # Configurar logging
    logger.remove()
    log_level = args.log_level.upper()

    if args.log:
        log_file = args.log
    else:
        logs_dir = base_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        log_file = (
            logs_dir
            / f"validate_interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

    logger.add(
        log_file,
        level=log_level,
        format="{time} | {level} | {name}:{function}:{line} | {message}",
    )
    logger.add(sys.stderr, level=log_level, format="{message}")

    logger.info("Iniciando validação de Interaction Records")
    logger.info(f"Arquivo de input: {args.input}")
    logger.info(f"Stage ID: {args.stage_id}")

    try:
        # Carregar CNJs e erros da planilha
        logger.info("Carregando CNJs e erros da planilha...")
        cnj_errors = InteractionValidator.load_cnj_errors_from_excel(str(args.input))

        # Criar cliente Ploomes
        logger.info("Conectando à API Ploomes...")
        client = PloomesClient(args.api_token)

        # Criar validador
        validator = InteractionValidator(client, cnj_errors)

        # Validar interactions
        logger.info(f"Validando interactions no estágio {args.stage_id}...")
        report = validator.validate_interactions_in_stage(args.stage_id)

        # Gerar relatório
        if args.output:
            output_file = args.output
        else:
            output_dir = base_dir / "output" / datetime.now().strftime("%d-%m-%Y")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = (
                output_dir
                / f"validate_interactions_{datetime.now().strftime('%H%M%S')}.xlsx"
            )

        logger.info(f"Gerando relatório em: {output_file}")
        validator.generate_report_excel(report, str(output_file))

        # Exibir resumo
        logger.info("=" * 60)
        logger.info("RESUMO DA VALIDAÇÃO DE INTERACTIONS")
        logger.info("=" * 60)
        logger.info(f"Total de negócios: {report.total_deals}")
        logger.info(f"Com interaction correta: {report.deals_with_correct_interaction}")
        logger.info(f"Com interaction incorreta: {report.deals_with_wrong_interaction}")
        logger.info(f"Sem interaction: {report.deals_without_interaction}")
        logger.info(f"Interactions criadas: {report.interactions_created}")
        logger.info(f"LastInteractionId atualizados: {report.last_interaction_updated}")
        logger.info(f"Erros: {report.errors}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
