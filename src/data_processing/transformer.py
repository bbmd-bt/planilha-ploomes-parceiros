import sys
from pathlib import Path

# Adicionar o diretório pai ao sys.path para imports funcionarem
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from typing import Optional

from utils.mapping import map_negotiator
from .normalizers import (
    extract_first_value,
    normalize_cnj,
    normalize_email,
    normalize_escritorio,
    normalize_phone,
    normalize_produto,
)


class PlanilhaTransformer:
    def __init__(self, ploomes_client=None, deletion_stage_id=None, mesa=None):
        self.errors = []
        self.ploomes_client = ploomes_client
        self.deletion_stage_id = deletion_stage_id
        self.mesa = mesa

    def transform(self, input_df: pd.DataFrame) -> pd.DataFrame:
        # Usar operações vetorizadas para melhor performance
        output_data = {}

        # CNJ
        cnj_series = (
            input_df.get("CNJ", pd.Series(dtype=str)).fillna("").apply(normalize_cnj)
        )
        invalid_cnj_mask = cnj_series.isna() | (cnj_series == "")
        cnj_raw_series = input_df.get("CNJ", pd.Series(dtype=str)).fillna("")
        for idx in invalid_cnj_mask[invalid_cnj_mask].index:
            cnj_raw = cnj_raw_series.iloc[idx]
            if cnj_raw:
                self.errors.append(
                    f"Linha {idx}: CNJ inválido - Valor original: '{cnj_raw}'"
                )
        output_data["CNJ"] = cnj_series.fillna("")

        # Nome do Lead
        output_data["Nome do Lead"] = input_df.get(
            "Nome do Cliente", pd.Series(dtype=str)
        ).fillna("")

        # Produto
        output_data["Produto"] = (
            input_df.get("Produto", pd.Series(dtype=str))
            .fillna("")
            .apply(normalize_produto)
        )

        # Negociador
        negociador_series = (
            input_df.get("Responsável", pd.Series(dtype=str))
            .fillna("")
            .apply(map_negotiator)
        )
        # Se mesa for BBMD, substituir "Franciele Menezes" por "Iasmin Barbosa"
        # e preencher vazios com "Iasmin Barbosa"
        if self.mesa and self.mesa.upper() == "BBMD":

            def adjust_negociador(x):
                if not x or x.strip() == "":
                    return "Iasmin Barbosa"
                elif x.strip().lower() == "franciele menezes":
                    return "Iasmin Barbosa"
                else:
                    return x

            negociador_series = negociador_series.apply(adjust_negociador)
        output_data["Negociador"] = negociador_series

        # E-mail
        email_raw_series = (
            input_df.get("E-mail do Cliente", pd.Series(dtype=str))
            .fillna("")
            .apply(extract_first_value)
        )
        output_data["E-mail"] = email_raw_series.apply(normalize_email)

        # Telefone
        tel_raw_series = (
            input_df.get("Telefones do Cliente", pd.Series(dtype=str))
            .fillna("")
            .apply(extract_first_value)
        )
        telefone_series = tel_raw_series.apply(normalize_phone)
        invalid_phone_mask = (
            telefone_series.isna() & tel_raw_series.notna() & (tel_raw_series != "")
        )
        for idx in invalid_phone_mask[invalid_phone_mask].index:
            tel_raw = tel_raw_series.iloc[idx]
            self.errors.append(
                f"Linha {idx}: Telefone inválido - Valor original: '{tel_raw}'"
            )
        output_data["Telefone"] = telefone_series.fillna("")

        # Escritório
        escritorio_raw_series = input_df.get(
            "Escritório", pd.Series("", index=input_df.index, dtype=str)
        )
        results = escritorio_raw_series.apply(normalize_escritorio)
        escritorio_series = results.apply(lambda x: x[0])
        original_series = results.apply(lambda x: x[1])

        # Para escritórios vazios, tentar buscar na Ploomes
        if self.ploomes_client and self.deletion_stage_id:
            cnj_series = output_data["CNJ"]  # Já processado acima
            for idx, escritorio in enumerate(escritorio_series):
                if not escritorio:  # Se vazio
                    cnj = cnj_series.iloc[idx]
                    if cnj:  # Se há CNJ
                        found_escritorio = self._find_escritorio_from_ploomes(cnj)
                        if found_escritorio:
                            escritorio_series = list(escritorio_series)
                            escritorio_series[idx] = found_escritorio
                            self.errors.append(
                                f"Linha {idx}: Escritório preenchido via Ploomes - "
                                f"CNJ: '{cnj}' → '{found_escritorio}'"
                            )

        output_data["Escritório"] = pd.Series(escritorio_series)
        # Adicionar erros para fuzzy matches
        for idx, original in enumerate(original_series):
            if original:
                self.errors.append(
                    f"Linha {idx}: Escritório corrigido via fuzzy match - "
                    f"Original: '{original}' → Corrigido: '{escritorio_series[idx]}'"
                )

        # Campos fixos
        output_data["OAB"] = ""
        output_data["Teste de Interesse"] = "Sim"
        output_data["Recompra"] = "Não"

        return pd.DataFrame(output_data)

    def get_error_report(self) -> str:
        if not self.errors:
            return "Nenhum erro encontrado."
        return "\n".join(self.errors)

    def _find_escritorio_from_ploomes(self, cnj: str) -> Optional[str]:
        """
        Busca o escritório na Ploomes para um CNJ dado.

        Primeiro busca o negócio no estágio de deleção com o CNJ.
        Se o negócio tem Title, usa como escritório.
        Caso contrário, pega o OriginDealId e busca o negócio de origem, usando o Title dele.

        Args:
            cnj: CNJ do negócio

        Returns:
            Nome do escritório ou None se não encontrado
        """
        if not self.ploomes_client or not self.deletion_stage_id:
            return None

        # Buscar negócios no estágio de deleção com o CNJ
        deals = self.ploomes_client.search_deals_by_cnj(cnj)
        if not deals:
            return None

        # Filtrar apenas os no estágio de deleção
        deletion_deals = [
            deal for deal in deals if deal.get("StageId") == self.deletion_stage_id
        ]
        if not deletion_deals:
            return None

        # Pegar o primeiro negócio
        deal = deletion_deals[0]

        # Se tem Title, usar como escritório
        title = deal.get("Title")
        if title and title.strip():
            return title.strip()

        # Caso contrário, pegar OriginDealId
        origin_deal_id = deal.get("OriginDealId")
        if origin_deal_id:
            origin_deal = self.ploomes_client.get_deal_by_id(origin_deal_id)
            if origin_deal:
                origin_title = origin_deal.get("Title")
                if origin_title and origin_title.strip():
                    return origin_title.strip()

        return None
