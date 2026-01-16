import pandas as pd

from mapping import map_negotiator
from normalizers import (
    extract_first_value,
    normalize_cnj,
    normalize_email,
    normalize_escritorio,
    normalize_phone,
    normalize_produto,
)


class PlanilhaTransformer:
    def __init__(self):
        self.errors = []

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
        output_data["Negociador"] = (
            input_df.get("Responsável", pd.Series(dtype=str))
            .fillna("")
            .apply(map_negotiator)
        )

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
        escritorio_raw_series = input_df.get("Escritório", pd.Series(dtype=str)).fillna(
            ""
        )
        escritorio_series, original_series = zip(
            *escritorio_raw_series.apply(normalize_escritorio)
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
