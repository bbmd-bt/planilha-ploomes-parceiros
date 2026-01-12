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
from validator import is_valid_cnj, is_valid_phone


class PlanilhaTransformer:
    def __init__(self):
        self.errors = []

    def transform(self, input_df: pd.DataFrame) -> pd.DataFrame:
        output_rows = []
        for idx, row in input_df.iterrows():
            # CNJ
            cnj_raw = row.get("CNJ", "")
            cnj = normalize_cnj(cnj_raw)
            if not cnj:
                self.errors.append(f"Linha {idx}: CNJ inválido - Valor original: '{cnj_raw}'")
                cnj = ""
            # Nome do Lead
            nome_lead = row.get("Nome do Cliente", "")
            # Produto
            produto = normalize_produto(row.get("Produto", ""))
            # Negociador
            negociador = map_negotiator(row.get("Responsável", ""))
            # E-mail
            email_raw = extract_first_value(row.get("E-mail do Cliente", ""))
            email = normalize_email(email_raw)
            # Telefone
            tel_raw = extract_first_value(row.get("Telefones do Cliente", ""))
            telefone = normalize_phone(tel_raw)
            if tel_raw and not telefone:
                self.errors.append(f"Linha {idx}: Telefone inválido - Valor original: '{tel_raw}'")
                telefone = ""
            # Escritório
            escritorio_raw = row.get("Escritório", "")
            escritorio, original_escritorio = normalize_escritorio(escritorio_raw)
            if original_escritorio:
                # Se foi feito fuzzy match via Levenshtein
                self.errors.append(
                    f"Linha {idx}: Escritório corrigido via fuzzy match - "
                    f"Original: '{original_escritorio}' → Corrigido: '{escritorio}'"
                )
            # OAB
            oab = ""
            # Teste de Interesse
            teste_interesse = "Sim"
            # Recompra
            recompra = "Não"
            output_rows.append(
                {
                    "CNJ": cnj,
                    "Nome do Lead": nome_lead,
                    "Produto": produto,
                    "Negociador": negociador,
                    "E-mail": email,
                    "Telefone": telefone,
                    "OAB": oab,
                    "Escritório": escritorio,
                    "Teste de Interesse": teste_interesse,
                    "Recompra": recompra,
                }
            )
        return pd.DataFrame(output_rows)

    def get_error_report(self) -> str:
        if not self.errors:
            return "Nenhum erro encontrado."
        return "\n".join(self.errors)
