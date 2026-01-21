import sys

# Adicionar o diretório src ao path para imports
sys.path.insert(0, "src")

import pandas as pd
from data_processing.normalizers import (
    normalize_cnj,
    normalize_phone,
    normalize_email,
    normalize_produto,
    extract_first_value,
)
from data_processing.transformer import PlanilhaTransformer


def test_normalize_cnj():
    assert normalize_cnj("12345678901234567890") == "1234567-89.0123.4.56.7890"
    assert normalize_cnj("123456789") is None


def test_normalize_phone():
    assert normalize_phone("11999999999") == "(11) 99999-9999"
    assert normalize_phone("123") is None


def test_normalize_email():
    assert normalize_email("Test@Example.Com") == "test@example.com"
    assert normalize_email("invalid") == ""


def test_normalize_produto():
    assert normalize_produto("Honorários") == "Honorários"
    assert normalize_produto("invalid") == "Integral"
    assert normalize_produto("Completa") == "Integral"


def test_extract_first_value():
    assert extract_first_value("value1; value2") == "value1"
    assert extract_first_value("") == ""


def test_transformer():
    # Criar DataFrame de teste
    data = {
        "CNJ": ["12345678901234567890"],
        "Nome do Cliente": ["João Silva"],
        "Produto": ["Honorários"],
        "Responsável": ["Maria"],
        "E-mail do Cliente": ["joao@email.com"],
        "Telefones do Cliente": ["11999999999"],
        "Escritório": ["Escritório ABC"],
    }
    df = pd.DataFrame(data)

    transformer = PlanilhaTransformer()
    result = transformer.transform(df)

    assert len(result) == 1
    assert result.iloc[0]["CNJ"] == "1234567-89.0123.4.56.7890"
    assert result.iloc[0]["Nome do Lead"] == "João Silva"
    assert result.iloc[0]["Produto"] == "Honorários"
    assert result.iloc[0]["E-mail"] == "joao@email.com"
    assert result.iloc[0]["Telefone"] == "(11) 99999-9999"


if __name__ == "__main__":
    test_normalize_cnj()
    test_normalize_phone()
    test_normalize_email()
    test_normalize_produto()
    test_extract_first_value()
    test_transformer()
    print("All tests passed")
