import json
from pathlib import Path
import re
from typing import Optional

import Levenshtein

from .validator import is_valid_email


# Cache para escritórios carregados por mesa
_ESCRITORIOS_CACHE: dict[str, dict] = {}  # noqa: F824


def _load_valid_escritorios(mesa: Optional[str] = None) -> dict:
    """
    Carrega a lista válida de escritórios do arquivo JSON específico da mesa.
    O resultado é armazenado em cache para eficiência.

    Args:
        mesa: Nome da mesa (btblue, bbmd, 2bativos). Se None, usa arquivo antigo.

    Returns:
        Dicionário com nomes válidos de escritórios
    """
    cache_key = mesa or "default"
    if cache_key in _ESCRITORIOS_CACHE:
        return _ESCRITORIOS_CACHE[cache_key]

    # Obtém o caminho do arquivo JSON
    current_dir = Path(__file__).parent.parent.parent
    if mesa:
        json_path = current_dir / "utils" / f"escritorios_{mesa.lower()}.json"
    else:
        json_path = current_dir / "utils" / "escritorios.json"

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            _ESCRITORIOS_CACHE[cache_key] = data.get("escritorios", {})
            return _ESCRITORIOS_CACHE[cache_key]
    except (FileNotFoundError, json.JSONDecodeError):
        # Se não conseguir carregar, retorna dicionário vazio
        _ESCRITORIOS_CACHE[cache_key] = {}
        return _ESCRITORIOS_CACHE[cache_key]


def _find_best_match(
    input_name: str, valid_names: list, threshold: float = 0.95
) -> Optional[str]:
    """
    Encontra o melhor match usando Levenshtein distance.

    Args:
        input_name: Nome do escritório a ser procurado
        valid_names: Lista de nomes válidos
        threshold: Limiar mínimo de similaridade (0-1)

    Returns:
        Nome válido mais similar, ou None se nenhum match atingir o threshold
    """
    if not valid_names or not input_name:
        return None

    best_match = None
    best_score = 0.0

    # Normaliza o nome de entrada para comparação
    input_normalized = input_name.strip().lower()

    for valid_name in valid_names:
        valid_normalized = valid_name.strip().lower()

        # Calcula a similaridade usando Levenshtein
        similarity = Levenshtein.ratio(input_normalized, valid_normalized)

        if similarity > best_score:
            best_score = similarity
            best_match = valid_name

    # Retorna o melhor match se atingiu o threshold
    if best_score >= threshold:
        return best_match

    return None


def normalize_escritorio(
    escritorio_str: str, mesa: Optional[str] = None
) -> tuple[str, Optional[str]]:
    """
    Normaliza o nome do escritório verificando contra a lista válida da mesa.

    Se o nome exato (case-insensitive) for encontrado, retorna o nome válido.
    Se não encontrado, usa Levenshtein distance para encontrar o melhor match.
    Se nenhum match for encontrado, retorna o nome original.

    Args:
        escritorio_str: Nome do escritório da entrada
        mesa: Nome da mesa para carregar o arquivo correto

    Returns:
        Tupla (nome_normalizado, nome_original_ou_matched)
        - Se foi feito match via Levenshtein, retorna o original para logging
        - Se foi match exato, retorna None como segundo elemento
    """
    if not escritorio_str or not isinstance(escritorio_str, str):
        return "", None

    escritorio_str = escritorio_str.strip()
    if not escritorio_str:
        return "", None

    valid_escritorios = _load_valid_escritorios(mesa)
    if not valid_escritorios:
        return escritorio_str, None

    valid_names = list(valid_escritorios.keys())

    # Tenta match exato (case-insensitive)
    for valid_name in valid_names:
        if escritorio_str.lower() == valid_name.lower():
            return valid_name, None

    # Tenta fuzzy match com Levenshtein
    matched_name = _find_best_match(escritorio_str, valid_names, threshold=0.93)

    if matched_name:
        # Retorna o nome válido encontrado e o original para logging
        return matched_name, escritorio_str

    # Se nenhum match, retorna o original
    return escritorio_str, None


def normalize_cnj(cnj_str: str) -> str | None:
    if not cnj_str or not isinstance(cnj_str, str):
        return None
    digits = re.sub(r"\D", "", cnj_str)
    if len(digits) != 20:
        return None
    # Formato: NNNNNNN-DD.AAAA.J.TR.OOOO
    return f"{digits[:7]}-{digits[7:9]}.{digits[9:13]}.{digits[13]}.{digits[14:16]}.{digits[16:]}"


def normalize_phone(phone_str: str) -> str | None:
    if not phone_str or not isinstance(phone_str, str):
        return None
    digits = re.sub(r"\D", "", phone_str)
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    elif len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    else:
        return None


def normalize_email(email_str: str) -> str:
    if not email_str or not isinstance(email_str, str):
        return ""
    email = email_str.strip().lower()

    if not is_valid_email(email):
        return ""
    return email


def normalize_produto(produto_str: str) -> str:
    valid = ["Integral", "Honorários", "Reclamante"]
    if not produto_str or not isinstance(produto_str, str) or not produto_str.strip():
        return "Integral"
    produto_lower = produto_str.strip().lower()
    if produto_lower == "completa":
        return "Integral"
    for v in valid:
        if produto_lower == v.lower():
            return v
    return "Integral"


def extract_first_value(values_str: str, separator: str = ";") -> str:
    if not values_str or not isinstance(values_str, str):
        return ""
    parts = [v.strip() for v in values_str.split(separator) if v.strip()]
    return parts[0] if parts else ""
