import json
from pathlib import Path

# Carrega mapeamento de negociadores do arquivo JSON
NEGOTIATOR_MAPPING = {}
_base_dir = Path(__file__).parent.parent
_neg_file = _base_dir / "utils" / "negociadores.json"
try:
    with open(_neg_file, "r", encoding="utf-8") as f:
        NEGOTIATOR_MAPPING = json.load(f)
except FileNotFoundError:
    pass  # Usa dicionário vazio se arquivo não encontrado


def map_negotiator(name: str) -> str:
    """
    Mapeia o nome do negociador para o nome correto.

    Args:
        name: Nome original do negociador

    Returns:
        Nome mapeado ou o nome original se não houver mapeamento
    """
    if not name or not isinstance(name, str):
        return name or ""

    # Remove espaços extras e normaliza
    normalized_name = name.strip()

    # Verifica mapeamento exato primeiro
    if normalized_name in NEGOTIATOR_MAPPING:
        return NEGOTIATOR_MAPPING[normalized_name]

    # Verifica mapeamento case-insensitive
    for key, value in NEGOTIATOR_MAPPING.items():
        if normalized_name.lower() == key.lower():
            return value

    # Retorna o nome original se não houver mapeamento
    return normalized_name
