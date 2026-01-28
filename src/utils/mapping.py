import json
from pathlib import Path
from typing import Optional

# Cache para mapeamentos de negociadores por mesa
_NEGOTIATOR_CACHE: dict[str, dict] = {}  # noqa: F824


def _load_negotiator_mapping(mesa: Optional[str] = None) -> dict:
    """
    Carrega o mapeamento de negociadores do arquivo JSON específico da mesa.

    Args:
        mesa: Nome da mesa (btblue, bbmd, 2bativos). Se None, usa arquivo antigo.

    Returns:
        Dicionário com mapeamento de negociadores
    """
    cache_key = mesa or "default"
    if cache_key in _NEGOTIATOR_CACHE:
        return _NEGOTIATOR_CACHE[cache_key]

    _base_dir = Path(__file__).parent.parent
    if mesa:
        _neg_file = _base_dir / "utils" / f"negociadores_{mesa.lower()}.json"
    else:
        _neg_file = _base_dir / "utils" / "negociadores.json"

    try:
        with open(_neg_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            _NEGOTIATOR_CACHE[cache_key] = data.get("negociadores", {})
            return _NEGOTIATOR_CACHE[cache_key]
    except FileNotFoundError:
        _NEGOTIATOR_CACHE[cache_key] = {}
        return _NEGOTIATOR_CACHE[cache_key]


def map_negotiator(name: str, mesa: Optional[str] = None) -> str:
    """
    Mapeia o nome do negociador para o nome correto baseado na mesa.

    Args:
        name: Nome original do negociador
        mesa: Nome da mesa para carregar o arquivo correto

    Returns:
        Nome mapeado ou o nome original se não houver mapeamento
    """
    if not name or not isinstance(name, str):
        return name or ""

    # Remove espaços extras e normaliza
    normalized_name = name.strip()

    negotiator_mapping = _load_negotiator_mapping(mesa)

    # Verifica mapeamento exato primeiro
    if normalized_name in negotiator_mapping:
        return negotiator_mapping[normalized_name]

    # Verifica mapeamento case-insensitive
    for key, value in negotiator_mapping.items():
        if normalized_name.lower() == key.lower():
            return value

    # Retorna o nome original se não houver mapeamento
    return normalized_name
