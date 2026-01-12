# Mapeamento de nomes de negociadores
NEGOTIATOR_MAPPING = {
    "rômulo montenegro": "Maria Clara do Amaral Fonseca",
    "Rômulo Montenegro": "Maria Clara do Amaral Fonseca",
}


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
