"""
Arquivo de configuração para o sistema de processamento de planilhas Ploomes.

Contém mapeamentos de mesas para seus respectivos stage IDs na Ploomes.
"""

# Mapeamento de mesas para seus deletion stage IDs
# Baseado na documentação de pipelines suportados
MESA_DELETION_STAGE_MAP = {
    "btblue": 110351653,  # BT Blue Pipeline
    "2bativos": 110351790,  # 2B Ativos Pipeline
    "bbmd": 110351792,  # BBMD Pipeline
}

# Mapeamento de mesas para seus target stage IDs (opcional, para referência)
MESA_TARGET_STAGE_MAP = {
    "btblue": 110351686,  # BT Blue Pipeline
    "2bativos": 110351791,  # 2B Ativos Pipeline
    "bbmd": 110351793,  # BBMD Pipeline
}
