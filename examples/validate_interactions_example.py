"""Exemplo de uso do módulo validate_interactions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.validate_interactions import InteractionValidator

load_dotenv()


def example_extract_cnj():
    """Exemplo extraindo CNJ de diferentes formatos."""
    print("\n" + "=" * 60)
    print("EXEMPLO: Extração de CNJ")
    print("=" * 60)

    deal1 = {
        "Title": "Deal 1",
        "OtherProperties": [
            {
                "FieldKey": "deal_20E8290A-809B-4CF1-9345-6B264AED7830",
                "StringValue": "1234567-89.0123.4.56.7890",
            }
        ],
    }

    deal2 = {
        "Title": "Deal 9876543-21.9876.5.43.2109",
        "OtherProperties": [],
    }

    print("\n📝 Deal com CNJ em OtherProperties:")
    cnj1 = InteractionValidator._extract_cnj_from_deal(deal1)
    print(f"CNJ encontrado: {cnj1}")

    print("\n📝 Deal com CNJ no título:")
    cnj2 = InteractionValidator._extract_cnj_from_deal(deal2)
    print(f"CNJ encontrado: {cnj2}")


if __name__ == "__main__":
    print("\n🚀 Exemplos de Uso do Módulo validate_interactions\n")
    example_extract_cnj()
    print("\n" + "=" * 60)
    print("✓ Exemplos concluídos!")
    print("=" * 60)
