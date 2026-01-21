"""
Testes para o módulo de configuração.
"""

from src.config import MESA_DELETION_STAGE_MAP, MESA_TARGET_STAGE_MAP


class TestMesaMapping:
    """Testes para mapeamentos de mesas."""

    def test_deletion_stage_map_btblue(self):
        """Verifica se btblue tem o deletion_stage_id correto."""
        assert MESA_DELETION_STAGE_MAP["btblue"] == 110351653

    def test_deletion_stage_map_2bativos(self):
        """Verifica se 2bativos tem o deletion_stage_id correto."""
        assert MESA_DELETION_STAGE_MAP["2bativos"] == 110351790

    def test_deletion_stage_map_bbmd(self):
        """Verifica se bbmd tem o deletion_stage_id correto."""
        assert MESA_DELETION_STAGE_MAP["bbmd"] == 110351792

    def test_target_stage_map_btblue(self):
        """Verifica se btblue tem o target_stage_id correto."""
        assert MESA_TARGET_STAGE_MAP["btblue"] == 110351686

    def test_target_stage_map_2bativos(self):
        """Verifica se 2bativos tem o target_stage_id correto."""
        assert MESA_TARGET_STAGE_MAP["2bativos"] == 110351791

    def test_target_stage_map_bbmd(self):
        """Verifica se bbmd tem o target_stage_id correto."""
        assert MESA_TARGET_STAGE_MAP["bbmd"] == 110351793

    def test_all_mesas_have_both_mappings(self):
        """Verifica se todas as mesas têm ambos os mapeamentos."""
        for mesa in MESA_DELETION_STAGE_MAP.keys():
            assert (
                mesa in MESA_TARGET_STAGE_MAP
            ), f"Mesa '{mesa}' tem deletion_stage_id mas não tem target_stage_id"

    def test_mesas_are_lowercase(self):
        """Verifica se todas as chaves são minúsculas."""
        for mesa in MESA_DELETION_STAGE_MAP.keys():
            assert mesa.islower(), f"Mesa '{mesa}' não está em minúsculas"
