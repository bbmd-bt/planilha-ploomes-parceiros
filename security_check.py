#!/usr/bin/env python3
"""
Script de verificação de segurança para o projeto planilha-ploomes-parceiros.

Este script executa verificações de segurança automatizadas incluindo:
- Análise de vulnerabilidades em dependências
- Análise estática de código para problemas de segurança
- Verificação de configurações seguras
"""

import subprocess  # nosec B404 - subprocess is needed for security checks
import sys
from pathlib import Path


def run_command(command: list, description: str) -> bool:
    """Executa um comando e retorna se foi bem-sucedido."""
    print(f"\n🔍 {description}")
    try:
        subprocess.run(
            command, capture_output=True, text=True, check=True
        )  # nosec B603
        print("✅ Sucesso")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Falhou: {e}")
        print(f"Saída de erro: {e.stderr}")
        return False


def main() -> int:
    """Executa todas as verificações de segurança."""
    print("🛡️  Iniciando verificações de segurança...")

    project_root = Path(__file__).parent

    # Verificar se estamos no ambiente virtual
    if not (project_root / "venv").exists():
        print(
            "❌ Ambiente virtual não encontrado. Execute: python3 -m venv venv && "
            "source venv/bin/activate && pip install -r requirements.txt"
        )
        return 1

    success = True

    # 1. Verificar vulnerabilidades em dependências
    success &= run_command(
        [sys.executable, "-m", "pip", "install", "--upgrade", "pip"], "Atualizando pip"
    )

    success &= run_command(
        [sys.executable, "-m", "safety", "scan", "--file", "requirements.txt"],
        "Verificando vulnerabilidades em dependências",
    )

    # 2. Análise estática com bandit
    success &= run_command(
        [sys.executable, "-m", "bandit", "-r", "src/"],
        "Executando análise de segurança com bandit",
    )

    # 3. Verificar se .env existe e está protegido
    env_file = project_root / ".env"
    if env_file.exists():
        print("\n🔍 Verificando arquivo .env")
        # Verificar se .env está no .gitignore
        gitignore = project_root / ".gitignore"
        if gitignore.exists():
            with open(gitignore, "r") as f:
                if ".env" in f.read():
                    print("✅ .env está no .gitignore")
                else:
                    print("❌ .env NÃO está no .gitignore - RISCO DE SEGURANÇA!")
                    success = False
        else:
            print("❌ .gitignore não encontrado")
            success = False
    else:
        print(
            "⚠️  Arquivo .env não encontrado - verifique se as variáveis de ambiente estão configuradas"
        )

    # 4. Verificar configurações de log
    print("\n🔍 Verificando configurações de log")
    main_file = project_root / "src" / "main.py"
    if main_file.exists():
        with open(main_file, "r") as f:
            content = f.read()
            if "serialize=True" in content:
                print("❌ Logs ainda usam serialize=True - pode expor dados sensíveis")
                success = False
            else:
                print("✅ Logs não usam serialização que possa expor dados")

    if success:
        print("\n🎉 Todas as verificações de segurança passaram!")
        return 0
    else:
        print(
            "\n💥 Algumas verificações de segurança falharam. Corrija os problemas antes de prosseguir."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
