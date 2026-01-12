#!/usr/bin/env python3
"""
Script para extrair lista única de escritórios responsáveis do CSV do Ploomes.

Este script lê um arquivo CSV contendo dados JSON na coluna 'payload',
extrai o campo 'escritorio_responsavel' de cada registro e salva uma lista
única em formato JSON para facilitar mapeamentos futuros.

Uso:
    python src/extract_escritorios.py --input caminho/arquivo.csv
"""

import argparse
import csv
import json
import os
import sys
from typing import Set


def extract_escritorio_from_payload(payload_str: str) -> str:
    """
    Extrai o valor do campo 'escritorio_responsavel' de uma string JSON escapada.

    Args:
        payload_str: String contendo JSON com aspas escapadas

    Returns:
        Nome do escritório responsável ou string vazia se não encontrado
    """
    try:
        # Remove aspas externas se existirem
        if payload_str.startswith('"') and payload_str.endswith('"'):
            payload_str = payload_str[1:-1]

        # Desserializa o JSON (com aspas escapadas)
        data = json.loads(payload_str)

        # Extrai o escritório responsável
        escritorio = data.get('escritorio_responsavel', '')

        # Remove espaços extras
        return escritorio.strip() if escritorio else ''

    except (json.JSONDecodeError, KeyError, TypeError):
        # Retorna vazio se houver erro no parsing
        return ''


def extract_unique_escritorios(csv_path: str) -> Set[str]:
    """
    Lê o arquivo CSV e extrai os escritórios únicos.

    Args:
        csv_path: Caminho para o arquivo CSV

    Returns:
        Conjunto de nomes únicos de escritórios (sem vazios)
    """
    escritorios = set()

    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            # Detecta automaticamente o delimitador
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter

            reader = csv.DictReader(csvfile, delimiter=delimiter)

            for row in reader:
                payload = row.get('payload', '')
                if payload:
                    escritorio = extract_escritorio_from_payload(payload)
                    if escritorio:  # Só adiciona se não estiver vazio
                        escritorios.add(escritorio)

    except FileNotFoundError:
        print(f"Erro: Arquivo não encontrado: {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Erro ao processar arquivo CSV: {e}")
        sys.exit(1)

    return escritorios


def save_escritorios_json(escritorios: Set[str], output_path: str) -> None:
    """
    Salva os escritórios em formato JSON otimizado para mapeamento.

    Args:
        escritorios: Conjunto de nomes únicos de escritórios
        output_path: Caminho onde salvar o arquivo JSON
    """
    # Converte para lista ordenada para consistência
    escritorios_list = sorted(list(escritorios))

    # Cria estrutura otimizada para mapeamento "de para"
    data = {
        "escritorios": {esc: esc for esc in escritorios_list},  # chave = valor original
        "total": len(escritorios_list)
    }

    # Garante que o diretório existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Salva com indentação para legibilidade
    with open(output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=2)


def main():
    """Função principal do script."""
    parser = argparse.ArgumentParser(
        description="Extrai lista única de escritórios do CSV do Ploomes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python src/extract_escritorios.py --input dados.csv
  python src/extract_escritorios.py --input "input/dados_ploomes.csv"
        """
    )

    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Caminho para o arquivo CSV de entrada'
    )

    parser.add_argument(
        '--output', '-o',
        default=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils', 'escritorios.json'),
        help='Caminho para o arquivo JSON de saída (padrão: utils/escritorios.json)'
    )

    args = parser.parse_args()

    # Verifica se o arquivo de entrada existe
    if not os.path.exists(args.input):
        print(f"Erro: Arquivo de entrada não encontrado: {args.input}")
        sys.exit(1)

    print(f"Processando arquivo: {args.input}")

    # Extrai escritórios únicos
    escritorios = extract_unique_escritorios(args.input)

    if not escritorios:
        print("Aviso: Nenhum escritório encontrado no arquivo CSV")
        sys.exit(0)

    # Salva em JSON
    save_escritorios_json(escritorios, args.output)

    print(f"✅ Extração concluída!")
    print(f"   Escritórios únicos encontrados: {len(escritorios)}")
    print(f"   Arquivo salvo em: {args.output}")

    # Mostra alguns exemplos
    exemplos = list(escritorios)[:5]
    if exemplos:
        print(f"   Exemplos: {', '.join(exemplos)}")


if __name__ == "__main__":
    main()
