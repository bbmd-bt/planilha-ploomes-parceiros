import argparse
import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from clients.ploomes_client import PloomesClient

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


"""
Validação de Criador de Negócios na Ploomes

Este script valida quais negócios de uma lista de CNJs não foram criados
pelo usuário de integração na plataforma Ploomes.

Para cada CNJ fornecido em uma planilha Excel, o script:
1. Consulta a API Ploomes para buscar negócios associados ao CNJ
2. Verifica se o CreatorId do negócio é diferente do ID do usuário de integração (110026673)
3. Separa os negócios que não foram criados pelo usuário de integração em uma planilha de saída
4. Deleta a planilha de entrada após execução bem-sucedida

Uso:
    python src/validate_creator.py --input input/cnjs.xlsx --output output/resultado.xlsx

Argumentos:
    --input: Caminho da planilha Excel com coluna 'CNJ' (obrigatório)
    --output: Caminho da planilha de saída (opcional)
    --api-token: Token da API Ploomes (opcional, padrão: PLOOMES_API_TOKEN do .env)

Nota: A planilha de entrada será automaticamente deletada após uma execução bem-sucedida.

Exemplo de planilha de entrada:
    | CNJ                      |
    |--------------------------|
    | 0020490-20.2022.5.04.0104 |

Exemplo de planilha de saída:
    | CNJ | DealId | Title | CreatorId | StatusId | PipelineId |
    |-----|--------|-------|-----------|----------|------------|
    | ... | ...    | ...   | ...       | ...      | ...        |

Autor: Gabriel Ribeiro Dias
Data: Janeiro 2026
"""


def main() -> int:
    # Define o diretório base do projeto (um nível acima de src)
    base_dir = Path(__file__).parent.parent

    parser = argparse.ArgumentParser(
        description="Valida quais negócios não foram criados pelo usuário de integração na Ploomes."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Arquivo de entrada (xlsx) com coluna 'CNJ'",
    )
    parser.add_argument(
        "--output",
        default=None,
        type=Path,
        help="Arquivo de saída (xlsx). Se não informado, será criado automaticamente.",
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("PLOOMES_API_TOKEN"),
        help="Token da API Ploomes (padrão: PLOOMES_API_TOKEN do .env)",
    )
    args = parser.parse_args()

    # Verificar se o arquivo de entrada existe
    if not args.input.exists():
        logger.error(f"Arquivo de entrada não encontrado: {args.input}")
        return 1

    # Configurar output padrão
    if args.output is None:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        args.output = base_dir / "output" / f"nao_criados_integracao_{timestamp}.xlsx"

    # Garantir que o diretório de output existe
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Inicializar cliente Ploomes
    if not args.api_token:
        logger.error(
            "Token da API Ploomes não fornecido. Use --api-token ou defina PLOOMES_API_TOKEN no .env"
        )
        return 1

    client = PloomesClient(api_token=args.api_token)

    # Ler planilha de entrada
    try:
        df_input = pd.read_excel(args.input)
        if "CNJ" not in df_input.columns:
            logger.error("Coluna 'CNJ' não encontrada na planilha de entrada")
            return 1
    except Exception as e:
        logger.error(f"Erro ao ler planilha de entrada: {e}")
        return 1

    # ID do usuário de integração
    INTEGRATION_USER_ID = 110026673

    # Lista para armazenar negócios não criados pelo usuário de integração
    invalid_deals = []

    # Processar cada CNJ
    for idx, row in df_input.iterrows():
        cnj = str(row["CNJ"]).strip()
        if not cnj:
            continue

        logger.info(f"Processando CNJ: {cnj}")

        # Buscar negócios por CNJ
        deals = client.search_deals_by_cnj(cnj)

        for deal in deals:
            creator_id = deal.get("CreatorId")
            if creator_id != INTEGRATION_USER_ID:
                # Adicionar à lista de inválidos
                invalid_deals.append(
                    {
                        "CNJ": cnj,
                        "DealId": deal.get("Id"),
                        "Title": deal.get("Title"),
                        "CreatorId": creator_id,
                        "StatusId": deal.get("StatusId"),
                        "PipelineId": deal.get("PipelineId"),
                    }
                )

    # Salvar planilha de output se houver negócios inválidos
    if invalid_deals:
        df_output = pd.DataFrame(invalid_deals)
        df_output.to_excel(args.output, index=False)
        logger.info(f"Planilha salva em: {args.output}")
        logger.info(
            f"Total de negócios não criados pelo usuário de integração: {len(invalid_deals)}"
        )
    else:
        logger.info("Todos os negócios foram criados pelo usuário de integração.")

    # Deletar planilha de input após execução bem-sucedida
    try:
        args.input.unlink()
        logger.info(f"Planilha de entrada deletada: {args.input}")
    except Exception as e:
        logger.warning(f"Não foi possível deletar a planilha de entrada: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
