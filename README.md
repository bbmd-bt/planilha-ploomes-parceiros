# Transformador de Planilha Ploomes Parceiros

Script Python para transformar planilhas de parceiros no formato padrão do Ploomes.

## Estrutura do Projeto

```
planilha-ploomes-parceiros/
├── .github/
│   └── workflows/
│       └── ci.yml          # Configuração de CI/CD
├── src/
│   ├── __init__.py          # Inicialização do pacote
│   ├── validator.py         # Funções de validação
│   ├── normalizers.py       # Funções de normalização
│   ├── mapping.py           # Mapeamento de nomes de negociadores
│   ├── transformer.py       # Lógica de transformação
│   ├── main.py             # Script principal
│   ├── extract_escritorios.py # Extração de escritórios únicos
│   ├── ploomes_client.py    # Cliente para API Ploomes
│   ├── ploomes_sync.py      # Lógica de sincronização
│   └── delete_deals.py      # Script de deleção de negócios
├── tests/                   # Testes automatizados
├── input/                   # Planilhas de entrada (.xlsx)
├── output/                  # Planilhas de saída (.xlsx)
├── utils/                   # Utilitários e arquivos auxiliares
├── logs/                    # Arquivos de log de erros
├── requirements.txt         # Dependências do projeto
└── README.md               # Este arquivo
```

## Instalação

1. Crie e ative um ambiente virtual (recomendado):

```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Uso

### Modo Básico

Coloque sua planilha de entrada em `input/entrada.xlsx` e execute:

```bash
python src/main.py --mesa "Nome da Mesa"
```

### Modo Personalizado

Especifique caminhos personalizados:

```bash
python src/main.py --input "caminho/entrada.xlsx" --mesa "Nome da Mesa" --output "caminho/saida.xlsx" --log "caminho/log.txt" --log-level DEBUG
```

### Opções de Comando

- `--input`: Caminho para o arquivo de entrada (.xlsx)
- `--mesa`: Nome da mesa referente aos leads (obrigatório)
- `--output`: Caminho para o arquivo de saída (.xlsx, opcional)
- `--log`: Caminho para o arquivo de log (opcional)
- `--log-level`: Nível de log (DEBUG, INFO, WARNING, ERROR, padrão: INFO)

### Extração de Escritórios

Para extrair uma lista única de escritórios responsáveis de um arquivo CSV do Ploomes:

```bash
python src/extract_escritorios.py --input "caminho/arquivo.csv"
```

Este script:

- Lê arquivos CSV com coluna `payload` contendo dados JSON
- Extrai o campo `escritorio_responsavel` de cada registro
- Remove duplicatas e registros vazios
- Salva em `utils/escritorios.json` com formato otimizado para mapeamentos

## Formato da Planilha de Entrada

A planilha de entrada deve conter as seguintes colunas:

| Coluna               | Descrição                   | Obrigatório |
| -------------------- | --------------------------- | ----------- |
| CNJ                  | Número CNJ do processo      | Sim         |
| Nome do Cliente      | Nome do lead                | Sim         |
| Produto              | Tipo de produto             | Não         |
| Responsável          | Nome do negociador          | Sim         |
| E-mail do Cliente    | E-mails (separados por ;)   | Não         |
| Telefones do Cliente | Telefones (separados por ;) | Não         |
| Escritório           | Nome do escritório          | Não         |

## Formato da Planilha de Saída

A planilha de saída terá as seguintes colunas:

| Coluna             | Formato                                       | Exemplo                   |
| ------------------ | --------------------------------------------- | ------------------------- |
| CNJ                | 0000000-00.0000.0.00.0000                     | 1234567-89.2023.1.01.0001 |
| Nome do Lead       | Texto                                         | João Silva                |
| Produto            | À Definir, Honorários, Reclamante ou Integral | Honorários                |
| Negociador         | Texto                                         | Maria Santos              |
| E-mail             | email@dominio.com                             | joao@email.com            |
| Telefone           | (00) 00000-0000                               | (41) 99999-9999           |
| OAB                | (vazio)                                       |                           |
| Escritório         | Texto                                         | Escritório XYZ            |
| Teste de Interesse | Sempre "Sim"                                  | Sim                       |
| Recompra           | Sempre "Não"                                  | Não                       |

## Regras de Transformação

### CNJ

- Formato de saída: `NNNNNNN-DD.AAAA.J.TR.OOOO` (20 dígitos)
- Se inválido: campo fica vazio e erro é registrado

### Produto

- Valores válidos: `À Definir`, `Honorários`, `Reclamante`, `Integral`
- Se vazio ou inválido: usa `À Definir`

### Negociador

- Aplica mapeamento de nomes de negociadores conforme definido em `src/mapping.py`
- Exemplo: "Romulo Montenegro" → "Maria Clara do Amaral Fonseca"
- Mapeamento é case-insensitive
- Nomes não mapeados permanecem inalterados

### E-mail

- Pega apenas o primeiro e-mail se houver múltiplos
- Normaliza para minúsculas
- Aceita mesmo se inválido

### Telefone

- Pega apenas o primeiro telefone se houver múltiplos
- Aceita três formatos de entrada:
  - Internacional: `5541998710932`
  - Simples: `44 9846-1632`
  - Completo: `(81) 99665-4939`
- Formato de saída: `(DD) NNNNN-NNNN` ou `(DD) NNNN-NNNN`
- Se inválido: campo fica vazio e erro é registrado

## Log de Erros

O script gera um arquivo de log com todos os erros encontrados durante o processamento:

```
Linha 5: CNJ inválido - Valor original: '12345'
Linha 12: Telefone inválido - Valor original: '999'
```

Os logs são salvos na pasta `logs/` com timestamp no nome do arquivo.

## Exemplo de Execução

```bash
$ python src/main.py --input "input/Negócios em aberto.xlsx"
Lendo planilha de entrada: input/Negócios em aberto.xlsx
Salvando planilha de saída: output/saida.xlsx
Salvando log de erros: logs/processamento_20251212_154657.log
Processamento concluído.
Linhas processadas: 26
Linhas com erro: 2
Veja detalhes no log: logs/processamento_20251212_154657.log
```

## Solução de Problemas

### Erro: "ImportError: attempted relative import"

- Certifique-se de executar o script a partir do diretório raiz do projeto
- Use: `python src/main.py` (não `python main.py` dentro de src)

### Erro: "OSError: Cannot save file into a non-existent directory"

- Verifique se as pastas `input/`, `output/` e `logs/` existem
- O script criará as pastas automaticamente se necessário

### Valores NaN ou vazios

- O script trata corretamente células vazias do Excel
- Valores vazios resultam em strings vazias na saída

## Deleção de Negócios na Ploomes

Este projeto inclui um script independente para deletar negócios na Ploomes baseado em CNJs de um arquivo Excel.

### Funcionalidades

- Lê CNJs de um arquivo Excel
- Busca negócios correspondentes na API Ploomes
- Move negócios para um estágio específico
- Deleta apenas os negócios que foram movidos com sucesso
- Gera relatório detalhado do processamento

### Pipelines Suportados

| Pipeline           | Target Stage ID | Deletion Stage ID |
| ------------------ | --------------- | ----------------- |
| BT Blue Pipeline   | 110351686       | 110351653         |
| 2B Ativos Pipeline | 110351791       | 110351790         |
| BBMD Pipeline      | 110351793       | 110351792         |

### Uso do Script de Deleção

```bash
python src/delete_deals.py --input "input/cnjs_erro.xlsx" --api-token "SEU_TOKEN_API" --pipeline "BT Blue Pipeline"
```

### Opções do Comando

- `--input`: Caminho para o arquivo Excel de entrada (obrigatório)
- `--api-token`: Token de autenticação da API Ploomes (obrigatório)
- `--pipeline`: Nome do pipeline a ser usado (obrigatório)
- `--output`: Caminho para o relatório de saída (opcional)
- `--log`: Caminho para o arquivo de log (opcional)
- `--log-level`: Nível de log (DEBUG, INFO, WARNING, ERROR)
- `--dry-run`: Executa em modo teste (sem fazer alterações reais)

### Formato do Arquivo de Entrada

O arquivo Excel deve conter pelo menos uma coluna chamada `CNJ` com os números dos processos.

### Relatório de Saída

O script gera um relatório Excel com duas abas:

1. **Resultados Detalhados**: Lista cada CNJ processado com status de movimentação e deleção
2. **Estatísticas**: Resumo consolidado do processamento

### Regras de Processamento

1. **Sequência**: Move o negócio para o estágio alvo primeiro
2. **Validação**: Verifica se a movimentação foi bem-sucedida via API
3. **Deleção**: Só deleta negócios que foram movidos com sucesso
4. **Segurança**: Não deleta se a movimentação falhar

## Testes

Execute os testes automatizados:

```bash
python -m pytest tests/
```

## CI/CD

O projeto inclui configuração de CI/CD com GitHub Actions que executa testes automaticamente em cada push e pull request.

## Tecnologias Utilizadas

- Python 3.11+
- pandas 2.3.3
- openpyxl 3.1.2
- python-Levenshtein 0.27.3
- pytest 9.0.1
- requests 2.31.0
