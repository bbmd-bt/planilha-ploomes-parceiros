# Script de Validação de Interaction Records

## Visão Geral

O script `validate_interactions.py` é responsável por validar e atualizar Interaction Records de negócios em um estágio específico da Ploomes. Ele:

1. **Carrega** CNJs e descrições de erro de uma planilha Excel
2. **Busca** negócios em um estágio específico da Ploomes
3. **Valida** se cada negócio possui a interaction record correto
4. **Cria** interaction records para negócios que não os possuem
5. **Atualiza** o campo `lastinteractionid` do negócio

## Pré-requisitos

- Python 3.8+
- Dependências do projeto instaladas (veja [README.md](README.md))
- Token da API Ploomes configurado em `.env`
- Arquivo Excel com CNJs e descrições de erro

## Uso

### Básico

```bash
python3 src/validate_interactions.py \
  --input input/erros.xlsx \
  --stage-id 110351653
```

### Com Argumentos Opcionais

```bash
python3 src/validate_interactions.py \
  --input input/erros.xlsx \
  --stage-id 110351653 \
  --output output/relatorio_interactions.xlsx \
  --api-token seu_token_aqui \
  --log logs/validate.log \
  --log-level DEBUG
```

## Argumentos

### Obrigatórios

- `--input INPUT_FILE`: Caminho para o arquivo Excel com CNJs e descrições de erro
- `--stage-id STAGE_ID`: ID do estágio na Ploomes onde validar os negócios

### Opcionais

- `--output OUTPUT_FILE`: Caminho para salvar o relatório (padrão: `output/[data]/validate_interactions_[hora].xlsx`)
- `--api-token TOKEN`: Token da API Ploomes (padrão: `PLOOMES_API_TOKEN` do `.env`)
- `--log LOG_FILE`: Arquivo de log (padrão: `logs/validate_interactions_[timestamp].log`)
- `--log-level LEVEL`: Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR` (padrão: `INFO`)

## Formato do Arquivo de Input

O arquivo Excel deve conter as seguintes colunas:

### Colunas Obrigatórias

- **CNJ**: O número CNJ do negócio

### Colunas Opcionais (uma delas)

- **Erro**: Descrição do erro
- **Error**: Descrição do erro em inglês
- **Description**: Descrição do erro
- **Descrição**: Descrição do erro em português

Exemplo:

```
CNJ                        | Erro
1234567-89.0123.4.56.7890  | Erro de validação: CPF inválido
9876543-21.9876.5.43.2109  | Erro: Documento não encontrado
5555555-55.5555.5.55.5555  | Erro: Dados incompletos
```

## Fluxo de Processamento

Para cada negócio no estágio especificado:

1. **Extração de CNJ**: Tenta extrair o CNJ do negócio via:
   - Campo customizado `deal_20E8290A-809B-4CF1-9345-6B264AED7830`
   - Ou via título do negócio (regex)

2. **Busca de Erro**: Procura a descrição de erro na planilha usando o CNJ

3. **Verificação de Interaction**: Se houver erro, verifica se o negócio já possui um `LastInteractionRecordId`

4. **Criação de Interaction**: Se não existir, cria um novo Interaction Record com a descrição do erro

5. **Atualização de Campo**: Atualiza o `lastinteractionid` do negócio com o ID da interaction criada

## Saída

O script gera:

### Arquivo de Log

Exemplo: `logs/validate_interactions_20260122_143025.log`

Contém informações detalhadas sobre cada operação realizada.

### Relatório Excel

O relatório contém duas abas:

#### Abas do Relatório

**1. Estatísticas**
- Total de negócios
- Com interaction
- Sem interaction
- Interactions criadas
- LastInteractionId atualizados
- Erros

**2. Detalhes**
- Deal ID
- CNJ
- Tinha Interaction
- Interaction Criada
- LastInteractionId Atualizado
- Erro (se houver)

## Exemplos de Uso

### Exemplo 1: Validação Simples

```bash
cd /home/homol/planilha-ploomes-parceiros
source venv/bin/activate

python3 src/validate_interactions.py \
  --input input/test_interactions.xlsx \
  --stage-id 110351653
```

### Exemplo 2: Com Relatório Personalizado

```bash
python3 src/validate_interactions.py \
  --input input/erros_202601.xlsx \
  --stage-id 110351653 \
  --output output/validacao_interactions_jan.xlsx \
  --log logs/interactions_jan.log
```

### Exemplo 3: Modo Debug

```bash
python3 src/validate_interactions.py \
  --input input/erros.xlsx \
  --stage-id 110351653 \
  --log-level DEBUG
```

## Mensagens de Log

### Sucesso

```
[INFO] Interaction Record criado com sucesso para negócio 12345: ID=67890
[INFO] Deal 12345: LastInteractionRecordId atualizado para 67890
```

### Avisos

```
[WARNING] Deal 12345: Já possui interaction record
[WARNING] Deal 12345: Lead NÃO encontrado em Parceiros
```

### Erros

```
[ERROR] Deal 12345: Falha ao criar interaction record
[ERROR] Deal 12345: Falha ao atualizar lastinteractionid
```

## Stage IDs Disponíveis

Confira em [config.py](src/config.py) para os stage IDs de deleção e target:

- **btblue**: 110351653 (deleção) / 110351686 (target)
- **2bativos**: 110351790 (deleção) / 110351791 (target)
- **bbmd**: 110351792 (deleção) / 110351793 (target)

## Testes

Execute os testes unitários:

```bash
source venv/bin/activate
python3 -m pytest tests/test_validate_interactions.py -v
```

## Tratamento de Erros

### Arquivo de Input Não Encontrado

```
[ERROR] Arquivo de input não encontrado: input/erros.xlsx
```

Verifique se o arquivo existe no caminho especificado.

### Coluna 'CNJ' Não Encontrada

```
[ERROR] Coluna 'CNJ' não encontrada no arquivo Excel
```

Verifique se o arquivo Excel contém a coluna 'CNJ'.

### Token Não Configurado

```
[ERROR] Token PLOOMES_API_TOKEN não encontrado em variáveis de ambiente
```

Configure o token em `.env` ou passe via `--api-token`.

## Integração com CI/CD

O script pode ser integrado em workflows de CI/CD. Exemplo em GitHub Actions:

```yaml
- name: Validate Interactions
  run: |
    source venv/bin/activate
    python3 src/validate_interactions.py \
      --input input/erros.xlsx \
      --stage-id 110351653 \
      --output output/report.xlsx
```

## Performance

- Rate limiting: Respeita 120 requisições/minuto da API Ploomes
- Processamento sequencial de negócios para evitar sobrecarga
- Logs estruturados para fácil rastreamento

## Segurança

- Validação de entrada em todos os parâmetros
- Tokens nunca são armazenados em logs
- HTTPS para todas as requisições à API
- Sem serialização de dados sensíveis

## Troubleshooting

### Script Muito Lento

Verifique:
- Conexão com internet
- Número de negócios no estágio
- Rate limiting da API Ploomes

### Interactions Não Sendo Criadas

Verifique:
- Arquivo Excel tem a coluna 'CNJ'?
- Os CNJs correspondem aos campos nos negócios?
- O token tem permissão para criar interactions?

### Erro "Deal não encontrado"

Verifique:
- O stage-id está correto?
- Os negócios existem no estágio especificado?
