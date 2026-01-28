# Transformador de Planilha Ploomes Parceiros

Script Python para transformar planilhas de parceiros no formato padrão do Ploomes.

## Segurança

Este projeto implementa várias medidas de segurança para proteger dados sensíveis e prevenir vulnerabilidades:

### Configuração Segura
- **Variáveis de Ambiente**: Tokens de API e credenciais são armazenados em arquivo `.env` (não versionado)
- **HTTPS**: Todas as comunicações com APIs externas usam HTTPS
- **Validação de Entrada**: Parâmetros de entrada são validados para prevenir injeções

### Verificações Automáticas
- **Análise de Segurança**: Uso do Bandit para detectar vulnerabilidades no código
- **Verificação de Dependências**: Uso do Safety para identificar vulnerabilidades em bibliotecas
- **Pre-commit Hooks**: Verificações automáticas antes de commits

### Como Executar Verificações de Segurança

```bash
# Executar verificações de segurança
python security_check.py

# Ou instalar e usar pre-commit
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

### Logs Seguros
- Logs não serializam dados que possam conter informações sensíveis
- Erros de API não expõem URLs completas ou tokens nos logs

## Estrutura do Projeto

```
planilha-ploomes-parceiros/
├── .github/
│   └── workflows/
│       └── ci.yml          # Configuração de CI/CD
├── src/
│   ├── __init__.py          # Inicialização do pacote
│   ├── config.py            # Configurações
│   ├── main.py             # Script principal
│   ├── validate_creator.py  # Validação de criador de negócios
│   ├── validate_interactions.py # Validação de interações
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── parceiros_client.py # Cliente para parceiros
│   │   └── ploomes_client.py    # Cliente para API Ploomes
│   ├── data_processing/
│   │   ├── __init__.py
│   │   ├── normalizers.py       # Funções de normalização
│   │   ├── transformer.py       # Lógica de transformação
│   │   └── validator.py         # Funções de validação
│   ├── database/
│   │   ├── __init__.py
│   │   └── db_updater.py       # Atualização de mapeamentos via API
│   ├── deletion/
│   │   ├── __init__.py
│   │   ├── delete_deals.py      # Script de deleção de negócios
│   │   └── delete_duplicate_deals.py # Remoção de duplicatas
│   ├── extraction/
│   │   ├── __init__.py
│   │   └── extract_escritorios.py # Extração de escritórios únicos
│   ├── sync/
│   │   ├── __init__.py
│   │   ├── mirror_pipeline.py   # Espelhamento de pipeline
│   │   └── ploomes_sync.py      # Lógica de sincronização
│   └── upload/
│       ├── __init__.py
│       └── upload_leads_history.py # Upload do histórico de leads
├── tests/                   # Testes automatizados
│   └── test_upload_leads_history.py # Testes do upload de histórico
├── input/                   # Planilhas de entrada (.xlsx)
├── output/                  # Planilhas de saída (.xlsx)
├── errors/                  # Planilhas de erros (.xlsx)
├── utils/                   # Utilitários e arquivos auxiliares
│   ├── escritorios.json     # Mapeamento de escritórios
│   └── negociadores.json    # Mapeamento de negociadores
├── logs/                    # Arquivos de log de erros
├── .env.example            # Exemplo de configuração de ambiente
├── requirements.txt         # Dependências do projeto
└── README.md               # Este arquivo
```

## Instalação

1. Crie e ative um ambiente virtual (recomendado):

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate  # Windows
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Configure o ambiente:

```bash
cp .env.example .env
```

Edite o arquivo `.env` com suas credenciais do banco de dados PostgreSQL.

## Upload do Histórico de Leads

Este script faz upload do histórico de leads bem-sucedidos e com erro para uma tabela PostgreSQL, permitindo rastrear o resultado da importação de leads por parceiro.

### Funcionalidades

- **Processamento Inteligente**: Identifica automaticamente leads bem-sucedidos (presentes apenas na planilha de sucesso) e leads com erro
- **UPSERT**: Atualiza registros existentes ou insere novos usando `ON CONFLICT`
- **Flexibilidade de Colunas**: Suporta tanto "Negociador" quanto "Responsável" nas planilhas
- **Modo Dry-Run**: Permite testar o processamento sem afetar o banco de dados
- **Logs Detalhados**: Acompanhamento completo do processamento

### Estrutura da Tabela

```sql
CREATE TABLE leads_parceiros_upload_history (
    id INTEGER PRIMARY KEY,
    cnj VARCHAR(255) NOT NULL UNIQUE,
    negociador VARCHAR(255) NOT NULL,
    mesa VARCHAR(255),
    error BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    escritorio TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Como Usar

#### Modo Básico

```bash
python src/upload/upload_leads_history.py \
  --success "input/todos_leads.xlsx" \
  --errors "input/leads_com_erro.xlsx" \
  --mesa "Nome da Mesa"
```

#### Modo Dry-Run (Recomendado para Testes)

```bash
python src/upload/upload_leads_history.py \
  --success "input/todos_leads.xlsx" \
  --errors "input/leads_com_erro.xlsx" \
  --mesa "Nome da Mesa" \
  --dry-run
```

#### Com Logging Detalhado

```bash
python src/upload/upload_leads_history.py \
  --success "input/todos_leads.xlsx" \
  --errors "input/leads_com_erro.xlsx" \
  --mesa "Nome da Mesa" \
  --log-level DEBUG \
  --log "logs/upload_history.log"
```

### Parâmetros

- `--success, -s`: Caminho para a planilha Excel com todos os leads (obrigatório)
- `--errors, -e`: Caminho para a planilha Excel com leads que falharam (obrigatório)
- `--mesa, -m`: Nome da mesa referente aos leads (obrigatório)
- `--dry-run`: Executa em modo teste, processa dados mas não envia ao banco
- `--log-level`: Nível de log (DEBUG, INFO, WARNING, ERROR) - padrão: INFO
- `--log`: Caminho para arquivo de log (opcional)

### Formato das Planilhas

#### Planilha de Sucesso (Todos os Leads)
Colunas obrigatórias:
- `CNJ`: Número do processo judicial
- `Negociador` ou `Responsável`: Nome do negociador responsável

Colunas opcionais:
- `Escritório`: Nome do escritório responsável

#### Planilha de Erros
Colunas obrigatórias:
- `CNJ`: Número do processo judicial
- `Negociador` ou `Responsável`: Nome do negociador responsável
- `Erro`: Mensagem de erro da importação

Colunas opcionais:
- `Escritório`: Nome do escritório responsável

### Configuração do Banco

Configure as variáveis de ambiente no arquivo `.env`:

```bash
# Opção 1: URL completa do banco
DATABASE_URL=postgresql://usuario:senha@localhost:5432/database

# Opção 2: Componentes separados
DB_HOST=localhost
DB_PORT=5432
DB_NAME=database
DB_USER=usuario
DB_PASSWORD=senha
```

### Exemplo de Saída

```
2024-01-20 10:30:15.123 | INFO     | __main__:run:230 - === Iniciando upload do histórico de leads ===
2024-01-20 10:30:15.123 | INFO     | __main__:run:231 - Planilha de sucesso: input/todos_leads.xlsx
2024-01-20 10:30:15.123 | INFO     | __main__:run:232 - Planilha de erros: input/leads_com_erro.xlsx
2024-01-20 10:30:15.123 | INFO     | __main__:run:233 - Mesa: Mesa JPA
2024-01-20 10:30:15.456 | INFO     | __main__:load_success_leads:102 - Carregados 187 leads da planilha de sucesso
2024-01-20 10:30:15.567 | INFO     | __main__:load_error_leads:128 - Carregados 184 leads com erro da planilha de erros
2024-01-20 10:30:15.678 | INFO     | __main__:process_leads:194 - Processados 3 leads bem-sucedidos e 184 leads com erro
2024-01-20 10:30:15.789 | INFO     | __main__:run:244 - === Upload concluído com sucesso ===
2024-01-20 10:30:15.789 | INFO     | __main__:run:245 - Total processado: 187
2024-01-20 10:30:15.789 | INFO     | __main__:run:246 - Leads bem-sucedidos: 3
2024-01-20 10:30:15.789 | INFO     | __main__:run:247 - Leads com erro: 184
```

**Nota**: Certifique-se de que o ambiente virtual esteja ativado (`source venv/bin/activate`) antes de executar qualquer comando.

## Validação de Criador de Negócios

Este script valida quais negócios de uma lista de CNJs (fornecida via planilha Excel) não foram criados pelo usuário de integração na Ploomes. Ele é útil para identificar negócios que podem ter sido criados manualmente ou por outros processos, garantindo a integridade dos dados automatizados.

### Funcionalidades

- **Consulta à API Ploomes**: Para cada CNJ na planilha de entrada, busca os negócios associados na Ploomes
- **Verificação de Criador**: Compara o `CreatorId` de cada negócio com o ID do usuário de integração (110026673)
- **Separação de Resultados**: Gera uma planilha separada apenas com os negócios que não foram criados pelo usuário de integração
- **Rate Limiting**: Respeita os limites de requisição da API Ploomes (120 req/min)
- **Logs Detalhados**: Acompanhamento completo do processamento com Loguru
- **Limpeza Automática**: Deleta a planilha de entrada após execução bem-sucedida

### Como Usar

#### Modo Básico

```bash
python src/validate_creator.py --input "input/cnjs_para_validar.xlsx"
```

#### Com Saída Personalizada

```bash
python src/validate_creator.py \
  --input "input/cnjs_para_validar.xlsx" \
  --output "output/negocios_manuais.xlsx" \
  --api-token "seu_token_aqui"
```

### Parâmetros

- `--input` (obrigatório): Caminho para a planilha Excel de entrada contendo a coluna "CNJ"
- `--output` (opcional): Caminho para a planilha Excel de saída. Se não informado, será criado automaticamente em `output/nao_criados_integracao_{timestamp}.xlsx`
- `--api-token` (opcional): Token da API Ploomes. Se não informado, usa a variável de ambiente `PLOOMES_API_TOKEN`

### Formato da Planilha de Entrada

A planilha de entrada deve conter pelo menos uma coluna chamada "CNJ" com os números dos processos no formato padrão (NNNNNNN-DD.AAAA.J.TR.OOOO).

| CNJ                      |
|--------------------------|
| 0020490-20.2022.5.04.0104 |
| 0001234-56.2023.8.01.0001 |
| ...                      |

### Formato da Planilha de Saída

A planilha de saída contém os negócios que não foram criados pelo usuário de integração, com as seguintes colunas:

| CNJ                      | DealId | Title          | CreatorId | StatusId | PipelineId |
|--------------------------|--------|----------------|-----------|----------|------------|
| 0020490-20.2022.5.04.0104 | 12345  | Processo XYZ   | 999999    | 10       | 5          |
| ...                      | ...    | ...            | ...       | ...      | ...        |

### Exemplo de Saída no Terminal

```
2026-01-27 10:00:00.000 | INFO     | __main__:main:45 - Processando CNJ: 0020490-20.2022.5.04.0104
2026-01-27 10:00:01.234 | INFO     | __main__:main:55 - Planilha salva em: output/nao_criados_integracao_20260127_100000.xlsx
2026-01-27 10:00:01.234 | INFO     | __main__:main:56 - Total de negócios não criados pelo usuário de integração: 5
```

### Configuração

Certifique-se de que o token da API Ploomes esteja configurado:

```bash
# No arquivo .env
PLOOMES_API_TOKEN=seu_token_aqui
```

Ou passe diretamente via parâmetro `--api-token`.

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
- `--mesa`: Nome da mesa referente aos leads (obrigatório). Mesas suportadas: `btblue`, `2bativos`, `bbmd`
- `--output`: Caminho para o arquivo de saída (.xlsx, opcional)
- `--log`: Caminho para o arquivo de log (opcional)
- `--log-level`: Nível de log (DEBUG, INFO, WARNING, ERROR, padrão: INFO)
- `--update-db`: Atualiza mapeamentos de escritórios e negociadores do banco de dados (opcional)
- `--deletion-stage-id`: ID do estágio de deleção na Ploomes (opcional). Se não informado, será detectado automaticamente pela mesa

### Detecção Automática de Estágio de Deleção

O script detecta automaticamente o `deletion_stage_id` com base na mesa fornecida. Você **não precisa mais** fornecer esse parâmetro manualmente:

```bash
# Antes (necessário fornecer --deletion-stage-id)
python src/main.py --input "entrada.xlsx" --mesa btblue --deletion-stage-id 110351653

# Agora (automático)
python src/main.py --input "entrada.xlsx" --mesa btblue
```

**Mapeamento automático:**
- `btblue` → deletion_stage_id: `110351653`
- `2bativos` → deletion_stage_id: `110351790`
- `bbmd` → deletion_stage_id: `110351792`

Se necessário, você ainda pode sobrescrever o valor automático fornecendo `--deletion-stage-id` manualmente.

**Nota**: O script deletará automaticamente o arquivo de entrada ao final da execução bem-sucedida.

### Extração de Escritórios

Para extrair uma lista única de escritórios responsáveis de um arquivo CSV do Ploomes:

```bash
python src/extraction/extract_escritorios.py --input "caminho/arquivo.csv"
```

Este script:

- Lê arquivos CSV com coluna `payload` contendo dados JSON
- Extrai o campo `escritorio_responsavel` de cada registro
- Remove duplicatas e registros vazios
- Salva em `utils/escritorios.json` com formato otimizado para mapeamentos

### Remoção de Negócios Duplicados

Para remover negócios duplicados de um funil específico da Ploomes baseado no CNJ:

```bash
python src/deletion/delete_duplicate_deals.py --pipeline-id <ID_DO_PIPELINE>
```

Este script:

- Recebe o ID do pipeline como parâmetro
- Busca todos os negócios no pipeline via API Ploomes
- Agrupa os negócios por CNJ
- Para cada conjunto de duplicatas, mantém o negócio mais antigo (baseado na data de criação)
- Remove os negócios duplicados mais recentes
- Suporta modo dry-run para simulação sem deletar dados

#### Opções do Comando

- `--pipeline-id`: ID do pipeline a ser processado (obrigatório)
- `--api-token`: Token da API Ploomes (opcional se definido em `PLOOMES_API_TOKEN`)
- `--dry-run`: Executa em modo simulação (não deleta nada)
- `--log-level`: Nível de log (DEBUG, INFO, WARNING, ERROR, padrão: INFO)

## Atualização de Mapeamentos

O script suporta atualização automática de mapeamentos de escritórios e negociadores.

### Funcionalidades

- **Escritórios e Negociadores**: Conecta à API da Parceiros usando credenciais baseadas na mesa (`--mesa`) e extrai nomes únicos de `escritorio_responsavel` e `negociador_responsavel` de todos os leads
- Atualiza `utils/escritorios.json` com mapeamentos de escritórios da API
- Atualiza `utils/negociadores.json` com mapeamentos de negociadores da API
- Integra-se ao fluxo principal via flag `--update-db`

### Configuração

#### Para Escritórios e Negociadores (API Parceiros)

Configure as credenciais da API Parceiros no arquivo `.env` baseadas na mesa:

- Para `btblue`: `PARCEIROS_BT_BLUE_USERNAME` e `PARCEIROS_BT_BLUE_PASSWORD`
- Para `2bativos`: `PARCEIROS_2B_ATIVOS_USERNAME` e `PARCEIROS_2B_ATIVOS_PASSWORD`
- Para `bbmd`: `PARCEIROS_BBMD_USERNAME` e `PARCEIROS_BBMD_PASSWORD`

### Uso com Atualização de Mapeamentos

Para executar a transformação com atualização automática dos mapeamentos:

```bash
python src/main.py --mesa "Nome da Mesa" --update-db
```

**Nota**: Se a atualização falhar, a execução será abortada para evitar processamento com dados desatualizados.

### Estrutura dos Dados JSON

Os arquivos `utils/escritorios.json` e `utils/negociadores.json` contêm mapeamentos no formato:

```json
{
  "1": "Escritório ABC",
  "2": "Escritório XYZ"
}
```

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
- Para scripts em subpastas, use o caminho completo, ex.: `python src/upload/upload_leads_history.py`

### Erro: "OSError: Cannot save file into a non-existent directory"

- Verifique se as pastas `input/`, `output/` e `logs/` existem
- O script criará as pastas automaticamente se necessário

### Valores NaN ou vazios

- O script trata corretamente células vazias do Excel
- Valores vazios resultam em strings vazias na saída

## Deleção de Negócios na Ploomes

Este projeto inclui um script independente para deletar negócios na Ploomes baseado em CNJs de um arquivo Excel.

### Funcionalidades

- Lê CNJs de um arquivo Excel (negócios a preservar)
- Busca todos os negócios no estágio de deleção criados antes das 17:00 do dia atual
- Exclui da deleção os negócios cujos CNJs estão na lista de preservação
- Deleta os negócios antigos restantes
- **Executa validação de interações para o estágio de destino**: Após mover os negócios preservados de volta ao estágio de origem, o script automaticamente executa o script de validação de interações (`validate_interactions.py`) para garantir que os negócios nos estágios de origem possuam as interaction records corretas. A validação é executada para cada estágio de origem único utilizado durante o processamento.
- **Faz upload do histórico no banco de dados**: Ao final da execução, o script automaticamente executa o upload do histórico de negócios (sucesso e erros) para a tabela `negociacoes` no banco de dados PostgreSQL, permitindo rastrear o resultado da deleção por mesa
- Gera relatório detalhado do processamento

### Pipelines Suportados

| Pipeline           | Target Stage ID | Deletion Stage ID | Mesa Key |
| ------------------ | --------------- | ----------------- | -------- |
| BT Blue Pipeline   | 110351686       | 110351653         | btblue   |
| 2B Ativos Pipeline | 110351791       | 110351790         | 2bativos |
| BBMD Pipeline      | 110351793       | 110351792         | bbmd     |

### Uso do Script de Deleção

```bash
# Usando token do arquivo .env (recomendado)
python src/deletion/delete_deals.py --input "input/cnjs_erro.xlsx" --pipeline "BT Blue Pipeline"

# Ou especificando token diretamente
python src/deletion/delete_deals.py --input "input/cnjs_erro.xlsx" --api-token "SEU_TOKEN_API" --pipeline "BT Blue Pipeline"

# Com logging detalhado
python src/deletion/delete_deals.py --input "input/cnjs_erro.xlsx" --pipeline "BT Blue Pipeline" --log-level DEBUG --log "logs/delecao.log"
```

### Configuração do Token da API

O token da API Ploomes pode ser configurado de duas formas:

1. **Arquivo .env** (recomendado):

   ```bash
   # Adicione ao arquivo .env
   PLOOMES_API_TOKEN=33561FACC9647F23BFD0865B3D474D88F40F35E5307ABCC73986812497D3A7F1C329C405B1AFD2B15023A56641950E8D5084FC63B3995E064B911CB2DF834509
   ```

2. **Parâmetro de linha de comando**:
   ```bash
   --api-token "SEU_TOKEN_API"
   ```

### Configuração de Credenciais Parceiros (Opcional)

Para validar negócios contra a API da Parceiros antes de deletar, configure as credenciais no arquivo `.env`:

```bash
# BT Blue
PARCEIROS_BT_BLUE_USERNAME=seu_usuario_btblue
PARCEIROS_BT_BLUE_PASSWORD=sua_senha_btblue

# 2B Ativos
PARCEIROS_2B_ATIVOS_USERNAME=seu_usuario_2bativos
PARCEIROS_2B_ATIVOS_PASSWORD=sua_senha_2bativos

# BBMD
PARCEIROS_BBMD_USERNAME=seu_usuario_bbmd
PARCEIROS_BBMD_PASSWORD=sua_senha_bbmd
```

Se essas credenciais não estiverem configuradas, a validação contra Parceiros será pulada, mas o script continuará funcionando normalmente.

### Configuração do Banco de Dados

Para que o upload do histórico funcione, configure as variáveis de banco de dados no arquivo `.env`:

```bash
# Opção 1: URL completa
DATABASE_URL=postgresql://usuario:senha@localhost:5432/database

# Opção 2: Componentes separados
DB_HOST=localhost
DB_PORT=5432
DB_NAME=database
DB_USER=usuario
DB_PASSWORD=senha
```

### Opções do Comando

- `--input`: Caminho para o arquivo Excel de entrada (obrigatório)
- `--api-token`: Token de autenticação da API Ploomes (opcional se configurado no .env)
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

1. **Carregamento**: Carregar CNJs do arquivo de entrada (estes negócios devem ser preservados, exceto aqueles com erro "já existe" que serão deletados)
2. **Filtragem de Preservação**: CNJs com erro contendo "já existe" são removidos da lista de preservação e marcados para deleção
3. **Busca de Antigos**: Buscar todos os negócios no estágio de deleção criados antes das 17:00 do dia atual
4. **Filtragem**: Excluir da deleção os negócios cujos CNJs estão na lista de preservação filtrada
5. **Deleção**: Deletar os negócios antigos filtrados (incluindo aqueles que tinham erro "já existe")
6. **Validação de Interações**: Executar validação de interações para estágios de origem usados
7. **Upload no Banco**: Fazer upload do histórico de deleção na tabela `negociacoes`
8. **Preservação**: Os negócios do arquivo de entrada sem erro "já existe" permanecem intocados na Ploomes

### Estrutura da Tabela de Upload

O script espera encontrar a tabela `negociacoes` com a seguinte estrutura:

```sql
CREATE TABLE negociacoes (
    id SERIAL PRIMARY KEY,
    cnj VARCHAR(255) NOT NULL,
    negociador VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mesa VARCHAR(255),
    error BOOLEAN DEFAULT FALSE,
    error_message TEXT
);
```

**Nota**: O script deletará automaticamente o arquivo de entrada ao final da execução bem-sucedida.

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
- psycopg2-binary 2.9.9
- python-dotenv 1.0.0
