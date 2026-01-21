---
applyTo: '**'
---

# Memória do Usuário - Gabriel Ribeiro Dias

## Informações Pessoais
- Nome: Gabriel Ribeiro Dias
- Email: gribeiro@btcreditos.com
- Empresa: BT Créditos (bbmd-bt)

## Preferências de Desenvolvimento
- Linguagem principal: Python
- Framework preferido: Pandas para processamento de dados
- Estilo de código: Segue Black formatter, Flake8, MyPy
- Controle de versão: Git com conventional commits
- Ambiente: Linux, VS Code, Virtualenv

## Projetos Recentes
### planilha-ploomes-parceiros
- **Descrição**: Sistema de processamento de planilhas para importação de parceiros na plataforma Ploomes
- **Tecnologias**: Python, Pandas, Ploomes API, OpenPyXL
- **Funcionalidades implementadas**:
  - Transformação de planilhas Excel para formato Ploomes
  - Validação e normalização de dados (CNJ, telefone, email, escritório)
  - Busca automática de escritório na API Ploomes quando campo vazio
  - Integração com variáveis de ambiente (.env)
  - Sistema de logging e relatórios de erro
- **Estrutura do projeto**:
  - `src/`: Código fonte principal
  - `tests/`: Testes unitários
  - `input/`: Planilhas de entrada
  - `output/`: Planilhas processadas
  - `logs/`: Arquivos de log
  - `utils/`: Utilitários e mapeamentos

## Padrões de Código
- Usa dotenv para carregar variáveis de ambiente
- Segue OWASP para segurança (validação de entrada, sanitização)
- Implementa rate limiting para APIs
- Usa logging estruturado com Loguru
- Mantém compatibilidade backward em mudanças

## Última Sessão (21/01/2026)
- **Tarefa**: Adicionar validação automática de escritório na planilha de importação
- **Implementação**:
  - Modificado `src/main.py` para aceitar `--api-token` e `--deletion-stage-id`
  - Atualizado `src/transformer.py` com método `_find_escritorio_from_ploomes()`
  - Busca negócio por CNJ no estágio de deleção
  - Usa Title como escritório, ou OrigemDealId se não houver
  - Carrega PLOOMES_API_TOKEN do .env
- **Commits**: Funcionalidade implementada e formatada com Black
- **Testes**: Todos passando (45 testes)

## Preferências de Comunicação
- Linguagem: Português brasileiro
- Estilo: Direto, técnico, com explicações claras
- Feedback: Prefere validação de mudanças com testes antes de commits
