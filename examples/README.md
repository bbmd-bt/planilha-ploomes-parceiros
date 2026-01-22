# Exemplos de Uso

Este diretório contém exemplos práticos de como usar os módulos do projeto.

## validate_interactions_example.py

Demonstra como usar a classe `InteractionValidator` para:

- Extrair CNJ de negócios em diferentes formatos
- Validar e atualizar Interaction Records
- Processar planilhas Excel

### Executar

```bash
cd ..
source venv/bin/activate
python examples/validate_interactions_example.py
```

### Saída Esperada

```
🚀 Exemplos de Uso do Módulo validate_interactions

============================================================
EXEMPLO: Extração de CNJ
============================================================

📝 Deal com CNJ em OtherProperties:
CNJ encontrado: 1234567-89.0123.4.56.7890

📝 Deal com CNJ no título:
CNJ encontrado: 9876543-21.9876.5.43.2109

============================================================
✓ Exemplos concluídos!
============================================================
```

## Como Usar o Módulo em Seu Código

```python
from src.clients.ploomes_client import PloomesClient
from src.validate_interactions import InteractionValidator

# Carregar CNJs e erros
cnj_errors = InteractionValidator.load_cnj_errors_from_excel("input/erros.xlsx")

# Criar cliente e validador
client = PloomesClient("seu_token_aqui")
validator = InteractionValidator(client, cnj_errors)

# Validar interactions
report = validator.validate_interactions_in_stage(110351653)

# Gerar relatório
validator.generate_report_excel(report, "output/report.xlsx")
```

## Mais Informações

Veja [VALIDATE_INTERACTIONS.md](../VALIDATE_INTERACTIONS.md) para documentação completa.
