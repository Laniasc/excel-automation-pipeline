# Excel Automation Pipeline
“Atualizado em 12/12/2025”
Automação de planilhas com Python: limpeza, validações de qualidade e integração de dados.

## Objetivo
- Ler planilhas Excel
- Padronizar colunas e tipos
- Validar dados e registrar erros
- Gerar saídas em CSV/Parquet e análises via SQL (DuckDB)

## Como rodar
1. Criar e ativar venv
2. Instalar dependências: `pip install -r requirements.txt`
3. Colocar arquivo em `data/` (não versionado)
4. Executar:

```powershell
py -3.13 .\src\main.py --input .\data\input.xlsx --sheet "Lançamentos" --header-row 1
```



## Saídas
- `output/clean.csv`
- `output/clean.parquet`
- `output/quality_report.csv`
- `output/quality_summary.csv`

## Validações de qualidade (dataset de teste)
Regras aplicadas no dataset `clean.csv`:

- `receita` e `despesa` são mutuamente exclusivas (nunca preenchidas ao mesmo tempo).
- Não existem registros com `receita` e `despesa` ambas vazias.
- Consistência por tipo:
  - `tipo=Receita` → `receita` preenchida e `despesa` vazia
  - `tipo=Despesa` → `despesa` preenchida e `receita` vazia

Evidência (execução local):
- receita_e_despesa_preenchidas: 0
- ambas_vazias: 0
- tipo=Receita com receita vazia: 0
- tipo=Despesa com despesa vazia: 0
