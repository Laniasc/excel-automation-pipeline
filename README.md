# Excel Automation Pipeline

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
4. Executar: `python src/main.py` (quando implementado)

## Saídas
- `output/clean.csv`
- `output/clean.parquet`
- `output/report.xlsx` (quando implementado)
