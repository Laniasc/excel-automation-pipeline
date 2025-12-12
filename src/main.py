from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
from loguru import logger


DEFAULT_INPUT = Path("data/input.xlsx")
DEFAULT_SHEET = "Lançamentos"
DEFAULT_HEADER_ROW = 1  # no seu arquivo, o cabeçalho está na linha 2 do Excel


REQUIRED_COLS = ["data", "tipo", "categoria", "descricao"]
DATE_COLS = ["data"]
NON_NEGATIVE_NUM_COLS = ["receita", "despesa"]
UNIQUE_KEY_COLS: List[str] = []


@dataclass
class QualityIssue:
    rule_id: str
    severity: str  # ERROR | WARN
    row_index: int
    column: Optional[str]
    message: str


def normalize_text(s: str) -> str:
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def slugify_col(name: str) -> str:
    name = normalize_text(str(name)).lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [slugify_col(c) for c in df.columns]
    return df


def drop_unnamed_and_empty_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df = df.dropna(axis=1, how="all")
    return df


def validate_required(df: pd.DataFrame, issues: List[QualityIssue]) -> None:
    for col in REQUIRED_COLS:
        if col not in df.columns:
            issues.append(QualityIssue("MISSING_REQUIRED_COLUMN", "ERROR", -1, col, f"Coluna obrigatória ausente: {col}"))
            continue

        missing = df[col].isna() | (df[col].astype(str).str.strip() == "")
        for idx in df.index[missing].tolist():
            issues.append(QualityIssue("REQUIRED_VALUE_MISSING", "ERROR", int(idx), col, f"Valor obrigatório ausente em {col}"))


def coerce_dates(df: pd.DataFrame, issues: List[QualityIssue]) -> pd.DataFrame:
    df = df.copy()
    for col in DATE_COLS:
        if col not in df.columns:
            continue

        parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        had_value = df[col].notna() & (df[col].astype(str).str.strip() != "")
        became_nat = parsed.isna() & had_value

        for idx in df.index[became_nat].tolist():
            issues.append(QualityIssue("DATE_PARSE_FAILED", "WARN", int(idx), col, f"Data inválida em {col}"))

        df[col] = parsed
    return df


def coerce_non_negative_numbers(df: pd.DataFrame, issues: List[QualityIssue]) -> pd.DataFrame:
    df = df.copy()
    for col in NON_NEGATIVE_NUM_COLS:
        if col not in df.columns:
            continue

        num = pd.to_numeric(df[col], errors="coerce")
        df[col] = num

        neg = num.notna() & (num < 0)
        for idx in df.index[neg].tolist():
            issues.append(QualityIssue("NEGATIVE_VALUE", "ERROR", int(idx), col, f"Valor negativo em {col}"))

    return df


def build_quality_report(issues: List[QualityIssue]) -> pd.DataFrame:
    cols = ["rule_id", "severity", "row_index", "column", "message"]
    rows = [
        {
            "rule_id": i.rule_id,
            "severity": i.severity,
            "row_index": i.row_index,
            "column": i.column,
            "message": i.message,
        }
        for i in issues
    ]
    return pd.DataFrame(rows, columns=cols)


def write_outputs(df_clean: pd.DataFrame, report: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    df_clean.to_csv(out_dir / "clean.csv", index=False, encoding="utf-8")
    df_clean.to_parquet(out_dir / "clean.parquet", index=False)
    report.to_csv(out_dir / "quality_report.csv", index=False, encoding="utf-8")

    con = duckdb.connect(database=":memory:")
    con.register("quality_report", report)

    summary = con.execute(
        """
        select severity, rule_id, count(*) as qtd
        from quality_report
        group by 1,2
        order by severity desc, qtd desc
        """
    ).df()
    summary.to_csv(out_dir / "quality_summary.csv", index=False, encoding="utf-8")
    con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT))
    parser.add_argument("--sheet", type=str, default=DEFAULT_SHEET)
    parser.add_argument("--header-row", type=int, default=DEFAULT_HEADER_ROW)
    parser.add_argument("--out", type=str, default="output")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path.resolve()}")

    logger.info(f"Lendo: {input_path} | aba={args.sheet} | header_row={args.header_row}")
    df = pd.read_excel(input_path, sheet_name=args.sheet, header=args.header_row, engine="openpyxl")

    df = drop_unnamed_and_empty_cols(df)
    df = standardize_columns(df)

    logger.info(f"Colunas após padronização: {list(df.columns)}")
    issues: List[QualityIssue] = []

    validate_required(df, issues)
    df = coerce_dates(df, issues)
    df = coerce_non_negative_numbers(df, issues)

    report = build_quality_report(issues)

    error_rows = set(report.loc[report["severity"] == "ERROR", "row_index"].tolist()) if not report.empty else set()
    error_rows = {r for r in error_rows if r >= 0}

    df_clean = df.drop(index=list(error_rows)).copy()

    logger.info(f"Erros: {(report['severity'] == 'ERROR').sum() if not report.empty else 0}")
    logger.info(f"Avisos: {(report['severity'] == 'WARN').sum() if not report.empty else 0}")
    logger.info(f"Linhas finais: {len(df_clean)}")

    write_outputs(df_clean, report, Path(args.out))
    logger.info("Saídas geradas em output/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

