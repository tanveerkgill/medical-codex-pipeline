import re
from pathlib import Path
from typing import Tuple, Optional, List
import sys

# Ensure project root is on sys.path when running this file directly
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import logging

from utils.common_functions import (
    setup_logging,
    ensure_file,
    iso_utc_now,
    save_to_formats,
    basic_cleanup,
    save_invalid_rows,
    resolve_default_hcpcs_url,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

# Example: accept either CSV or TXT placed here
RAW_FILE = INPUT_DIR / "hcpcs_codes_2024.csv"

# HCPCS Level II: one letter (A–V) + 4 digits (e.g., A0428, G0008, J3490)
HCPCS_PATTERN = re.compile(r"^[A-V]\d{4}$")


def find_column(candidates: List[str], columns: List[str]) -> Optional[str]:
    """Return the first column matching candidate names (case-insensitive)."""
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def validate_hcpcs_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """Validate HCPCS codes and locate description column."""
    code_col = find_column(["HCPCS", "Code", "code"], list(df.columns))
    if not code_col:
        raise ValueError("Missing required column: HCPCS/Code")

    desc_col = find_column(
        ["Long Description", "Description", "Short Description", "desc", "Desc"],
        list(df.columns),
    )
    if not desc_col:
        raise ValueError("Missing required description column")

    df = df.copy()
    df[code_col] = df[code_col].astype(str).str.strip().str.upper()
    valid_mask = df[code_col].str.match(HCPCS_PATTERN, na=False)

    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()
    return valid_rows, invalid_rows, code_col, desc_col


def clean_hcpcs_data(df: pd.DataFrame, code_col: str, desc_col: str) -> pd.DataFrame:
    """Standardize HCPCS output columns and metadata."""
    df = df.rename(columns={code_col: "code", desc_col: "description"})
    df = basic_cleanup(df)
    df = df.dropna(subset=["code", "description"]).drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]


def main():
    """Entry point for HCPCS processing pipeline."""
    setup_logging(LOG_DIR / "hcpcs.log")
    logging.info("=" * 60)
    logging.info("Starting HCPCS processor")
    logging.info("=" * 60)
    # Allow override via HCPCS_URL; supports .csv or .txt
    raw_path = ensure_file(
        RAW_FILE,
        "HCPCS_URL",
        timeout=45,
        retries=3,
        prefer_regex=r".*\.csv$",
        url_override=resolve_default_hcpcs_url(),
    )
    sep = "\t" if raw_path.suffix.lower() == ".txt" else ","
    raw_df = pd.read_csv(raw_path, dtype=str, sep=sep, on_bad_lines="warn", low_memory=False)

    valid_df, invalid_df, code_col, desc_col = validate_hcpcs_data(raw_df)
    invalid_count = len(invalid_df)
    if invalid_count:
        logging.warning("HCPCS: %d invalid rows detected", invalid_count)
        save_invalid_rows(invalid_df, ERROR_DIR / "hcpcs_invalid")
    else:
        logging.info("HCPCS: no invalid rows detected")

    clean_df = clean_hcpcs_data(valid_df, code_col=code_col, desc_col=desc_col)
    output_base = OUTPUT_CSV_DIR / "hcpcs_clean"
    output_csv = output_base.with_suffix(".csv")
    logging.info("HCPCS: saving %d clean rows to %s", len(clean_df), output_csv)
    save_to_formats(clean_df, output_base)
    print("✅ HCPCS processing completed")


if __name__ == "__main__":
    main()
