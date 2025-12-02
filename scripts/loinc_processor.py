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
    resolve_default_loinc_url,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

RAW_FILE = INPUT_DIR / "loinc_sample.csv"

# LOINC format: digits hyphen digit (e.g., 718-7, 4548-4). Keep it simple and strict.
LOINC_PATTERN = re.compile(r"^[0-9]{1,5}-[0-9]$")


def find_column(candidates: List[str], columns: List[str]) -> Optional[str]:
    """Return the first column matching candidate names (case-insensitive)."""
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def validate_loinc_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """Validate LOINC codes and locate description column."""
    code_col = find_column(["LOINC_NUM", "LoincNum", "code"], list(df.columns))
    if not code_col:
        raise ValueError("Missing required column: LOINC_NUM")

    desc_col = find_column(
        ["LONG_COMMON_NAME", "ShortName", "Component", "Description", "LONG_COMMON_NAME_TEXT"],
        list(df.columns),
    )
    if not desc_col:
        raise ValueError("Missing required description column")

    df = df.copy()
    df[code_col] = df[code_col].astype(str).str.strip().str.upper()
    valid_mask = df[code_col].str.match(LOINC_PATTERN, na=False)

    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()
    return valid_rows, invalid_rows, code_col, desc_col


def clean_loinc_data(df: pd.DataFrame, code_col: str, desc_col: str) -> pd.DataFrame:
    """Standardize LOINC output columns and metadata."""
    df = df.rename(columns={code_col: "code", desc_col: "description"})
    df = basic_cleanup(df)
    df = df.dropna(subset=["code", "description"]).drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]


def main():
    """Process LOINC data into standardized CSV output."""
    setup_logging(LOG_DIR / "loinc.log")
    logging.info("=" * 60)
    logging.info("Starting LOINC processor")
    logging.info("=" * 60)
    raw_path = ensure_file(
        RAW_FILE,
        "LOINC_URL",
        timeout=45,
        retries=3,
        url_override=resolve_default_loinc_url(),
    )
    raw_df = pd.read_csv(raw_path, dtype=str, on_bad_lines="warn", low_memory=False)

    valid_df, invalid_df, code_col, desc_col = validate_loinc_data(raw_df)
    invalid_count = len(invalid_df)
    if invalid_count:
        logging.warning("LOINC: %d invalid rows detected", invalid_count)
        save_invalid_rows(invalid_df, ERROR_DIR / "loinc_invalid")
    else:
        logging.info("LOINC: no invalid rows detected")

    clean_df = clean_loinc_data(valid_df, code_col=code_col, desc_col=desc_col)
    output_base = OUTPUT_CSV_DIR / "loinc_clean"
    output_csv = output_base.with_suffix(".csv")
    logging.info("LOINC: saving %d clean rows to %s", len(clean_df), output_csv)
    save_to_formats(clean_df, output_base)
    print("✅ LOINC processing completed")


if __name__ == "__main__":
    main()
