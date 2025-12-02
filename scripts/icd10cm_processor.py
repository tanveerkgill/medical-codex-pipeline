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
    resolve_default_icd10cm_url,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

# Sample local filename; supports download via ICD10CM_URL
RAW_FILE = INPUT_DIR / "icd10cm_codes_2024.txt"

# Typical ICD-10-CM format: Letter + 2 digits, optional '.' + up to 4 alphanumerics
ICD10CM_PATTERN = re.compile(r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$")


def find_column(candidates: List[str], columns: List[str]) -> Optional[str]:
    """Return the first matching column from a list of case-insensitive candidates."""
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def validate_icd10cm_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """Validate ICD-10-CM codes and locate description columns."""
    code_col = find_column(["Code", "code"], list(df.columns))
    if not code_col:
        raise ValueError("Missing required column: Code")

    # Prefer Long Description, else Description, else Short Description
    desc_col = find_column(
        ["Long Description", "Description", "Short Description", "desc", "Desc"],
        list(df.columns),
    )
    if not desc_col:
        raise ValueError("Missing required description column")

    df = df.copy()
    df[code_col] = df[code_col].astype(str).str.strip().str.upper()
    valid_mask = df[code_col].str.match(ICD10CM_PATTERN, na=False)

    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()
    return valid_rows, invalid_rows, code_col, desc_col


def clean_icd10cm_data(df: pd.DataFrame, code_col: str, desc_col: str) -> pd.DataFrame:
    """Standardize ICD-10-CM output columns and metadata."""
    df = df.rename(columns={code_col: "code", desc_col: "description"})
    df = basic_cleanup(df)
    df = df.dropna(subset=["code", "description"]).drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]


def main():
    """Entry point for ICD-10-CM processing pipeline."""
    setup_logging(LOG_DIR / "icd10cm.log")
    logging.info("=" * 60)
    logging.info("Starting ICD-10-CM processor")
    logging.info("=" * 60)
    default_url = resolve_default_icd10cm_url()
    raw_path = ensure_file(
        RAW_FILE,
        "ICD10CM_URL",
        timeout=45,
        retries=3,
        url_override=default_url,
    )
    # ICD-10-CM files are typically tab-delimited
    raw_df = pd.read_csv(raw_path, dtype=str, sep='\t', on_bad_lines="warn", low_memory=False)

    valid_df, invalid_df, code_col, desc_col = validate_icd10cm_data(raw_df)
    invalid_count = len(invalid_df)
    if invalid_count:
        logging.warning("ICD-10-CM: %d invalid rows detected", invalid_count)
        save_invalid_rows(invalid_df, ERROR_DIR / "icd10cm_invalid")
    else:
        logging.info("ICD-10-CM: no invalid rows detected")

    clean_df = clean_icd10cm_data(valid_df, code_col=code_col, desc_col=desc_col)
    output_base = OUTPUT_CSV_DIR / "icd10cm_clean"
    output_csv = output_base.with_suffix(".csv")
    logging.info("ICD-10-CM: saving %d clean rows to %s", len(clean_df), output_csv)
    save_to_formats(clean_df, output_base)
    print("✅ ICD-10-CM processing completed")


if __name__ == "__main__":
    main()
