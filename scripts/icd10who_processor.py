import re
from pathlib import Path
from typing import Tuple
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
    resolve_default_icd10who_url,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

RAW_FILE = INPUT_DIR / "icd10who_codes_2024.csv"

ICD10_PATTERN = re.compile(r"^[A-Z][0-9][0-9](\.[0-9A-Z]{1,4})?$")


def validate_icd10_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate ICD-10 WHO records for required columns and code format."""
    required_columns = ["Code", "Description"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df["Code"] = df["Code"].astype(str).str.strip().str.upper()
    valid_mask = df["Code"].str.match(ICD10_PATTERN, na=False)

    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()

    return valid_rows, invalid_rows


def clean_icd10_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize ICD-10 WHO output columns and metadata."""
    df = df.rename(
        columns={
            "Code": "code",
            "Description": "description",
        }
    )
    df = basic_cleanup(df)
    df = df.dropna(subset=["code", "description"])
    df = df.drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]


def main():
    """Entry point for ICD-10 WHO processing pipeline."""
    setup_logging(LOG_DIR / "icd10who.log")
    logging.info("=" * 60)
    logging.info("Starting ICD-10 WHO processor")
    logging.info("=" * 60)
    raw_path = ensure_file(
        RAW_FILE,
        "ICD10WHO_URL",
        timeout=30,
        retries=3,
        url_override=resolve_default_icd10who_url(),
    )
    raw_df = pd.read_csv(raw_path, dtype=str, on_bad_lines="skip", quotechar='"', escapechar='\\')
    valid_df, invalid_df = validate_icd10_data(raw_df)
    invalid_count = len(invalid_df)
    if invalid_count:
        logging.warning("ICD-10 WHO: %d invalid rows detected", invalid_count)
        save_invalid_rows(invalid_df, ERROR_DIR / "icd10who_invalid")
    else:
        logging.info("ICD-10 WHO: no invalid rows detected")
    clean_df = clean_icd10_data(valid_df)
    output_base = OUTPUT_CSV_DIR / "icd10who_clean"
    output_csv = output_base.with_suffix(".csv")
    logging.info("ICD-10 WHO: saving %d clean rows to %s", len(clean_df), output_csv)
    save_to_formats(clean_df, output_base)
    print("✅ ICD-10 WHO processing completed")


if __name__ == "__main__":
    main()
