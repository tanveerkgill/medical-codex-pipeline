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
    resolve_default_snomed_url,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

# Default expected local file; can be overridden by SNOMED_URL
RAW_FILE = INPUT_DIR / "snomed_us_sample.csv"

# SNOMED CT conceptId is a numeric SCTID, typically 6–18 digits
SNOMED_SCTID_PATTERN = re.compile(r"^\d{6,18}$")


def find_column(candidates: List[str], columns: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def validate_snomed_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """
    Validate presence of code/description-like columns and basic SCTID format.
    """
    code_col = find_column(["conceptId", "Concept ID", "id", "code", "sctid"], list(df.columns))
    if not code_col:
        raise ValueError("Missing required column for SNOMED concept identifier (e.g., conceptId)")

    desc_col = find_column(
        [
            "FSN",
            "Fully Specified Name",
            "term",
            "Term",
            "Preferred Term",
            "PT",
            "Description",
        ],
        list(df.columns),
    )
    if not desc_col:
        raise ValueError("Missing required description-like column (FSN/term/Description)")

    df = df.copy()
    df[code_col] = df[code_col].astype(str).str.strip()
    valid_mask = df[code_col].str.match(SNOMED_SCTID_PATTERN, na=False)

    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()
    return valid_rows, invalid_rows, code_col, desc_col


def clean_snomed_data(df: pd.DataFrame, code_col: str, desc_col: str) -> pd.DataFrame:
    """
    Standardize to code, description, last_updated.
    """
    df = df.rename(columns={code_col: "code", desc_col: "description"})
    df = basic_cleanup(df)
    df = df.dropna(subset=["code", "description"]).drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]


def main():
    """
    Process SNOMED CT sample (or remote via SNOMED_URL) into standardized CSV.
    """
    setup_logging(LOG_DIR / "snomed.log")
    logging.info("=" * 60)
    logging.info("Starting SNOMED CT processor")
    logging.info("=" * 60)
    raw_path = ensure_file(
        RAW_FILE,
        "SNOMED_URL",
        timeout=45,
        retries=3,
        url_override=resolve_default_snomed_url(),
    )
    raw_df = pd.read_csv(raw_path, dtype=str, on_bad_lines="warn", low_memory=False)

    valid_df, invalid_df, code_col, desc_col = validate_snomed_data(raw_df)
    if not invalid_df.empty:
        save_invalid_rows(invalid_df, ERROR_DIR / "snomed_invalid")

    clean_df = clean_snomed_data(valid_df, code_col=code_col, desc_col=desc_col)
    save_to_formats(clean_df, OUTPUT_CSV_DIR / "snomed_clean")
    print("✅ SNOMED processing completed")


if __name__ == "__main__":
    main()

