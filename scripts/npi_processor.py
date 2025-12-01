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
    resolve_latest_npi_monthly_zip,
)

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_CSV_DIR = BASE_DIR / "output" / "csv"
ERROR_DIR = BASE_DIR / "output" / "errors"
LOG_DIR = BASE_DIR / "logs"

RAW_FILE = INPUT_DIR / "npi_registry.csv"

NPI_10_DIGITS = re.compile(r"^\d{10}$")

def luhn_check_digit(number_without_check: str) -> int:
    digits = [int(ch) for ch in number_without_check]
    total = 0
    parity = (len(digits) + 1) % 2
    for idx, d in enumerate(digits):
        if idx % 2 == parity:
            d = d * 2
            if d > 9:
                d -= 9
        total += d
    return (10 - (total % 10)) % 10


def is_valid_npi(npi_str: str) -> bool:
    if not isinstance(npi_str, str):
        return False
    digits = re.sub(r"\D", "", npi_str)
    if not NPI_10_DIGITS.match(digits):
        return False
    first9, check = digits[:9], int(digits[-1])
    base = "80840" + first9
    return luhn_check_digit(base) == check


def find_column(candidates: List[str], columns: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def build_description(df: pd.DataFrame) -> pd.Series:
    org_candidates = [
        "Provider Organization Name (Legal Business Name)",
        "Provider Organization Name",
        "Organization Name",
        "Org Name",
    ]
    last_candidates = ["Provider Last Name (Legal Name)", "Last Name", "Provider Last Name"]
    first_candidates = ["Provider First Name", "First Name"]
    middle_candidates = ["Provider Middle Name", "Middle Name"]
    credential_candidates = ["Provider Credential Text", "Credential", "Credentials"]

    org_col = find_column(org_candidates, list(df.columns))
    last_col = find_column(last_candidates, list(df.columns))
    first_col = find_column(first_candidates, list(df.columns))
    middle_col = find_column(middle_candidates, list(df.columns))
    cred_col = find_column(credential_candidates, list(df.columns))

    if org_col:
        desc = df[org_col].fillna("").astype(str).str.strip()
        return desc

    last = df[last_col].fillna("").astype(str).str.strip() if last_col else ""
    first = df[first_col].fillna("").astype(str).str.strip() if first_col else ""
    middle = df[middle_col].fillna("").astype(str).str.strip() if middle_col else ""
    cred = df[cred_col].fillna("").astype(str).str.strip() if cred_col else ""

    if isinstance(last, pd.Series) and isinstance(first, pd.Series):
        first_middle = first + (" " + middle).where(middle.ne(""), "") if isinstance(middle, pd.Series) else first
        base = last.where(last.eq(""), last + ", ") + first_middle
        composed = base + (" " + cred).where(cred.ne(""), "") if isinstance(cred, pd.Series) else base
        return composed.str.strip()

    return pd.Series([""] * len(df))

def validate_npi_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    npi_col = find_column(["NPI", "npi"], list(df.columns))
    if not npi_col:
        raise ValueError("Missing required column: NPI")

    df = df.copy()
    df[npi_col] = df[npi_col].astype(str).fillna("").str.replace(r"\D", "", regex=True)
    ten_digit_mask = df[npi_col].str.fullmatch(r"\d{10}", na=False)
    luhn_mask = df[npi_col].apply(is_valid_npi)
    valid_mask = ten_digit_mask & luhn_mask
    valid_rows = df[valid_mask].copy()
    invalid_rows = df[~valid_mask].copy()
    return valid_rows, invalid_rows, npi_col


def clean_npi_data(df: pd.DataFrame, npi_col: str) -> pd.DataFrame:
    df = df.copy()
    df["description"] = build_description(df)
    df = df.rename(columns={npi_col: "code"})
    df = basic_cleanup(df)
    df = df.dropna(subset=["code"])
    if "description" in df.columns:
        df["description"] = df["description"].replace("", pd.NA)
        df = df.dropna(subset=["description"])
    df = df.drop_duplicates(subset=["code"])
    df["last_updated"] = iso_utc_now()
    return df[["code", "description", "last_updated"]]

def main():
    setup_logging(LOG_DIR / "npi.log")
    logging.info("=" * 60)
    logging.info("Starting NPI processor")
    logging.info("=" * 60)
    raw_path = ensure_file(
        RAW_FILE,
        "NPI_URL",
        timeout=60,
        retries=3,
        prefer_regex=r"npidata_pfile_.*\.csv$",
        exclude_regex=r"(FileHeader|FileSchema)",
        url_override=resolve_latest_npi_monthly_zip(),
    )
    raw_df = pd.read_csv(raw_path, dtype=str, on_bad_lines="warn", low_memory=False)
    valid_df, invalid_df, npi_col = validate_npi_data(raw_df)
    if not invalid_df.empty:
        save_invalid_rows(invalid_df, ERROR_DIR / "npi_invalid")
    clean_df = clean_npi_data(valid_df, npi_col=npi_col)
    save_to_formats(clean_df, OUTPUT_CSV_DIR / "npi_clean")
    print("✅ NPI processing completed")


if __name__ == "__main__":
    main()