from pathlib import Path
import os
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests
import re
import zipfile
from io import BytesIO
from datetime import datetime
from typing import Optional


def basic_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shared cleanup logic for all codexes:
    - Normalize and trim text fields
    """
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.strip().str.upper()

    if "description" in df.columns:
        df["description"] = df["description"].astype(str).str.strip().str.title()

    return df


def save_to_formats(df: pd.DataFrame, base_path: Path):
    """
    Save DataFrame to standardized CSV format.
    """
    base_path.parent.mkdir(parents=True, exist_ok=True)

    output_csv = base_path.with_suffix(".csv")
    df.to_csv(output_csv, index=False)
    print(f"✅ Clean file saved: {output_csv}")


def save_invalid_rows(df: pd.DataFrame, base_path: Path):
    """
    Save invalid rows for inspection.

    """
    if df.empty:
        return

    base_path.parent.mkdir(parents=True, exist_ok=True)

    output_csv = base_path.with_suffix(".csv")
    df.to_csv(output_csv, index=False)

    print(f"⚠️ Invalid rows saved: {output_csv}")


def setup_logging(log_file: Path):
    """
    Configure root logger.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def ensure_file(
    raw_path: Path,
    url_env_var: str,
    timeout: int = 60,
    retries: int = 3,
    prefer_regex: str | None = None,
    exclude_regex: str | None = None,
    url_override: str | None = None,
) -> Path:
    """
    Ensure file exists at raw_path. Tries online download first, falls back to local file if download fails.
    """
    # Ensure we're working with absolute path
    raw_path = Path(raw_path).resolve()
    
    # Determine URL source
    env_url = os.getenv(url_env_var, "")
    url = env_url or (url_override or "")
    
    # Log source type and path info
    logging.info(f"🔍 Checking file: {raw_path}")
    logging.info(f"   Absolute path: {raw_path}")
    logging.info(f"   File exists: {raw_path.exists()}")
    
    if url_override and not env_url:
        source_type = "HARDCODED URL"
    elif env_url:
        source_type = f"ENV VAR ({url_env_var})"
    elif url_override is None:
        source_type = "URL RESOLUTION FAILED"
    else:
        source_type = "UNKNOWN"
    
    last_error = None
    
    # Try online download first if URL is available
    if url:
        logging.info(f"🌐 Attempting online download first: {raw_path}")
        logging.info(f"   Source: {source_type}")
        logging.info(f"   URL: {url}")
        
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(1, retries + 1):
            try:
                logging.info(f"   Download attempt {attempt}/{retries}...")
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                if not resp.content:
                    raise ValueError("Downloaded file is empty")

                content_type = resp.headers.get("Content-Type", "").lower()
                url_lower = url.lower()
                is_zip = url_lower.endswith(".zip") or "zip" in content_type

                if is_zip:
                    with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                        members = [zi for zi in zf.infolist() if not zi.is_dir()]
                        if not members:
                            raise ValueError("Zip file contains no files")

                        def is_textual(name: str) -> bool:
                            return name.lower().endswith((".csv", ".txt", ".tsv"))

                        candidates = [zi for zi in members if is_textual(zi.filename)]

                        if prefer_regex:
                            pref_re = re.compile(prefer_regex)
                            preferred = [zi for zi in candidates if pref_re.search(zi.filename)]
                            if preferred:
                                candidates = preferred

                        if exclude_regex:
                            excl_re = re.compile(exclude_regex)
                            candidates = [zi for zi in candidates if not excl_re.search(zi.filename)]

                        if not candidates:
                            # Fallback to any file if no textual candidates survived
                            candidates = members

                        # Choose the largest remaining candidate (heuristic for main dataset)
                        chosen = max(candidates, key=lambda zi: zi.file_size)

                        suffix = Path(chosen.filename).suffix or raw_path.suffix
                        target_path = raw_path.with_suffix(suffix)
                        with zf.open(chosen) as src, open(target_path, "wb") as dst:
                            dst.write(src.read())
                        logging.info(f"✅ ONLINE DOWNLOAD SUCCESSFUL")
                        logging.info(f"   Extracted {chosen.filename} ({chosen.file_size:,} bytes) to {target_path}")
                        return target_path
                else:
                    # Direct file download
                    target_path = raw_path
                    # If server suggests a filename with different suffix, honor it for better downstream handling
                    cd = resp.headers.get("Content-Disposition", "")
                    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
                    if m:
                        suggested = Path(m.group(1)).suffix
                        if suggested and suggested != target_path.suffix:
                            target_path = raw_path.with_suffix(suggested)
                    file_size = len(resp.content)
                    target_path.write_bytes(resp.content)
                    logging.info(f"✅ ONLINE DOWNLOAD SUCCESSFUL")
                    logging.info(f"   Downloaded {file_size:,} bytes to {target_path}")
                    return target_path
            except Exception as e:
                last_error = e
                logging.warning(f"   Download attempt {attempt} failed: {e}")
        
        logging.warning("❌ ONLINE DOWNLOAD FAILED - All download attempts exhausted")
        logging.warning(f"   Last error: {last_error}")
        logging.info("   Falling back to local file check...")
    else:
        # No URL available, check local file first
        if url_override is None:
            logging.info(f"📁 URL resolution failed (could not find valid URL), checking for local file: {raw_path}")
        else:
            logging.info(f"📁 No URL available (no {url_env_var} env var), checking for local file: {raw_path}")
    
    # Fallback to local file (always check, whether download failed or no URL was available)
    logging.info(f"🔍 Final check - File exists: {raw_path.exists()}")
    if raw_path.exists():
        logging.info(f"📁 Using local file: {raw_path}")
        logging.info("   Source: LOCAL FILE (online download failed or unavailable)")
        return raw_path
    
    # No URL and no local file - raise error with helpful debugging info
    error_msg = f"Input not found at {raw_path}"
    error_msg += f"\n   Absolute path checked: {raw_path}"
    error_msg += f"\n   Parent directory exists: {raw_path.parent.exists()}"
    if raw_path.parent.exists():
        error_msg += f"\n   Files in parent directory: {list(raw_path.parent.iterdir())[:10]}"
    
    if not url:
        if url_override is None:
            error_msg += f"\n   URL resolution failed (no {url_env_var} env var and url_override is None)"
        else:
            error_msg += f"\n   {url_env_var} is not set"
        raise FileNotFoundError(error_msg)
    else:
        error_msg += f"\n   Online download failed: {last_error}"
        raise FileNotFoundError(error_msg)

 


def resolve_default_icd10cm_url(year: Optional[int] = None, timeout: int = 8) -> Optional[str]:
    """
    Return hardcoded ICD-10-CM URL. Tries current year first, falls back to previous year.
    """
    y = year or datetime.utcnow().year
    # Always return a URL - let the download attempt handle errors
    # Try current year first, fallback to previous year
    for candidate_year in (y, y - 1):
        url = f"https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/{candidate_year}/icd10cm_codes_{candidate_year}.txt"
        # Quick HEAD check to prefer working URL, but always return something
        try:
            r = requests.head(url, timeout=timeout)
            if r.ok:
                return url
        except Exception:
            continue
    # Return current year URL even if HEAD check failed (download will handle errors)
    return f"https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/{y}/icd10cm_codes_{y}.txt"


def resolve_default_hcpcs_url(year: Optional[int] = None, timeout: int = 8) -> Optional[str]:
    """
    Return hardcoded HCPCS URL. Tries current year first, falls back to previous year.
    """
    y = year or datetime.utcnow().year
    # Always return a URL - let the download attempt handle errors
    for candidate_year in (y, y - 1):
        url = f"https://www.cms.gov/files/zip/{candidate_year}-alpha-numeric-hcpcs-file.zip"
        # Quick HEAD check to prefer working URL, but always return something
        try:
            r = requests.head(url, timeout=timeout)
            if r.ok:
                return url
        except Exception:
            continue
    # Return current year URL even if HEAD check failed (download will handle errors)
    return f"https://www.cms.gov/files/zip/{y}-alpha-numeric-hcpcs-file.zip"


def resolve_latest_npi_monthly_zip(timeout: int = 12) -> Optional[str]:
    """
    Parse the NPPES listing page and return the latest npidata_pfile_* CSV ZIP URL.
    """
    index_url = "https://download.cms.gov/nppes/NPI_Files.html"
    try:
        resp = requests.get(index_url, timeout=timeout)
        resp.raise_for_status()
    except Exception:
        return None

    html = resp.text
    # Find absolute links to npidata_pfile_* CSV zips
    # Example: https://download.cms.gov/nppes/NPI_Files/Monthly/2025-11-01/npidata_pfile_20251101-CSV.zip
    pattern = re.compile(r"https://download\.cms\.gov/nppes/NPI_Files/(?:Monthly|Weekly)/[^\"']*/npidata_pfile_(\d{8})-CSV\.zip", re.IGNORECASE)
    candidates: list[tuple[int, str]] = []
    for m in pattern.finditer(html):
        date_int = int(m.group(1))
        url = m.group(0)
        candidates.append((date_int, url))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def resolve_default_icd10who_url() -> Optional[str]:
    """
    Default ICD-10 WHO URL resolver. Hardcoded URL - may require manual download.
    """
    # Common public ICD-10 WHO CSV sources (update year as needed)
    year = datetime.utcnow().year
    # Try common patterns - these may need to be updated based on actual source
    candidates = [
        f"https://icd.who.int/browse10/{year}/en/Download/GetICD10CSV",
        f"https://icd.who.int/browse10/{year-1}/en/Download/GetICD10CSV",
    ]
    for url in candidates:
        try:
            r = requests.head(url, timeout=5, allow_redirects=True)
            if r.ok:
                return url
        except Exception:
            continue
    # Fallback: return a common pattern (may require manual download/registration)
    return f"https://icd.who.int/browse10/{year}/en/Download/GetICD10CSV"


def resolve_default_loinc_url() -> Optional[str]:
    """
    Default LOINC URL resolver. Hardcoded URL - may require registration/login.
    """
    # LOINC download URL pattern (may require registration)
    # Common pattern: https://loinc.org/downloads/loinc-table/
    # Direct download typically requires login, but attempt common URL
    return "https://loinc.org/downloads/loinc-table/loinc-table.zip"


def resolve_default_rxnorm_url() -> Optional[str]:
    """
    Default RxNorm URL resolver. Hardcoded URL - requires UMLS credentials.
    """
    # RxNorm full monthly release (requires UMLS license)
    # Pattern: https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip
    return "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"


def resolve_default_snomed_url() -> Optional[str]:
    """
    Default SNOMED CT URL resolver. Hardcoded URL - requires UMLS license.
    """
    # SNOMED CT US Edition download (requires UMLS license)
    # Common pattern - actual URL may vary by release
    return "https://download.nlm.nih.gov/umls/kss/snomedct_us/SnomedCT_US.zip"


def iso_utc_now() -> str:
    """
    ISO 8601 UTC timestamp (seconds precision).
    """
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
