# Medical Codex Pipeline

Production-style ETL pipelines for processing and standardizing major medical codex datasets used in healthcare systems.

## ✅ Currently Implemented Codexes

- ICD-10 (WHO) – International disease classification codes
- ICD-10-CM (US) – Diagnosis codes
- NPI (US) – National Provider Identifier registry
- HCPCS (US) – Healthcare procedure codes
- LOINC (US) – Logical Observation Identifiers, Names, and Codes
- RxNorm (US) – RxNorm Normative Pharmacologic Classification
- SNOMED CT (US) – Systematized Nomenclature of Medicine, Clinical Terms

## 📁 Project Structure

```
medical-codex-pipeline/

├── input/              # Raw data files
├── scripts/            # ETL processing scripts
│   ├── icd10who_processor.py
│   ├── icd10cm_processor.py
│   ├── hcpcs_processor.py
│   ├── npi_processor.py
│   ├── loinc_processor.py
│   ├── rxnorm_processor.py
│   ├── snomed_processor.py
├── output/
│   └── csv/            # Clean standardized CSV outputs
├── utils/              # Shared utility functions
├── requirements.txt
└── README.md
```

## ⚙️ Setuppyth

```bash
cd medical-codex-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 🌐 Quick demo 

```bash
python scripts/icd10cm_processor.py
python scripts/hcpcs_processor.py
python scripts/npi_processor.py
python scripts/icd10who_processor.py
python scripts/loinc_processor.py
python scripts/rxnorm_processor.py
python scripts/snomed_processor.py
```

## ▶️ Run: ICD-10 (WHO) example

Place the ICD-10 file at:

```
input/icd10who_codes_2024.csv
```

Required columns:
- Code
- Description

Run:

```bash
python scripts/icd10who_processor.py
```

Output:
```
output/csv/icd10who_clean.csv
```

Download via env var
```bash
export HCPCS_URL="https://www.cms.gov/files/zip/2024-alpha-numeric-hcpcs-file.zip"
python scripts/hcpcs_processor.py
```

## 📦 Standardized Output Schema

All codex outputs use:
- code
- description
- last_updated

Example:
```
code,description,last_updated
A00,Cholera,2025-01-01 12:00:00
```

## 🧠 Tech Stack

- Python 3.9+
- pandas
- requests
- logging
- pathlib

## 🔗 Data Sources (for testing/downloading)

- ICD-10-CM: `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/` (codes_{year}.txt)
- ICD-10 (WHO): various conversions exist; supply CSV with Code/Description
- HCPCS: `https://www.cms.gov/medicare/coding/medhcpcsgeninfo` (Alpha-Numeric ZIPs)
- NPI: `https://download.cms.gov/nppes/NPI_Files.html` (Monthly/Weekly CSV ZIPs)
- LOINC: `https://loinc.org/downloads/loinc/` (registration required)
- RxNorm: `https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html` (UMLS credentials often required) or use RxNav APIs
- SNOMED CT US: `https://www.nlm.nih.gov/healthit/snomedct/us_edition.html` (UMLS license)