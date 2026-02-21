# Referrals Data Pipeline

## Overview

This project consolidates referral-related call data from multiple source spreadsheets into a single target Google Sheet for reporting and analysis.

The pipeline reads source files, cleans and standardizes fields, and produces four output datasets:

- `Master`
- `Part`
- `Referral`
- `Fact`

## Repository Structure

- `Referrals.R`: main ETL script.
- `docs/requirements.md`: client-facing project requirements.

## Data Flow (High Level)

1. Authenticate with Google.
2. Read configured source tabs from Google Sheets/Excel files.
3. Detect headers and clean column/value formatting.
4. Build `Master` dataset and standardize fields.
5. Build `Part`, `Referral`, and `Fact` datasets.
6. Write outputs to the target Google Sheet tabs.

## Requirements

- R environment with CRAN access.
- Google Drive and Google Sheets authenticated.

## Run

From this folder:

```bash
Rscript Referrals.R
```

## Notes

- The script installs required R packages if missing.
