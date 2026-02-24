# Referral Project Requirements

## 1. Project purpose

This project must consolidate call and referral information coming from multiple sources into a single Google Sheets document with clear and consistent tables.

The final result should allow any team member to:

- review recorded encounters,
- view participant data,
- view each referral separately,
- and analyze data in a final fact-style table (`Fact`).

## 2. Expected client-facing outcome

When the process runs, it must update one target Google Sheet with 4 main tabs:

- `Master`
- `Part`
- `Referral`
- `Fact`

In addition, it must read a categories tab (`Categ_Ref`) to classify referrals.

## 3. Input data sources

The process must automatically read multiple source files (Google Sheets/Excel), using the configured tab for each source, and merge them into a single workflow.

`Categ_Ref` is the official referral-category dictionary and must be maintained in long format with exactly these columns:

- `Referral`
- `Category`

## 4. Functional requirements

### 4.1 Initial read and preparation

- Read each configured source.
- Detect the correct header row in each dataset.
- Clean text and column names (spaces, invisible characters, HTML tags, name variants).
- Align columns that are equal or semantically equivalent.
- Build one consolidated base dataset.

### 4.2 Build `Master`

The `Master` table must:

- merge all sources into one dataset,
- standardize key fields (for example: city, county, postal code, language, contact method, gender, age),
- normalize missing values,
- keep the original `ReferralsMade` column,
- add a `RefID` column next to `ReferralsMade`, using the referral IDs generated in `Referral`,
- create additional columns for each unique value found in `ReferralsMade`, marking `X` when applicable,
- add an incremental encounter identifier named `EnID`.

### 4.3 Date and time handling

`CallDateAndTimeStart` and `CallDateAndTimeEnd` must be stored in the destination as real datetime values (not plain text and not unix timestamp values).

### 4.4 Build `Part` (participants)

Create a participant dataset from `Master` with demographic and location fields, including:

- incremental `PartID`,
- relationship to the encounter through `EnID`.

### 4.5 Build `Referral`

Create a dataset where each referral is represented in its own row.

Requirements:

- final columns: `EnID`, `RefID`, `Referral`, `Category`,
- skip rows with no referral,
- map category using the dictionary loaded from `Categ_Ref` (long format),
- use normalized matching for referral keys (trimmed/lowercase/normalized spaces) to reduce mapping misses,
- avoid invalid category placeholders caused by parsing issues (for example, `Category = "Referral"` for all rows).

### 4.6 Build `Fact`

Create an analysis-ready fact dataset with columns:

- `CallStartDate`
- `CallEndDate`
- `EnID`
- `Narrative`
- `ContactType`
- `PartID`
- `RefID`

Important rule:

- each referral must be represented in its own row.

## 5. Data quality rules

- Do not keep duplicate columns in `Master`.
- Do not duplicate derived columns created from `ReferralsMade`.
- Standardize equivalent values to support reporting.
- Maintain traceability between datasets using IDs (`EnID`, `PartID`, `RefID`).

## 6. Deliverables

The process must produce:

1. Updated target Google Sheet (`Master`, `Part`, `Referral`, `Fact`).
2. Consistent data structure for analysis.
3. Clear requirements documentation for team reference (`docs/requirements.md`).

## 7. Latest requirements added

The following requirements were added after initial implementation and are now part of the expected behavior:

### 02/23/2026

1. `Master` must include `RefID` next to `ReferralsMade`.
2. `Fact` must include `CallStartDate` and `CallEndDate` as datetime fields; `Date` is no longer required.
3. `Categ_Ref` must be treated as a long-format dictionary (`Referral`, `Category`).
4. Category mapping in `Referral` must use robust matching compatible with the current `Categ_Ref` format.
