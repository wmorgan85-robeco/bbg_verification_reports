# Bloomberg Data License - Billing Verification

Monthly verification of the Bloomberg Data License scheduled-data invoice for Robeco's two scheduled accounts:

| Account | ID |
| --- | --- |
| Main | `301491` |
| PORT | `791626` |

The output is a report notebook that reconciles the invoice, explains month-over-month changes and attributes billed securities to the jobs that request them. It is sent to the team that receives the invoice.

## Contents

| Notebook | Purpose |
| --- | --- |
| `report_core` | Shared core: imports, chart helpers, data loading (account filter, ENV enrichment), report parameters, completeness check, core metrics. Run by `analysis_v1` and investigation notebooks via `%run` |
| `analysis_v1` | Monthly report: executive summary, reconciliation, MoM change, job attribution, usage trends, carried-over age |
| `ingest_files_v1` | Loads Bloomberg verification report files (`dlverreport_<account>_<yyyymm>.tar.gz`) into the verification tables |
| `utils` | Shared enums and mappings (`Billable_Products`, `process_map`); `%run` by `report_core` |
| `investigations/` | One-off deep dives, named `YYYY-MM-DD_topic` or `YYYY-MM_topic`. Each does `%run "../report_core"` and has a header cell stating date, purpose and version |

## Monthly process

Ingest is manual — files are downloaded as one bulk export from service.bloomberg.com mid-month. There is no schedule.

1. Download the month's verification report files from Bloomberg and drop them in the incoming folder (see *Storage*).
2. Run `ingest_files_v1` top to bottom. The batch loop processes every file in `incoming`:
   * files for accounts outside `KNOWN_ACCOUNTS` are moved to `excluded`
   * files already processed (same name **and** checksum in the process log) are skipped
   * on success the file is moved to `archive`; on failure it stays in `incoming` for retry
3. Check the monitoring cells at the end of the ingest notebook: no files in-flight, no failed files.
4. Run `analysis_v1` top to bottom. The report month is the latest `REPORT_YR_MONTH` in the summary table; nothing needs editing.
5. Review the **Executive Summary** flags and fill in the **Analyst notes** cell.

## Data

Tables are created and accessed through the internal `tables` API (`%run include/env`).

| Table | Grain | Source file |
| --- | --- | --- |
| `bbg.verification_sched_summary` | account, month, product, security | scheduled summary (invoice basis) |
| `bbg.verification_sched_detail` | account, request (`OUTPUT_ID`), security, timestamp | scheduled securities detail |
| `bbg.verification_file_process_log` | file, process date | ingest log (`PROCESSING` / `COMPLETED` / `FAILED` / `SKIPPED` / `OVERRIDE`) |

Key columns:

* `ACCOUNT_ID` is a **string** in both tables.
* `REPORT_YR_MONTH` is `yyyy/MM` in the summary. The detail has no billing month; `analysis_v1` derives it from the calendar month of `PROCESSED_TIME`, which aligns with the billing month (verified: zero mismatches since 2023/01).
* `CTRB_BILLING = "Y"` marks billed requests in the detail.
* `SERVICE`: `F` = FTP, `H` = Web Services, `P` = named jobs (mainly PORT).
* `OUTPUT_ID`: per-file for FTP (`B02_<date>_<time>.ENC`, prefix maps to a process via `process_map`), per-request for Web Services (`<n>-<n>.OUT`), job name for `P` (e.g. `BBP_Analytics_PROD`).

### Storage

Azure Data Lake `rbqrrhippoprd` / container `datascientist`, under `users/mw/bbg_verification_reports/`:

* `incoming/` - files waiting to be loaded
* `archive/` - successfully loaded files
* `excluded/` - files for accounts not in scope

## Report (`analysis_v1`) definitions

* **Billed unique**: distinct (`BILLABLE_PRODUCT`, `ID_BB_UNIQUE`) pairs in the summary for the month. Matches the invoice; `sum(GROUP_AC)` matches billed access.
* **Carried over / not requested**: billed in the summary but with no billed request in the detail that month (securities still billed from earlier downloads).
* **Reconciliation MATCH**: every active summary security appears in the detail, and every billed detail security appears in the summary.
* **Job**: FTP files grouped by process (e.g. `FTP B02 Security`); non-Bxx FTP files (History*, HDUMOS*, BDL_*) grouped as `FTP Ad-hoc requests`; all Web Services requests in one bucket; `P` jobs by `OUTPUT_ID`.
* **Exclusive**: billed pairs requested by only one job that month, i.e. volume that would not be billed that month without the job.
* **ENV**: suffix of named `OUTPUT_ID`s (`PROD`, `UAT`, `ACC`, `TST`, `TEST`, `DEV`); `N/A` for FTP files and Web Services.

### Flags

| Rule | Condition |
| --- | --- |
| MoM change | billed unique for a product moved by >= 10% **and** >= 50 vs prior month |
| MoM access change | same rule on `GROUP_AC` |
| New / discontinued product | product appears or disappears vs prior month |
| Non-prod billed | billed requests from a job with a non-production `ENV` |
| Reconciliation | summary vs detail check fails |
| Incomplete data | an account is missing the summary or detail file for the report month |

Thresholds are `MOM_PCT_THRESHOLD` and `MOM_ABS_THRESHOLD` in the *Report Parameters* cell.

## Known caveats

* `STATUS` (`ACTIVE` / `INACTIVE`) comes from `TOTAL_USAGE > 0`, which is 0 for some PORT securities that were requested. Use *carried over* (detail-based) rather than `STATUS` for attribution.
* Account `35922` (GLAC-EGF backfill, Jan–Feb 2024, zero billed usage) is present in the raw tables but excluded at load time in `report_core`.
* One-off investigations live in `investigations/` and are not part of the monthly report.
* No cost figures are available yet, so the report works in counts (unique securities, access), not currency.
