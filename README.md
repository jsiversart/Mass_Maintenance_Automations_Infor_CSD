# Mass Maintenance Automations — Infor CloudSuite Distribution

Python automation suite for Infor CSD (CloudSuite Distribution) mass maintenance workflows.
Each job follows a two-part pattern: **Part 1** queries the ERP or reads input files to identify records to change and generates SAAMM (mass maintenance) criteria; **Part 2** consumes the exported SAAMM file and applies the programmatic updates before re-import.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Jobs](#jobs)
- [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Requirements](#requirements)

---

## Overview

Infor CSD mass maintenance ("SAAMM") is a powerful but manual process: a user defines criteria, exports a tab-delimited file, edits it, and re-imports it. This suite automates the identification and editing steps, reducing manual work and human error across six recurring maintenance categories.

The workflow for every job is:

```
pt1.py → generates SAAMM criteria (product/warehouse lists, CSVs, logs)
  ↓
User exports SAAMM file from ERP → places in job's data folder
  ↓
pt2.py → reads export, applies field updates, overwrites file for re-import
```

A unified CLI (`CLI.py`) is provided to run jobs without navigating into each folder.

---

## Project Structure

```
Mass_Maintenance_Automations_Infor_CSD/
│
├── core/                        # Shared utilities
│   ├── __init__.py
│   ├── config.py                # ← you create this from config_sample.py
│   ├── config_sample.py         # Template — copy and fill in your paths/credentials
│   ├── etl_utils.py             # JDBC→SQLite sync, Excel/CSV loaders
│   ├── notifier.py              # Gmail SMTP email notifications
│   └── queries.py               # All SQL queries (Compass JDBC + SQLite)
│
├── mass_jobs/
│   ├── count_flag_sup/          # Set count flag on superseded products with on-hand qty
│   │   ├── pt1.py
│   │   └── pt2.py
│   ├── dnr/                     # Mark NLA/superseded OOS products as Do Not Replenish (X)
│   │   ├── pt1.py
│   │   └── pt2.py
│   ├── icsl_audit/              # Audit & correct ICSL/ICSW replenishment settings
│   │   ├── pt1.py
│   │   └── pt2.py
│   ├── nla_sup/                 # Process supersede/NLA pairs across ICSP + ICSW
│   │   ├── pt1.py
│   │   └── pt2.py
│   ├── oan_to_stock/            # Convert order-as-needed items to stock status
│   │   ├── pt1.py
│   │   └── pt2.py
│   ├── thresholds/              # Apply order threshold edits (linept, orderpt, etc.)
│   │   ├── pt1.py
│   │   └── pt2.py
│   └── whse_rank_e/             # Set warehouse rank to 'E' for superseded items with qty
│       ├── pt1.py
│       └── pt2.py
│
└── CLI.py                       # Unified command-line runner
```

---

## Jobs

### `count_flag_sup` — Count Flag: Superseded Products
Identifies superseded products (ICSP `descrip_2 LIKE '%REPLD%'`) that still have on-hand quantity above zero across active warehouses. Sets `countfl = 'yes'` in the SAAMM export so those records are picked up in cycle counting.

| Part | What it does |
|------|-------------|
| pt1 | Queries Compass via JDBC; emails a `.txt` with distinct prod/whse lists as SAAMM criteria |
| pt2 | Loads the criteria CSV + most recent `mmicsw*` export; sets `countfl = 'yes'` for matches |

---

### `dnr` — Do Not Replenish (Status X)
Finds NLA ("No Longer Available") and superseded products that are out of stock in both DCs and most branch warehouses. Marks them `statustype = 'X'` so they are no longer replenished.

Runs two queries: one for LK/MISC product lines (eligible for full DNR), one to surface non-LK prodline anomalies for manual review.

| Part | What it does |
|------|-------------|
| pt1 | Runs both queries; writes a dated log with comma-separated criteria and a CSV of prodline issues |
| pt2 | Loads criteria CSV + most recent `mmicsw*` export; sets `statustype = 'X'` for matched pairs |

---

### `icsl_audit` — ICSL/ICSW Replenishment Audit
The most complex job. Audits every product-line/warehouse combination in ICSL against a set of business rules covering order calculation type, usage control, ARP paths, seasonal flags, and more. Only rows where at least one field *would change* are surfaced.

Uses a local SQLite database as a computation layer — reference data (ARP path exceptions, usage control rules, warehouse metadata) is loaded from maintained Excel/CSV files before each run.

| Part | What it does |
|------|-------------|
| pt1 | (Optionally) syncs fresh ICSL data from Compass; refreshes SQLite reference tables; runs audit SQL; saves results CSV + impacted prodline list + dated log |
| pt2 | Reads audit results CSV; auto-discovers the SAAMM export in the `in/` subfolder; applies `new_*` column values to the matching base columns; writes updated file to `out/` |

---

### `nla_sup` — Supersede / NLA Processing
Processes a manually-maintained Excel list of old→new supersede pairs and NLA (No Longer Available) declarations. Updates both the ICSP (product description) and ICSW (vendor product, product line) SAAMM exports.

Includes validation checks (old product not found in export, new product missing warehouse rows), an OEM NLA alert for core-vendor products with recent usage, and email notifications to stakeholders.

| Part | What it does |
|------|-------------|
| pt1 | Reads the Excel lookup; prints and saves comma-delimited prod lists for SAAMM |
| pt2 | Updates ICSP (`descrip_2`) and ICSW (`vendprod`, `prodline`) for all pairs; runs validation checks; appends processed items to a supervisor review log; sends email alerts |

---

### `oan_to_stock` — Order-As-Needed → Stock Conversion
Converts products from OAN (Order As Needed) status to STOCK status for specific warehouse/product combinations provided in an Excel file.

| Part | What it does |
|------|-------------|
| pt1 | Reads the Excel file; writes distinct prod/whse lists to a `.txt` file for SAAMM set creation |
| pt2 | Reads mmicsw export; sets `statustype = 'S'` for matched pairs; archives the input Excel |

---

### `thresholds` — Order Threshold Edits
Applies order point threshold edits (linept, minthreshold, orderpt, ordptadjty, statustype) to ICSW based on input Excel/CSV reports. Supports warehouse-25, warehouse-50, warehouse-05, status-only, and branch threshold files. Includes an interactive approval prompt before overwriting existing non-standard `threshrefer` values.

| Part | What it does |
|------|-------------|
| pt1 | Scans the threshold folder for all report files; extracts part numbers from column A; writes a comma-delimited list and log for use with the SAAMM "order threshold" template |
| pt2 | Loads mmicsw export + all input files; pre-scans for `threshrefer` values needing approval; applies edits by priority (branch > full edit > status-only); archives input files |

---

### `whse_rank_e` — Warehouse Rank E (Superseded Items)
Finds superseded products (identified via the ICSEC cross-reference table) that still have available quantity at non-DC warehouses and do not already have `whserank = 'E'`. Sets rank to `'E'` to deprioritize replenishment at those locations.

| Part | What it does |
|------|-------------|
| pt1 | Queries Compass via JDBC; saves a CSV of prod/whse matches; writes a log with comma-separated lists for SAAMM |
| pt2 | Loads the CSV + most recent `mmicsw*` export; sets `whserank = 'E'` for matched pairs |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/jsiversart/Mass_Maintenance_Automations_Infor_CSD.git
cd Mass_Maintenance_Automations_Infor_CSD
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure

Copy the sample config and fill in your values:

```bash
cp core/config_sample.py core/config.py
```

Edit `core/config.py`:

- **`PATHS`** — Update every path to match your local folder structure
- **`JDBC`** — Fill in your Infor Compass JDBC connection details (`class`, `url`, `jar`, `user`, `password`)
- **`EMAILS`** — Set your email addresses
- **`GMAIL_CREDS`** — Set your Gmail address and create a [Gmail App Password](https://support.google.com/accounts/answer/185833); then set it as an environment variable:

```bash
# Windows
set GMAIL_APP_PASSWORD=your_app_password_here

# Mac/Linux
export GMAIL_APP_PASSWORD=your_app_password_here
```

> `config.py` is listed in `.gitignore` and will never be committed.

### 4. (Optional) Install the CLI as a command

Add a `pyproject.toml` entry point or run directly via Python (see [Usage](#usage)).

---

## Configuration

All configurable values live in `core/config.py` (created from `config_sample.py`).

### PATHS

| Key | Description |
|-----|-------------|
| `purchdata` | Path to the local SQLite database used for ICSL audit |
| `saamms` | Root folder for SAAMM export files |
| `count_flag_data` | Folder where `mmicsw*` exports for count flag are saved |
| `icsl_data` | Root folder for dated ICSL audit run folders |
| `sups_nlas_data` | Folder containing the NLA/SUP Excel files and SAAMM exports |
| `oantostock` | Folder containing the OAN-to-stock Excel input file |
| `threshold_data` | Folder containing threshold report Excel/CSV files |
| `whse_rank_data` | Folder where `mmicsw*` exports for warehouse rank are saved |
| `icsw_maint_spreadsheet` | Path to the Excel file with ARP path exceptions and usage control rules |
| `warehouse_info_csv` | Path to the warehouse info CSV (used by ICSL audit) |

### JDBC

Standard JDBC connection parameters for your Infor Compass instance. The `jar` field should point to your JDBC driver `.jar` file.

### EMAILS / GMAIL_CREDS

Used by `notifier.py` for automated email alerts. Gmail App Password is loaded from the `GMAIL_APP_PASSWORD` environment variable — never hardcode it.

---

## Usage

### Via CLI

```bash
# Run Part 1 for a job
python CLI.py pt1 dnr

# Run Part 2 for a job
python CLI.py pt2 dnr

# Run both parts (pauses between for manual ERP export step)
python CLI.py run dnr

# List all available jobs
python CLI.py list
```

### Directly

```bash
cd mass_jobs/dnr
python pt1.py
# ... export SAAMM from ERP, place in data folder ...
python pt2.py
```

---

## Requirements

```
pandas
openpyxl
jaydebeapi
JPype1
```

> `smtplib`, `sqlite3`, `pathlib`, `csv`, `shutil`, `glob`, and `argparse` are all Python standard library — no install needed.

A `requirements.txt` is included in the repo root.
