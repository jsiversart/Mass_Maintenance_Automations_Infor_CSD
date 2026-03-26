# thresholds/pt1.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import os
import pandas as pd
from datetime import datetime
from core.config import PATHS



# =============================================================================
# HOW TO USE THIS SCRIPT 
# =============================================================================
#
# PURPOSE:
#   This script extracts all part numbers from threshold-related reports in the
#   Threshold working directory and produces:
#       • A comma-delimited list of all unique parts
#       • A detailed extraction log saved in the same folder
#
# WHAT YOU NEED TO DO BEFORE RUNNING:
#
#   1. Make sure your working directory contains one or more of the following:
#         • A "25..." report (e.g., 25_threshold.xlsx)
#         • A "5..." report (e.g., 5 threshold.csv / 50 threshold.xlsx)
#         • A "Status..." report
#         • (Optional) A "Branch Threshold" report
#
#      The script automatically detects:
#         - .xlsx files
#         - .csv files
#
#   2. Ensure that column A of each report contains product numbers.
#      (The script ONLY reads column A.)
#
#   3. Verify that bogus column-header values (e.g., "product", "Part Number")
#      are listed in the exclude_terms set. These values are ignored.
#
# HOW TO RUN:
#
#   • Simply execute the script in Python — no parameters required.
#     Example:
#         python threshold_extract.py
#
# WHAT THE SCRIPT DOES WHEN RUN:
#
#   1. Creates a log file named:
#          threshold_extract_log_MMDDYY.txt
#
#   2. Scans the directory for all threshold-related reports and logs what it
#      finds.
#
#   3. Reads column A from each file, removes header-like values, and combines
#      part numbers across all files.
#
#   4. Prints a comma-delimited list of all unique part numbers to the console.
#
#   5. Writes the same list into the log file, along with details about:
#         • Each file processed
#         • How many parts came from each
#         • Any read errors
#
# WHAT YOU DO AFTER RUNNING:
#
#   • Copy the printed comma-delimited list into SAAMM.
#   • Use "order threshold template" as the base set when creating the SAAMM set.
#   • Review the log (optional) for audit purposes or troubleshooting.
#
# TROUBLESHOOTING:
#
#   • If you see “No valid part numbers found”:
#         - Ensure the files are in the correct folder
#         - Ensure part numbers are in column A
#         - Make sure header names are included in exclude_terms
#
#   • If Excel files fail to open:
#         - Verify they are not open in another program
#
# =============================================================================


def main():

    # === CONFIGURATION ===
    folder_path = PATHS["threshold_data"]
    exclude_terms = {"Part Number", "product", "prod"}  # Case-insensitive match

    # === SETUP LOG FILE ===
    today = datetime.now().strftime("%m%d%y")
    log_path = os.path.join(folder_path, f"threshold_extract_log_{today}.txt")

    with open(log_path, "w", encoding="utf-8") as log:

        log.write(f"=== Threshold Extraction Log — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")

        # === STEP 1: Find relevant files ===
        files = os.listdir(folder_path)

        # Allow both .xlsx and .csv
        def is_target_file(f):
            name_lower = f.lower()
            return (
                (name_lower.endswith('.xlsx') or name_lower.endswith('.csv'))
                and (f.startswith('25') or f.startswith('5') or f.startswith('Status'))
            )

        target_files = [f for f in files if is_target_file(f)]

        # Include Branch Threshold file if present
        branch_file = next(
            (f for f in files if "branch threshold" in f.lower() and (f.lower().endswith(".xlsx") or f.lower().endswith(".csv"))),
            None
        )
        if branch_file:
            log.write(f"Branch Threshold report found: {branch_file}\n")
            target_files.append(branch_file)
        else:
            log.write("No Branch Threshold report found.\n")

        if not target_files:
            msg = "No threshold-related reports found.\n"
            log.write(msg)
            print(msg.strip())
            exit()

        log.write(f"Found {len(target_files)} report(s):\n")
        for f in target_files:
            log.write(f"  - {f}\n")

        # === STEP 2: Read and collect part numbers from column A ===
        all_parts = set()
        exclude_lower = {term.lower() for term in exclude_terms}

        def read_column_a(file_path):
            """Read column A from xlsx or csv file and return Series of parts."""
            if file_path.lower().endswith(".xlsx"):
                df = pd.read_excel(file_path, usecols='A', header=None, dtype=str)
            else:
                df = pd.read_csv(file_path, usecols=[0], header=None, dtype=str)
            return df.iloc[:, 0].dropna().str.strip()

        for filename in target_files:
            file_path = os.path.join(folder_path, filename)
            try:
                parts = read_column_a(file_path)
                clean_parts = parts[~parts.str.lower().isin(exclude_lower)]
                unique_parts = clean_parts.unique()
                all_parts.update(unique_parts)
                log.write(f"Processed {filename}: {len(unique_parts)} part(s) collected.\n")

            except Exception as e:
                log.write(f"Error reading {filename}: {e}\n")

        # === STEP 3: Write results to log and console ===
        if all_parts:
            part_list = ",".join(sorted(all_parts))
            log.write(f"\nTotal unique parts collected: {len(all_parts)}\n")
            log.write("Extraction complete.\n\n")
            log.write("=== Part List ===\n")
            log.write("=== USE SAAMM [order threshold template] ===\n")
            log.write(part_list + "\n")

            print("\nCreate SAAMM set based off of 'order threshold ALL template', list the following part #:")
            print(part_list)
            print(f"\nLog saved to: {log_path}")
        else:
            log.write("\nNo valid part numbers found in the available reports.\n")
            print("No valid part numbers found in the available reports.")
            print(f"\nLog saved to: {log_path}")


if __name__ == "__main__":
    main()