# thresholds/pt2.py

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
import shutil
from core.config import PATHS



# =============================================================================
# SUMMARY — HOW THIS SCRIPT WORKS (BY CHATGPT)
# =============================================================================
#
# PURPOSE:
#   Automate Threshold editing by:
#   • Importing mmicsw.txt
#   • Applying edits from various Excel/CSV files (25, 50, 5, Status, Branch Threshold)
#   • Updating the mmicsw file based on rules
#   • Archiving the processed Excel files
#
# OVERVIEW OF LOGIC:
#
#   1. LOAD mmicsw.txt
#      - Find the mmicsw*.txt file in the working folder
#      - Read into a DataFrame
#      - Normalize key columns to strings to prevent dtype conflicts
#
#   2. LOAD INPUT EXCEL/CSV FILES
#      - Functions scan the working directory for:
#          "25*.xlsx/csv" → marks warehouse 25 edits
#          "50*.xlsx/csv" → marks warehouse 50 edits
#          "5 *.xlsx/csv" → marks warehouse 05 edits (note the space)
#          "status*.xlsx/csv" → applies status-only updates
#          "*branch threshold*.xlsx/csv" → highest-priority overrides
#
#      - Each file produces a DataFrame of product/warehouse pairs
#      - These are converted into sets of keys for fast lookup
#
#   3. COMBINE 25/50/5 FILES
#      - Merges them into one list of "full edits"
#      - These have second-priority after Branch Threshold
#
#   4. APPLY EDIT LOGIC (row-by-row)
#      - For each product/warehouse:
#
#        (A) Branch Threshold logic (TOP PRIORITY)
#            • statustype = 'S'
#            • ordptadjty = 't'
#            • linept = minthreshold = orderpt = "1"
#            • threshrefer = today's date (with approval check)
#
#        (B) Full edits (25/50/5)
#            • linept = "1"
#            • minthreshold = "1" unless vendor = 775
#            • orderpt = "1"
#            • ordptadjty = "t"
#            • Convert statustype 'O' → 'S'
#            • threshrefer = today's date (with approval check)
#
#        (C) Status-only file logic (if not already captured above)
#            • statustype = 'S'
#
#      - If none match, row is unchanged
#
#   5. WRITE UPDATED TXT FILE
#      - Overwrites the original mmicsw.txt
#      - Uses tab-delimited format
#
#   6. ARCHIVE INPUT FILES
#      - Moves all Excel/CSV files used to the /archive folder
#
#   7. DONE
#      - Prints completion message
#
# =============================================================================



# === CONFIGURATION ===
folder_path = PATHS["threshold_data"]
today_str = datetime.now().strftime("JS%m%d%y")
archive_path = os.path.join(folder_path, "archive")

# Global dictionary to store approval decisions for this run
threshrefer_approvals = {}

def normalize_whse(x):
    if x is None:
        return x
    x = str(x).strip()
    # Leave 3+ digit warehouses unchanged
    if len(x) >= 3:
        return x
    # Normalize 1-digit or 2-digit warehouses
    return x.zfill(2)

def should_overwrite_threshrefer(current_value):
    """
    Determine if threshrefer should be overwritten.
    - If blank or starts with 'JS': auto-approve
    - Otherwise: ask user for approval (once per distinct value per run)
    
    Returns True if overwrite approved, False otherwise.
    """
    current_value = str(current_value).strip()
    
    # Auto-approve if blank or starts with JS
    if current_value == "" or current_value.upper().startswith("JS"):
        return True
    
    # Check if we've already asked about this value in this run
    if current_value in threshrefer_approvals:
        return threshrefer_approvals[current_value]
    
    # Ask user for approval
    print(f"\n{'='*60}")
    print("THRESHREFER OVERWRITE APPROVAL NEEDED - GO OVER THESE WITH CALEB")
    print(f"{'='*60}")
    print(f"Current threshrefer value: '{current_value}'")
    print(f"Will be overwritten to: '{today_str}'")
    print(f"{'='*60}")
    
    while True:
        response = input(f"Approve overwriting '{current_value}' to '{today_str}'? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            threshrefer_approvals[current_value] = True
            print(f"✓ Approved. All rows with threshrefer='{current_value}' will be updated.\n")
            return True
        elif response in ['n', 'no']:
            threshrefer_approvals[current_value] = False
            print(f"✗ Denied. Rows with threshrefer='{current_value}' will keep their current value.\n")
            return False
        else:
            print("Please enter 'y' or 'n'.")

def main():
    # === STEP 1: Load the mmicsw txt file ===
    mmicsw_file = next(f for f in os.listdir(folder_path) if f.startswith("mmicsw") and f.endswith(".txt"))
    mmicsw_path = os.path.join(folder_path, mmicsw_file)

    df_txt = pd.read_csv(mmicsw_path, sep='\t', dtype=str, keep_default_na=False)

    # Normalize critical columns
    for col in ['linept', 'minthreshold', 'orderpt', 'ordptadjty', 'statustype']:
        if col in df_txt.columns:
            df_txt[col] = df_txt[col].astype(str)

    df_txt["whse"] = df_txt["whse"].apply(normalize_whse)

    # === STEP 2: Load Excel/CSV files ===
    excel_files = os.listdir(folder_path)

    def safe_load_table(prefix, whse):
        """Load Excel or CSV by prefix; return DataFrame and filename, or None,None if not found."""
        try:
            file = next(f for f in excel_files 
                    if f.lower().startswith(prefix.lower()) 
                    and (f.lower().endswith(".xlsx") or f.lower().endswith(".csv")))
            file_path = os.path.join(folder_path, file)
            
            if file.lower().endswith(".xlsx"):
                df = pd.read_excel(file_path, usecols="A", header=None, dtype=str).dropna()
            else:
                df = pd.read_csv(file_path, usecols=[0], header=None, dtype=str).dropna()
            
            df = df.rename(columns={0: "prod"})
            df["whse"] = whse
            return df, file
        except StopIteration:
            print(f"No {prefix} file found — skipping.")
            return None, None

    df_25, excel_25 = safe_load_table("25", "25")
    df_50, excel_50 = safe_load_table("50", "50")
    df_5, excel_5 = safe_load_table("5 ", "05")  # note space in "5 "

    # Branch Threshold file
    branch_file = next((f for f in excel_files 
                    if "branch threshold" in f.lower() 
                    and (f.lower().endswith(".xlsx") or f.lower().endswith(".csv"))), None)

    if branch_file:
        print(f"Branch Threshold report found: {branch_file}")
        branch_path = os.path.join(folder_path, branch_file)
        
        if branch_file.lower().endswith(".xlsx"):
            df_branch = pd.read_excel(branch_path, usecols="A:B", header=None, dtype=str).dropna()
        else:
            df_branch = pd.read_csv(branch_path, usecols=[0,1], header=None, dtype=str).dropna()
        
        df_branch.columns = ["prod", "whse"]
        df_branch["whse"] = df_branch["whse"].apply(normalize_whse)
        branch_keys = set(tuple(x) for x in df_branch.to_records(index=False))
    else:
        df_branch = None
        branch_keys = set()

    # Status file (optional)
    status_file = next((f for f in excel_files 
                    if f.lower().startswith("status") 
                    and (f.lower().endswith(".xlsx") or f.lower().endswith(".csv"))), None)

    if status_file:
        status_path = os.path.join(folder_path, status_file)
        
        if status_file.lower().endswith(".xlsx"):
            df_status = pd.read_excel(status_path, usecols="A:B", header=None, dtype=str).dropna()
        else:
            df_status = pd.read_csv(status_path, usecols=[0,1], header=None, dtype=str).dropna()
        
        df_status.columns = ["prod", "whse"]
        status_keys = set(tuple(x) for x in df_status.to_records(index=False))
    else:
        df_status = None
        status_keys = set()

    # Warehouse normalization
    if df_25 is not None:
        df_25["whse"] = df_25["whse"].apply(normalize_whse)
    if df_50 is not None:
        df_50["whse"] = df_50["whse"].apply(normalize_whse)
    if df_5 is not None:
        df_5["whse"] = df_5["whse"].apply(normalize_whse)
    if df_branch is not None:
        df_branch["whse"] = df_branch["whse"].apply(normalize_whse)
    if df_status is not None:
        df_status["whse"] = df_status["whse"].apply(normalize_whse)

    # Combine 25/50/5 for full edits
    dfs = [df for df in [df_25, df_50, df_5] if df is not None]
    if dfs:
        edit_df = pd.concat(dfs)
        edit_keys = set(tuple(x) for x in edit_df.to_records(index=False))
    else:
        edit_df = pd.DataFrame(columns=["prod", "whse"])
        edit_keys = set()

    # === STEP 3: Pre-scan for unique threshrefer values that need approval ===
    print("\n" + "="*60)
    print("SCANNING FOR THRESHREFER VALUES REQUIRING APPROVAL")
    print("="*60)

    unique_threshrefer_values = set()

    for _, row in df_txt.iterrows():
        key = (row['prod'], row['whse'])
        
        # Check if this row will be edited (branch or full edit)
        if key in branch_keys or key in edit_keys:
            current_threshrefer = str(row.get('threshrefer', '')).strip()
            
            # If not blank and not starting with JS, add to set for approval
            if current_threshrefer != "" and not current_threshrefer.upper().startswith("JS"):
                unique_threshrefer_values.add(current_threshrefer)

    if unique_threshrefer_values:
        print(f"\nFound {len(unique_threshrefer_values)} unique threshrefer value(s) requiring approval:")
        for val in sorted(unique_threshrefer_values):
            print(f"  - '{val}'")
        print("\nYou will be prompted to approve each one.\n")
        
        # Get approval for each unique value
        for val in sorted(unique_threshrefer_values):
            should_overwrite_threshrefer(val)
    else:
        print("No threshrefer values require manual approval.\n")

    # === STEP 4: Apply all edits ===
    def apply_all_edits(row):
        key = (row['prod'], row['whse'])
        
        # Branch Threshold edits (highest priority)
        if key in branch_keys:
            row['statustype'] = "S"
            row['ordptadjty'] = "t"
            row['minthreshold'] = "1"
            row['orderpt'] = "1"
            row['linept'] = "1"
            
            # Update threshrefer only if approved
            if should_overwrite_threshrefer(row.get('threshrefer', '')):
                row['threshrefer'] = today_str
            
            return row
        
        # Full edits (25/50/5)
        if key in edit_keys:
            row['linept'] = "1"
            if row.get('arpvendno', '') != "775":
                row['minthreshold'] = "1"
            row['orderpt'] = "1"
            row['ordptadjty'] = "t"
            if row['statustype'].lower() == "o":
                row['statustype'] = "S"
            
            # Update threshrefer only if approved
            if should_overwrite_threshrefer(row.get('threshrefer', '')):
                row['threshrefer'] = today_str
            
            return row
        
        # Status-only edits
        if key in status_keys and (key not in edit_keys):
            row['statustype'] = "S"
            return row
        
        return row

    df_txt = df_txt.apply(apply_all_edits, axis=1)

    # === STEP 5: Overwrite the original txt file ===
    df_txt.to_csv(mmicsw_path, sep='\t', index=False, na_rep='')

    # === STEP 6: Move processed Excel/CSV files to archive ===
    files_to_archive = [f for f in [excel_25, excel_50, excel_5, status_file, branch_file] if f]

    os.makedirs(archive_path, exist_ok=True)

    for filename in files_to_archive:
        shutil.move(os.path.join(folder_path, filename), os.path.join(archive_path, filename))

    print("\n" + "="*60)
    print("THRESHOLD PROCESSING COMPLETE")
    print("="*60)
    print(f"✓ TXT file updated: {mmicsw_file}")
    print(f"✓ Files archived: {len(files_to_archive)}")
    print(f"✓ Threshrefer approvals recorded: {len(threshrefer_approvals)}")
    print("="*60)


if __name__ == "__main__":
    main()