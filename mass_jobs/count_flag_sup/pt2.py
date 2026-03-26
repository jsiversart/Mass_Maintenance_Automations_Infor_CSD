# count_flag_sup/pt2.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))



import pandas as pd
from core.config import PATHS


def main():
    # === STEP 1: LOAD MATCH FILE ===
    df_sql = pd.read_csv("prod_whse_matches_count_flag.csv", dtype={'prod': str, 'whse': str})

    # Pad whse values to always be 2 characters
    df_sql['whse'] = df_sql['whse'].astype(str).str.zfill(2)

    df_sql['key'] = df_sql['prod'].astype(str) + "_" + df_sql['whse']

    # === STEP 2: FIND MOST RECENT 'mmicsw' FILE ===
    downloads_folder = PATHS["count_flag_data"]
    prefix = "mmicsw"

    matching_files = sorted(
        [f for f in downloads_folder.glob(f"{prefix}*") if f.is_file()],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not matching_files:
        print("No matching 'mmicsw' file found.")
        exit()

    erp_file = matching_files[0]
    print(f"Using ERP file: {erp_file.name}")

    # === STEP 3: LOAD ERP FILE AND UPDATE warehouse rank ===
    df_erp = pd.read_csv(erp_file, sep='\t', dtype={'prod': str, 'whse': str})

    # Pad whse values here too so keys match format
    df_erp['whse'] = df_erp['whse'].astype(str).str.zfill(2)

    df_erp['key'] = df_erp['prod'].astype(str) + "_" + df_erp['whse']

    df_erp.loc[df_erp['key'].isin(df_sql['key']), 'countfl'] = 'yes'
    df_erp.drop(columns='key', inplace=True)

    # === STEP 4: SAVE UPDATED FILE ===
    output_file = erp_file.with_name(f"{erp_file.name}")
    df_erp.to_csv(erp_file, sep='\t', index=False)
    print(f"Updated file saved (overwritten): {erp_file.name}")


if __name__ == "__main__":
    main()
