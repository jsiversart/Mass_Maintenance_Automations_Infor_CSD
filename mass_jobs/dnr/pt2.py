# dnr/pt2.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))



import pandas as pd
from pathlib import Path
from core.config import PATHS




# === CONFIG ===
MATCH_FILE = "prod_whse_matches_dnr.csv"
INPUT_FOLDER = PATHS["dnr"]
PREFIX = "mmicsw"
DRY_RUN = False   # set True to test without saving

def normalize_whse_series(s):
    """Turn a Series into 2-digit warehouse strings safely."""
    # Convert to string, strip whitespace, handle NaN
    s = s.fillna("").astype(str).str.strip()
    # If looks like a float (e.g., '1.0'), drop .0
    s = s.replace(r'\.0+$', '', regex=True)
    # Remove any stray decimal if it's integer-like '1.00' etc.
    s = s.replace(r'(\d+)\.\d+$', r'\1', regex=True)
    # Zero-pad; empty strings remain empty
    return s.apply(lambda x: x.zfill(2) if x != "" else "")

def load_csv_safe(path, **kwargs):
    """Load CSV with some safe defaults and show sample."""
    try:
        df = pd.read_csv(path, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to read {path!s}: {e}")
    return df

def main():
    # === STEP 1: Load match file ===
    match_path = Path(MATCH_FILE)
    if not match_path.exists():
        raise SystemExit(f"Match file not found: {match_path}")

    df_sql = load_csv_safe(match_path)
    print(f"Loaded match file: {match_path} rows={len(df_sql)} cols={list(df_sql.columns)}")

    # sanity: required columns
    if 'prod' not in df_sql.columns or 'whse' not in df_sql.columns:
        raise SystemExit("Match file must contain 'prod' and 'whse' columns.")

    # normalize whse
    df_sql['whse_orig'] = df_sql['whse']  # keep original for debugging
    df_sql['whse'] = normalize_whse_series(df_sql['whse'])
    df_sql['prod'] = df_sql['prod'].astype(str).str.strip()
    df_sql['key'] = df_sql['prod'] + "_" + df_sql['whse']

    print("Sample of normalized match file (prod, whse_orig, whse, key):")
    print(df_sql[['prod','whse_orig','whse','key']].head(10).to_string(index=False))

    # === STEP 2: find most recent ERP file ===
    matching_files = sorted(
        [f for f in INPUT_FOLDER.glob(f"{PREFIX}*") if f.is_file()],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not matching_files:
        raise SystemExit(f"No matching '{PREFIX}*' files in {INPUT_FOLDER}")

    erp_file = matching_files[0]
    print(f"Using ERP file: {erp_file.name}")

    # === STEP 3: load ERP file ===
    # try common separators; you used sep='\t' earlier, keep that but fallback to auto
    try:
        df_erp = load_csv_safe(erp_file, sep='\t', dtype=str)
    except Exception:
        df_erp = load_csv_safe(erp_file, dtype=str)

    print(f"Loaded ERP file: rows={len(df_erp)} cols={list(df_erp.columns)}")

    # check required columns present
    if 'prod' not in df_erp.columns or 'whse' not in df_erp.columns:
        raise SystemExit("ERP file must contain 'prod' and 'whse' columns.")

    # normalize ERP whse and prod
    df_erp['whse_orig'] = df_erp['whse']
    df_erp['whse'] = normalize_whse_series(df_erp['whse'])
    df_erp['prod'] = df_erp['prod'].astype(str).str.strip()
    df_erp['key'] = df_erp['prod'] + "_" + df_erp['whse']

    print("Sample of normalized ERP data (prod, whse_orig, whse, key):")
    print(df_erp[['prod','whse_orig','whse','key']].head(10).to_string(index=False))

    # === DIAGNOSTIC: show unmatched keys counts ===
    # how many keys in match file are present in ERP
    matched_keys = df_sql['key'].isin(df_erp['key']).sum()
    total_keys = len(df_sql)
    print(f"Match-file keys present in ERP: {matched_keys} / {total_keys}")

    # show some keys that did NOT match (first 10)
    missing_keys = df_sql.loc[~df_sql['key'].isin(df_erp['key']), ['prod','whse','key']].head(20)
    if not missing_keys.empty:
        print("Sample keys from match file NOT found in ERP (first 20):")
        print(missing_keys.to_string(index=False))
    else:
        print("All match-file keys found in ERP (or at least first sample).")

    # === STEP 4: update status ===
    # show pre-update distribution of statustype column (if present)
    if 'statustype' in df_erp.columns:
        print("ERP statustype value counts BEFORE update:")
        print(df_erp['statustype'].value_counts(dropna=False).to_string())
    else:
        print("ERP file has no 'statustype' column; will create it.")

    # Apply update to matched rows
    mask = df_erp['key'].isin(df_sql['key'])
    updated_count = mask.sum()
    print(f"Rows to update in ERP: {updated_count}")

    if updated_count == 0:
        print("No rows matched — nothing to update. Exiting.")
        return

    # Backup original ERP file before overwrite
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # backup_path = erp_file.with_name(f"{erp_file.stem}_backup_{timestamp}{erp_file.suffix}")
    # shutil.copy2(erp_file, backup_path)
    # print(f"Backup of ERP file saved to: {backup_path.name}")

    # set statustype = 'X' for matched rows
    df_erp.loc[mask, 'statustype'] = 'X'

    print("Sample of updated rows (first 10):")
    print(df_erp.loc[mask, ['prod','whse','statustype']].head(10).to_string(index=False))

    # === SAVE (unless dry run) ===
    if DRY_RUN:
        print("DRY RUN: no changes written. To persist changes set DRY_RUN=False.")
    else:
        # save back using same sep as read (tab)
        try:
            df_erp.to_csv(erp_file, sep='\t', index=False)
        except Exception as e:
            # attempt default save
            df_erp.to_csv(erp_file, index=False)
            print("Warning: failed to save with tab separator; saved with default separator instead.")
        print(f"Updated file saved (overwritten): {erp_file.name}")
        #print(f"Original file backed up as: {backup_path.name}")

if __name__ == "__main__":
    main()
