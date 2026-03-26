# oan_to_stock/pt2.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import os
import pandas as pd
import shutil
from core.config import PATHS



# === CONFIGURATION ===
folder_path = PATHS["oantostock"]
archive_folder = os.path.join(folder_path, "archive")

def clean_prod(p):
    if p is None:
        return ""
    p = str(p).strip()
    if p.startswith("'"):   # strip leading apostrophe
        p = p[1:]
    return p

def main():
    # === STEP 1: Locate files ===
    txt_file = next(f for f in os.listdir(folder_path) if f.startswith("mmicsw") and f.endswith(".txt"))
    excel_file = next(f for f in os.listdir(folder_path) if f.endswith(".xlsx"))

    txt_path = os.path.join(folder_path, txt_file)
    excel_path = os.path.join(folder_path, excel_file)

    # === STEP 2: Load TXT ===
    df_txt = pd.read_csv(txt_path, sep="\t", dtype=str, keep_default_na=False)
    df_txt["whse"] = df_txt["whse"].str.zfill(2)
    df_txt["prod"] = df_txt["prod"].apply(clean_prod)

    # === STEP 3: Load Excel and clean ===
    df_excel = pd.read_excel(excel_path, usecols="A:B", dtype=str, header=None)
    df_excel.columns = ["whse", "prod"]
    df_excel = df_excel.dropna()
    df_excel = df_excel[~df_excel["prod"].str.strip().str.lower().eq("part number")]

    # Normalize
    df_excel["whse"] = df_excel["whse"].str.strip().str.zfill(2)
    df_excel["prod"] = df_excel["prod"].apply(clean_prod)

    # Create set of valid (prod, whse) pairs
    valid_keys = set(df_excel.apply(lambda x: (x["prod"], x["whse"]), axis=1))

    # === STEP 4: Update statustype ===
    updated_count = 0

    def update_status(row):
        global updated_count
        if (row["prod"], row["whse"]) in valid_keys and row["statustype"] != "S":
            updated_count += 1
            return "S"
        return row["statustype"]

    df_txt["statustype"] = df_txt.apply(update_status, axis=1)

    # === STEP 5: Write TXT ===
    df_txt.to_csv(txt_path, sep="\t", index=False, na_rep='')

    # === STEP 6: Archive Excel ===
    os.makedirs(archive_folder, exist_ok=True)
    shutil.move(excel_path, os.path.join(archive_folder, excel_file))

    # === STEP 7 ===
    print(f"OAN to STOCK update complete. {updated_count} row(s) updated. Excel file archived.")

if __name__ == "__main__":
    main()