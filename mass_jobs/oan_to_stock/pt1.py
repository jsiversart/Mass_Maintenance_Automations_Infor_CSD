# oan_to_stock/pt1.py

import sys
from pathlib import Path


# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

import os
import pandas as pd
from core.config import PATHS

def main():
    # === CONFIGURATION ===
    folder_path = PATHS["oantostock"]

    # === STEP 1: Find the Excel file (assumes there's only one) ===
    excel_file = next((f for f in os.listdir(folder_path) if f.endswith('.xlsx')), None)

    if not excel_file:
        print("No Excel file found in the folder.")
    else:
        file_path = os.path.join(folder_path, excel_file)

        # === STEP 2: Read WHSE + PROD columns ===
        df = pd.read_excel(file_path, usecols="A:B", header=None, dtype=str).dropna()
        df.columns = ["whse", "prod"]

        # Clean fields
        df["whse"] = df["whse"].str.strip().str.zfill(2)
        df["prod"] = df["prod"].str.strip()

        # Remove header rows
        df = df[~df["prod"].str.lower().eq("part number")]

        # === STEP 3: Build lists ===
        prods = sorted(df["prod"].unique())
        whses = sorted(df["whse"].unique())

        prod_csv = ",".join(prods)
        whse_csv = ",".join(whses)

        distinct_pairs = set(df.apply(lambda x: (x["prod"], x["whse"]), axis=1))
        pair_count = len(distinct_pairs)

        # === STEP 4: Write lists to TXT ===
        list_file = os.path.join(folder_path, "oan_to_stock_lists.txt")

        with open(list_file, "w") as f:

            # Distinct pair count
            f.write(f"Distinct PROD/WHSE pairs: {pair_count}\n\n")

            # SAAMM set note

            f.write("Generate a SAAMM set using the 'OAN TO STOCK TEMPLATE', filtering to these products / warehouses:\n\n")

            # PRODs
            f.write(f"Distinct PRODs ({len(prods)}):\n")
            f.write(prod_csv + "\n\n")

            # WHSEs
            f.write(f"Distinct WHSEs ({len(whses)}):\n")
            f.write(whse_csv + "\n")

        print(f"List file written: {list_file}")

        # === DONE ===
        print("====DONE====")
        print("Generate a SAAMM set using the 'OAN TO STOCK TEMPLATE', filtering to these products:")
        print(prod_csv)

if __name__ == "__main__":
    main()