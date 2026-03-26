# nla_sup/pt1.py

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


def main():

    # --- Flexible settings ---
    file_path = PATHS["sups_nlas_data"] / "NLAs and SUPs to update via SAAMM.xlsx"
    old_col = "A"   # column for old numbers
    new_col = "B"   # column for new numbers
    sheet_name = 0  # 0 = first sheet, or use sheet name if needed

    # --- Load Excel ---
    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=f"{old_col}:{new_col}")

    # Rename for clarity (this way if you move them around, only change old_col/new_col above)
    df.columns = ["Old", "New"]

    # Trim whitespace & drop empty rows
    df = df.map(lambda x: str(x).strip() if pd.notna(x) else "")

    # --- Build Lists ---
    # 1. Comma-delimited, trimmed list of both old and new (label "Prod list")
    prod_list = ",".join([val for val in pd.concat([df["Old"], df["New"]]) if val and val.upper() != "NLA"])

    # 2. Comma-delimited, trimmed list of old only, formatted with quotes
    old_list = ",".join([f"'{val}'" for val in df["Old"] if val])

    # --- Export / Print ---
    print("Product list: use with 'SUP NLA ICSP TEMPLATE' and 'SUP NLA ICSW TEMPLATE' IN SAAMM")
    print(prod_list)
    print("\nOld No list only: Check for still on order")
    print(old_list)

    #Optional: save to text file
    out_path = file_path.parent / "Sup_NLA_Output.txt"
    with open(out_path, "w") as f:
            f.write("Product list: use with 'SUP NLA ICSP TEMPLATE' and 'SUP NLA ICSW TEMPLATE' IN SAAMM\n")
            f.write(prod_list + "\n\n")
            f.write("Old No list only: Check for still on order\n")
            f.write(old_list)

    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()