# icsl_audit/pt2.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import pandas as pd
import csv
from core.config import PATHS




ROOT = PATHS["icsl_data"]

def normalize_whse(val):
    try:
        return str(int(str(val).strip()))
    except:
        return str(val).strip()

def main():
    # ---------------------------
    # 1. locate newest dated folder
    # ---------------------------
    newest = max([d for d in ROOT.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime)
    summary_folder = newest / "summary"
    in_folder = newest / "in"
    out_folder = newest / "out"
    out_folder.mkdir(exist_ok=True)

    # ---------------------------
    # 2. find source CSV (only CSV in summary)
    # ---------------------------
    source_csv = next(summary_folder.glob("*.csv"))

    # ---------------------------
    # 3. find target file (prefer .txt; otherwise any single CSV/TXT)
    #    we'll try to detect delimiter for target
    # ---------------------------
    txt_candidates = list(in_folder.glob("*.txt"))
    csv_candidates = list(in_folder.glob("*.csv"))
    if txt_candidates:
        target_path = txt_candidates[0]
    elif csv_candidates:
        target_path = csv_candidates[0]
    else:
        raise FileNotFoundError(f"No .txt or .csv found in {in_folder}")

    # ---------------------------
    # 4. load source and target (preserve everything as strings)
    # ---------------------------
    src = pd.read_csv(source_csv, dtype=str, keep_default_na=False).fillna("")
    # Detect delimiter for target, then load as strings preserving blanks
    with open(target_path, "r", encoding="cp1252", newline="") as fh:
        sample = fh.read(2048)
        sniffer = csv.Sniffer()
        try:
            delim = sniffer.sniff(sample).delimiter
        except Exception:
            delim = ","  # fallback
    target = pd.read_csv(
        target_path,
        dtype=str,
        keep_default_na=False,
        sep=delim,
        encoding="cp1252"
    ).fillna("")
    # ---------------------------
    # 5. normalize whse on both
    # ---------------------------
    if "whse" not in src.columns:
        raise KeyError(f"Source CSV missing 'whse' column: {source_csv}")
    if "whse" not in target.columns:
        raise KeyError(f"Target file missing 'whse' column: {target_path}")

    src["whse_norm"] = src["whse"].apply(normalize_whse)
    target["whse_norm"] = target["whse"].apply(normalize_whse)

    # ---------------------------
    # 6. discover update pairs from SOURCE (new_x -> x)
    # ---------------------------
    new_cols = [c for c in src.columns if c.startswith("new_")]
    base_cols = [c[4:] for c in new_cols if c[4:] in target.columns]  # ensure target has base column
    field_pairs = [(base, "new_" + base) for base in base_cols]

    if not field_pairs:
        print("No matching new_*/base pairs found between source and target. Exiting.")
        raise SystemExit

    # ---------------------------
    # 7. build a lookup from source keyed by (prodline, whse_norm)
    #    For each key keep the last occurrence (if duplicates exist)
    # ---------------------------
    # ensure prodline exists
    if "prodline" not in src.columns or "prodline" not in target.columns:
        raise KeyError("Both source and target must include 'prodline' column for matching.")

    src_index = src.set_index(["prodline", "whse_norm"], drop=False)
    # If duplicate keys exist, keep last (you can change to first if you prefer)
    src_index = src_index[~src_index.index.duplicated(keep="last")]

    # ---------------------------
    # 8. update target rows by looking up key in src_index
    # ---------------------------
    updated_count = 0
    for idx, trow in target.iterrows():
        key = (trow.get("prodline", ""), trow.get("whse_norm", ""))
        if key in src_index.index:
            srow = src_index.loc[key]
            changed = False
            for base, newcol in field_pairs:
                newval = srow.get(newcol, "")
                if newval is None:
                    newval = ""
                if str(newval).strip() != "":
                    # overwrite target's base column with newval
                    # preserve exact formatting of other fields
                    if str(target.at[idx, base]) != str(newval):
                        target.at[idx, base] = str(newval)
                        changed = True
            if changed:
                updated_count += 1

    # ---------------------------
    # 9. finalize and write output to out_folder
    # ---------------------------
    out_name = target_path.stem + "_updated" + target_path.suffix
    out_path = out_folder / out_name

    # drop helper column
    target = target.drop(columns=["whse_norm"], errors="ignore")

    # write back with the detected delimiter
    target.to_csv(out_path, index=False, sep=delim, encoding="cp1252", lineterminator="\n")

    print(f"Source: {source_csv}")
    print(f"Target (original): {target_path}")
    print(f"Updated output: {out_path}")
    print(f"Fields updated (pairs): {field_pairs}")
    print(f"Rows updated: {updated_count}")

if __name__ == "__main__":
    main()
