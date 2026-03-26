# icsl_audit/pt1.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import jpype
from core.config import PATHS, JDBC
from core.etl_utils import sync_remote_to_local,  load_excel_to_sqlite, load_csv_to_sqlite
from core.queries import ICSL_COMPASS, ICSL_SQLITE
from pathlib import Path
from datetime import datetime
import pandas as pd
import sqlite3

def main():

    # ============================================================
    # PART 1: ICSL / ICSW AUDIT — DATA EXTRACTION & PREP
    # ============================================================
    #
    # PURPOSE:
    # - Pull current ICSL data from Compass (optionally)
    # - Load supporting rule/reference data into SQLite
    # - Run audit logic to identify rows requiring changes
    # - Stage results, summaries, and logs for Part 2 processing
    #
    # WHAT THIS PART DOES:
    #
    # 1. (Optional) Extract fresh ICSL data from Compass via JDBC
    #    - Controlled by RUN_COMPASS flag
    #    - Writes results into local SQLite table: icsl_audit_data
    #
    # 2. Refresh supporting rule tables in SQLite from Excel / CSV
    #    - arp path exceptions
    #    - usage control rules
    #    - warehouse metadata
    #
    # 3. Run a complex audit SQL query against SQLite that:
    #    - Calculates "current" vs "new" values for many ICSW fields
    #    - Applies business rules for order calc type, usage control,
    #      ARP paths, seasonal handling, etc.
    #    - Filters to ONLY rows where at least one value would change
    #
    # 4. Create a dated audit folder structure:
    #    - /in      → where SAAMM exports will be dropped
    #    - /out     → where updated files will be written by Part 2
    #    - /summary → audit outputs, logs, and reference artifacts
    #
    # 5. Save audit results for downstream use:
    #    - Full audit dataset → CSV (for Part 2 consumption)
    #    - Comma-delimited list of impacted product lines
    #
    # 6. Capture console output to a timestamped log file
    #
    # 7. Print a human-readable process summary with next steps
    #
    # OUTPUTS FROM PART 1:
    # - summary/icls_audit_results.csv
    # - summary/impacted_products.txt
    # - summary/icsw_audit_log_<timestamp>.txt
    #
    # NEXT STEP:
    # - User exports SAAMM file
    # - Places it into the [in] folder
    # - Runs Part 2
    # ============================================================


    # === load all ICSW audit data to purchdata ===

    RUN_COMPASS = True   # ← set to True only when you WANT to extract fresh data

    # utils

    def fix_bigints(df):
        # If JVM isn't running, there are no BigInteger objects
        try:
            if not jpype.isJVMStarted():
                return df

            biginteger = jpype.JClass("java.math.BigInteger")
            for col in df.columns:
                df[col] = df[col].apply(
                    lambda x: int(x) if isinstance(x, biginteger) else x
                )
            return df

        except Exception:
            # If anything goes wrong, return df unchanged
            return df
    # PRINT SUMMARY

    def printsummary():
        print("========================================")
        print("        ICSL AUDIT — PROCESS STEPS       ")
        print("========================================\n")
        print()
        print("STEP 1")
        print("• Create SAAMM for  using:")
        print("    [ICSL AUDIT TEMPLATE]")
        print()
        print("STEP 2")
        print("• Place SAAMM export into the 'in' subfolder")
        print("  of the dated ICSL AUDIT folder.")
        print()
        print("STEP 3")
        print("• Run Part 2 of the script.")
        print("• Once complete, load the updated mass maintenance files")
        print("  and apply updates.")
        print()
        print("========================================")


    # # -----------------------
    # # COMPASS PORTION
    # # -----------------------

    #=== Run sync ===

    if RUN_COMPASS:
        print("Running Compass extract…")
        sync_remote_to_local(
            JDBC,
            PATHS["purchdata"],
            ICSL_COMPASS,
            "icsl_audit_data",
        )
    else:
        print("Skipping Compass extract — using existing icsl_audit_data table.")


    # refresh purch_data with data maintained via spreadsheet
    with sqlite3.connect(PATHS["purchdata"]) as conn:
        load_excel_to_sqlite(
            conn,
            filepath=PATHS["icsw_maint_spreadsheet"],
            sheet_name="arppath_exceptions",
            table_name="icsw_arppath_exceptions"
        )
        load_excel_to_sqlite(
            conn,
            filepath=PATHS["icsw_maint_spreadsheet"],
            sheet_name="usagectrl_rules",
            table_name="icsw_usagectrl_rules"
        )
        load_csv_to_sqlite(
            conn,
            filepath=PATHS["warehouse_info_csv"],
            table_name="whseinfo",
            # optional CSV params:
            # sep=";", encoding="utf-8", dtype=str
        )

    # -----------------------
    # NEW ORDERED EXECUTION
    # -----------------------



    # Base directory
    monthly_base = PATHS["icsl_data"]

    today_folder = monthly_base / datetime.now().strftime("%m%d%Y")
    today_folder.mkdir(parents=True, exist_ok=True)

    # Now redefine your paths INSIDE the dated folder
    input_before_dir = today_folder / "in"
    output_after_dir = today_folder / "out"
    summary_folder = today_folder / "summary"

    # Create subfolders if needed
    input_before_dir.mkdir(parents=True, exist_ok=True)
    output_after_dir.mkdir(parents=True, exist_ok=True)
    summary_folder.mkdir(parents=True, exist_ok=True)


    with sqlite3.connect(PATHS["purchdata"]) as conn:

        # ===============================================================
        # 1. RUN QUERY DIRECTLY → GET DATAFRAME FOR PART 2
        # ===============================================================
        audit_df = pd.read_sql_query(ICSL_SQLITE, conn)

        # Fix BigInts if coming from Compass JDBC
        audit_df = fix_bigints(audit_df)

        # Save the dataframe so Part 2 can access it
        audit_df_path = summary_folder / "icls_audit_results.csv"
        audit_df.to_csv(audit_df_path, index=False)

        # ===============================================================
        # 2. COMMA-DELIMITED LIST OF IMPACTED PRODUCTS
        # ===============================================================
        # Assuming impacted products are uniquely identified by vendno + prodline + whse.
        # If you want a different identifier, tell me.
        if "prodline" in audit_df.columns:
            # unique comma-delimited list of prodlines
            impacted_list = ",".join(
                audit_df["prodline"].astype(str).unique()
            )
        else:
            impacted_list = ",".join(audit_df.index.astype(str))

        impacted_txt = summary_folder / "impacted_products.txt"
        with open(impacted_txt, "w", encoding="utf-8") as f:
            f.write(impacted_list)

        print(f"\nIMPACTED PRODUCT LINES:\n{impacted_list}\n")
        print("Saved impacted list →", impacted_txt)
        print("Saved audit dataframe for Part 2 →", audit_df_path)

    import sys
    import datetime

    #=== DUPLICATE CONSOLE OUTPUT TO TEXT FILE ===
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = summary_folder / f"icsl_audit_log_{timestamp}.txt"

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()

    log_file = open(log_path, "w", encoding="utf-8")
    sys.stdout = Tee(sys.stdout, log_file)
    sys.stderr = Tee(sys.stderr, log_file)

    printsummary()
    print("== PROCESS COMPLETE == ONCE EXPORT IN [IN] SUBFOLDER, PROCEED TO PART 2 ==")


if __name__ == "__main__":
    main()
