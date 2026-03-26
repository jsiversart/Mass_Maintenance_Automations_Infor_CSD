# dnr/pt1.py

import sys
from pathlib import Path



# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import jaydebeapi
import pandas as pd
from datetime import datetime
from pathlib import Path
from core.config import PATHS, JDBC
from core.queries import DNR_SQL_1, DNR_SQL_2


def main():

    #PURPOSE - IDENTIFY NLA / SUPERSEDED PRODUCT THAT IS OUT OF STOCK IN MOST LOCATIONS AND BOTH DCS
    #CREATED- 06-10-2025 BY JULIAN SIVERS
    #LAST EDIT- 03-12-2026 BY JULIAN SIVERS


    # === CONFIGURATION ===
    today = datetime.now().strftime("%m%d%y")
    log_path = PATHS["saamms"] / "DNR" / f"DNR_log_{today}.txt"
    output_path = PATHS["saamms"] / "DNR" / f"DNR Prodline issues {today}.csv"


    # === CONNECT ===
    conn = jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
    )

    # === RUN FIRST QUERY ===
    curs = conn.cursor()
    curs.execute(DNR_SQL_1)
    results1 = curs.fetchall()
    columns1 = [desc[0] for desc in curs.description]
    df_sql = pd.DataFrame(results1, columns=columns1)

    # === EXPORT PRODUCTS FROM FIRST QUERY ===
    distinct_prods = df_sql['prod'].drop_duplicates().tolist()
    comma_separated = ",".join(distinct_prods)


    distinct_pairs_count = df_sql.drop_duplicates(subset=['whse', 'prod']).shape[0]

    df_sql.to_csv("prod_whse_matches_dnr.csv", index=False)
    print("Saved prod/whse matches to 'prod_whse_matches_dnr.csv'")

    # === RUN SECOND QUERY ===
    curs.execute(DNR_SQL_2)
    results2 = curs.fetchall()
    columns2 = [desc[0] for desc in curs.description]
    df_dnr = pd.DataFrame(results2, columns=columns2)

    # === EXPORT IF RESULTS EXIST ===

    # === WRITE LOG ===
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_path, "w") as log:
        log.write("=" * 60 + "\n")
        log.write("  DNR PROCESS PT 1 — Run Log\n")
        log.write(f"  {run_timestamp}\n")
        log.write("=" * 60 + "\n\n")

        # --- Query 1 results (LK / MISC) ---
        log.write("[ QUERY 1 — LK / MISC Prodlines ]\n")
        log.write(f"  Distinct products found : {len(distinct_prods)}\n")
        log.write(f"  Distinct (whse, prod) pairs : {distinct_pairs_count}\n\n")
        log.write("  Comma-separated product list\n")
        log.write("  (paste into SAAMM template 'dnr report template', then run Part 2):\n\n")
        log.write(f"  {comma_separated}\n\n")

        # --- Query 2 results (non-LK / non-MISC) ---
        log.write("[ QUERY 2 — Non-LK / Non-MISC Prodline Issues ]\n")
        if not df_dnr.empty:
            dnr_count = df_dnr['prod'].nunique()
            log.write(f"  Unique products with prodline issues : {dnr_count}\n")
            log.write(f"  Output saved to : {output_path}\n\n")
            df_dnr.to_csv(output_path, index=False)
        else:
            log.write("  No prodline issues found.\n\n")

        log.write("=" * 60 + "\n")
        log.write("  END OF LOG\n")
        log.write("=" * 60 + "\n")

    # === CLEANUP ===
    curs.close()
    conn.close()

    print(f"DNR query complete. Log saved to: {log_path}")

if __name__ == "__main__":
    main()
