# whse_rank_e/pt1.py

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
from core.config import JDBC, PATHS
from core.queries import WHSE_RANK_QUERY




def main():



    # === CONFIGURATION ===
    driver_class = JDBC["class"]
    jdbc_url = JDBC["url"]
    driver_path = JDBC["jar"]
    username = JDBC["user"]
    password = JDBC["password"]
    today = datetime.now().strftime("%m%d%y")
    directory = PATHS["whse_rank_data"]
    log_path = directory / f"warehouserankE_log_{today}.txt"


    # === CONNECT ===
    conn = jaydebeapi.connect(
        driver_class,
        jdbc_url,
        [username, password],
        driver_path
    )

    # === RUN FIRST QUERY ===
    curs = conn.cursor()
    curs.execute(WHSE_RANK_QUERY)
    results1 = curs.fetchall()
    columns1 = [desc[0] for desc in curs.description]
    df_sql = pd.DataFrame(results1, columns=columns1)

    # === EXPORT PRODUCTS FROM FIRST QUERY ===
    distinct_prods = df_sql['prod'].drop_duplicates().tolist()
    comma_separated = ",".join(distinct_prods)

    with open(log_path, "w") as log:
        log.write("Comma-separated list of products to insert into SAAMM template 'SUP'ING ITEM WHSERANK TEMPLATE', run part 2 after export is saved to downloads:")
        log.write(comma_separated)

        # Get distinct warehouses
        distinct_whses = df_sql['whse'].drop_duplicates().tolist()
        comma_separated_whse = ",".join(distinct_whses)

        log.write("\nComma-separated list of warehouses:")
        log.write(comma_separated_whse)

        distinct_pairs_count = df_sql.drop_duplicates(subset=['whse', 'prod']).shape[0]
        log.write(f"Total distinct (whse, prod) pairs: {distinct_pairs_count}")

    df_sql.to_csv("prod_whse_matches_rank.csv", index=False)
    print("Saved prod/whse matches to 'prod_whse_matches.csv'; exported SAAMM criteria to log file")


    # === CLEANUP ===
    curs.close()
    conn.close()

if __name__ == "__main__":
    main()