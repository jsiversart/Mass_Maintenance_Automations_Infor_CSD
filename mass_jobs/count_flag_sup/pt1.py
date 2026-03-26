#count_flag_sup/pt1.py

import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import jaydebeapi
import pandas as pd
from pathlib import Path
from tempfile import NamedTemporaryFile
from core.notifier import send_email
from core.config import JDBC, EMAILS
from core.queries import COUNT_FLAG_QUERY



# === OUTPUTS ===
CSV_OUTPUT = "prod_whse_matches_count_flag.csv"
EMAIL_SUBJECT = "COUNT FLAG SAAMM"
RECIPIENTS = EMAILS["mass_maint_user"]



def run_query_and_email():
    print("🔗 Connecting to JDBC database...")

    conn = jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
    )

    # === RUN QUERY ===
    df = pd.read_sql_query(COUNT_FLAG_QUERY, conn)
    conn.close()

    print(f"Retrieved {len(df)} rows.")

    # === CLEAN + NORMALIZE ===
    df["prod"] = df["prod"].astype(str)
    df["whse"] = df["whse"].astype(str).str.zfill(2)
    df["key"] = df["prod"] + "_" + df["whse"]

    # === SAVE CSV FOR PART 2 ===
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"Saved CSV → {CSV_OUTPUT}")

    # === BUILD DISTINCT PROD + WHSE LISTS FOR EMAIL TEXT FILE ===
    distinct_prods = sorted(df["prod"].astype(str).unique())
    distinct_whses = sorted(df["whse"].astype(str).unique())

    prod_list = ",".join(distinct_prods)
    whse_list = ",".join(distinct_whses)

    # === WRITE TEMP TEXT FILE (distinct values only) ===
    tmp = NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8")

    tmp.write("### DISTINCT PROD LIST ###\n")
    tmp.write(prod_list if prod_list else "(no prod results)")
    tmp.write("\n\n")

    tmp.write("### DISTINCT WHSE LIST ###\n")
    tmp.write(whse_list if whse_list else "(no whse results)")
    tmp.write("\n\n")
    
    tmp.close()

    # === Email text ===
    email_body = (
        "Attached are the values identified to update count flag via SAAMM.  Save SAAMM export to P:\Analyst Files\Working directory\Mass Maintenance Automations.\n\n"
        "The CSV used by the Mass Maint Process is also saved locally.\n\n"
        "— Automation Bot"
    )

    # === SEND EMAIL ===
    send_email(
        subject=EMAIL_SUBJECT,
        body=email_body,
        to_addrs=RECIPIENTS,
        attachments=[tmp.name]
    )

    print(f"📧 Email sent with attachment: {tmp.name}")


if __name__ == "__main__":
    run_query_and_email()
