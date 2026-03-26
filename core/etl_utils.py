# etl_utils.py
# --- Ensure repo root is in Python path ---

import sys
from pathlib import Path

# Automatically find the repo root (assumes 'core' folder is in the root)
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

import jaydebeapi
import sqlite3
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import csv
from typing import List

def refresh_csdpdata(
    sqlite_db_path,
    jdbc,
    sql_query,
    max_age_hours=None,
    force=False
) -> bool:
    """
    Returns True if refresh occurred, False if skipped.
    """
    """
    Rebuilds the csdpdata table from the remote DB
    and tags each row with a load timestamp.
    """
    last_ts = None

    def get_last_load_ts(sqlite_db_path):
        try:
            conn = sqlite3.connect(sqlite_db_path)
            cur = conn.cursor()
            cur.execute("SELECT MAX(load_ts) FROM csdpdata")
            row = cur.fetchone()
            return row[0] if row and row[0] else None
        except sqlite3.Error:
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if not force and max_age_hours is not None:

        last_ts = get_last_load_ts(sqlite_db_path)

    if last_ts:
        last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
        age = datetime.now() - last_dt

        if age < timedelta(hours=max_age_hours):
            print(
                f"csdpdata is up to date "
                f"(last load {age.total_seconds()/3600:.2f} hours ago)"
            )
            return False   # nothing done

    # === Required JDBC creds ===

    REQUIRED_JDBC_KEYS = {"class", "url", "user", "password", "jar"}

    missing = REQUIRED_JDBC_KEYS - jdbc.keys()
    if missing:
            raise ValueError(f"Missing JDBC config keys: {missing}")



    # === Step 1: Connect to remote DB via JDBC ===
    conn_remote = jaydebeapi.connect(
        jdbc["class"],
        jdbc["url"],
        [jdbc["user"], jdbc["password"]],
        jdbc["jar"]
    )
    cursor_remote = conn_remote.cursor()

    # === Step 2: Read and run SQL query ===

    cursor_remote.execute(sql_query)
    results = cursor_remote.fetchall()
    columns = [desc[0] for desc in cursor_remote.description]

    # === Step 3: Connect to SQLite and prepare csdpdata table ===
    conn_sqlite = sqlite3.connect(sqlite_db_path)
    cursor_sqlite = conn_sqlite.cursor()

    # Drop existing csdpdata table if it exists
    cursor_sqlite.execute("DROP TABLE IF EXISTS csdpdata")

    # Dynamically create csdpdata table based on remote result columns
    load_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
    column_defs += ', "load_ts" TEXT'
    create_table_sql = f'CREATE TABLE csdpdata ({column_defs})'
    cursor_sqlite.execute(create_table_sql)

    # Insert all rows into csdpdata
    insert_sql = f'''
    INSERT INTO csdpdata (
        {", ".join(columns)}, load_ts
    ) VALUES (
        {", ".join(["?"] * len(columns))}, ?
    )
    '''
    results_with_ts = [row + (load_ts,) for row in results]
    cursor_sqlite.executemany(insert_sql, results_with_ts)

    conn_sqlite.commit()

    # Index maintenance

    indexes = [
        'CREATE INDEX IF NOT EXISTS idx_csdpdata_prod ON csdpdata("prod")',
        'CREATE INDEX IF NOT EXISTS idx_csdpdata_lookup_prod ON csdpdata("lookup_prod")',
        'CREATE INDEX IF NOT EXISTS idx_csdpdata_arpvendno ON csdpdata("arpvendno")',
        'CREATE INDEX IF NOT EXISTS idx_csdpdata_partition ON csdpdata("prod","arpvendno")'
    ]

    for sql in indexes:
        cursor_sqlite.execute(sql)

    conn_sqlite.commit()

    #  End index maintenance


    # === Step 4: Cleanup ===
    cursor_remote.close()
    conn_remote.close()
    cursor_sqlite.close()
    conn_sqlite.close()

    print("Query executed and csdpdata table updated in SQLite.")
    return True  # refresh occurred


def export_stndcost_flag_csv(df, output_folder, flag_value=1):
    """
    Export stndcost flag CSV used by data conversion.
    Columns kept as-is; adds/overwrites 'stndcost' with flag_value.
    """
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%y%m%d")
    file_path = output_folder / f"sc{timestamp}.csv"

    df_out = df.copy()
    df_out["stndcost"] = flag_value

    df_out.to_csv(file_path, index=False, encoding="utf-8")
    print(f"📄 Exported stndcost flag CSV ({len(df_out):,} rows): {file_path}")
    return file_path

def summarize_chunk_updates(dataname, df, chunk_limit=29000):
    import datetime

    # Global summary
    total_records = len(df)
    distinct_prods_all = list(df["prod"].dropna().unique())
    distinct_whse_all = list(df["whse"].dropna().unique())
    pair_count_all = df[["whse", "prod"]].drop_duplicates().shape[0]

    # --------------------------------------
    # CHUNK DF BASED ON PRODUCT LIST STRING
    # --------------------------------------
    chunks = []
    current_prod_list = []
    current_char_length = 0

    # iterate products in sorted order for deterministic output
    for prod in sorted(distinct_prods_all, key=str):
        prod_str = str(prod)

        # Length if this is added (include comma if list not empty)
        added_length = len(prod_str) + (1 if current_prod_list else 0)

        # If adding exceeds chunk limit → start new chunk
        if current_char_length + added_length > chunk_limit:
            chunks.append(current_prod_list)
            current_prod_list = [prod_str]
            current_char_length = len(prod_str)
        else:
            current_prod_list.append(prod_str)
            current_char_length += added_length

    # append last chunk
    if current_prod_list:
        chunks.append(current_prod_list)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # --------------------------------------
    # BUILD PER-CHUNK SUMMARIES
    # --------------------------------------
    chunk_details = []

    for i, prod_list in enumerate(chunks, start=1):

        # slice DF by product
        chunk_df = df[df["prod"].isin(prod_list)]

        # distinct lists
        whse_list = sorted(chunk_df["whse"].dropna().unique(), key=str)

        chunk_info = {
            "chunk_number": i,
            "record_count": len(chunk_df),
            "prod_count": len(prod_list),
            "whse_count": len(whse_list),
            "prod_list_csv": ",".join(prod_list),
            "whse_list_csv": ",".join(map(str, whse_list)),
            "chunk_df": chunk_df,
        }
        chunk_details.append(chunk_info)

    # --------------------------------------
    # Print high-level summary
    # --------------------------------------
    summary_lines = [
        f"=== {dataname} SUMMARY ({timestamp}) ===",
        f"Total records: {total_records:,}",
        f"Distinct products: {len(distinct_prods_all):,}",
        f"Distinct warehouses: {len(distinct_whse_all):,}",
        f"Distinct whse/prod pairs: {pair_count_all:,}",
        f"Chunk count: {len(chunk_details)}",
    ]

    print("\n" + "\n".join(summary_lines))

    return {
        "timestamp": timestamp,
        "overall": {
            "total_records": total_records,
            "distinct_products": distinct_prods_all,
            "distinct_warehouses": distinct_whse_all,
            "pair_count": pair_count_all,
        },
        "chunks": chunk_details,
        "summary_text": "\n".join(summary_lines),
    }

import datetime
from pathlib import Path

def export_chunk_summary(chunk_info, output_dir: Path, prefix="chunk"):
    """
    chunk_info is a dict created by summarize_chunk_updates(), with keys:
      - chunk_number (int)
      - chunk_df (DataFrame slice)
      - prod_list_csv (comma-delimited, no spaces) OR you can build it here
      - whse_list_csv (comma-delimited, no spaces)
    This writes two files for each chunk:
      - chunk_XX_summary.txt  (human readable single-file summary)
      - chunk_XX.csv          (the actual rows for that chunk)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    num = chunk_info["chunk_number"]
    chunk_df = chunk_info["chunk_df"]

    # Build distinct lists (defensive: recompute here to be sure)
    distinct_prods = sorted(chunk_df["prod"].dropna().astype(str).unique(), key=str)
    distinct_whse = sorted(chunk_df["whse"].dropna().astype(str).unique(), key=str)

    # CSV strings with NO spaces
    prod_csv = ",".join(distinct_prods)
    whse_csv = ",".join(distinct_whse)

    # Distinct prod/whse pair count
    pair_count = (
        chunk_df[["prod", "whse"]]
        .dropna()
        .drop_duplicates()
        .shape[0]
    )

    record_count = len(chunk_df)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write the CSV of rows
    csv_fname = output_dir / f"{prefix}_{num:02d}_rows_{ts}.csv"
    chunk_df.to_csv(csv_fname, index=False)

    # Write the summary text file — formatted exactly as requested
    summary_fname = output_dir / f"{prefix}_{num:02d}_summary_{ts}.txt"
    with open(summary_fname, "w", encoding="utf-8") as fh:
        fh.write(f"Record_count:{record_count}\n")
        fh.write(f"ProdWhse_pairs:{pair_count}\n")
        fh.write(f"Products:{prod_csv}\n")
        fh.write(f"Warehouses:{whse_csv}\n")

    print(f"📦 Chunk {num:02d} exported: rows -> {csv_fname.name}, summary -> {summary_fname.name}")

    # Return filenames in case caller needs them
    return {"csv": csv_fname, "summary": summary_fname}



def export_non_776_stndcost_csv(df, output_folder):
    """
    Output raw SQL results as: non_776_prod_w_stndcost_MMDDYY.csv
    """
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    date_str = datetime.datetime.now().strftime("%m%d%y")
    file_path = output_folder / f"non_776_prod_w_stndcost_{date_str}.csv"

    df.to_csv(file_path, index=False, encoding="utf-8")
    print(f"📄 Exported non-776 stndcost CSV ({len(df):,} rows): {file_path}")
    return file_path

def summarize_df_updates(dataname, df):
    total_records = len(df)
    distinct_prods = sorted(df["prod"].dropna().unique())
    distinct_whse = sorted(df["whse"].dropna().unique())
    prod_count = len(distinct_prods)
    whse_count = len(distinct_whse)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    summary_lines = [
        f"=== {dataname} SUMMARY ({timestamp}) ===",
        f"Total records: {total_records:,}",
        f"Distinct products: {prod_count:,}",
        f"Distinct warehouses: {','.join(map(str, distinct_whse))}",
    ]

    # ---- FIXED: join list into a single printable string ----
    summary_text = "\n".join(summary_lines)
    print("\n" + summary_text)

    # ---- Keep original return structure exactly as before ----
    return (
        total_records,
        distinct_prods,
        distinct_whse,
        prod_count,
        timestamp,
        summary_text,
    )

def run_local_audit_sql_extended(
        conn,
        sql_chunked,
        sql_stndcost,
        sql_non_776_stndcost,
        summary_output_dir=None,
    ):
    """
    Run three queries and return three dataframes:
      - df_chunked: items that must be processed using chunking
      - df_stndcost_flag: items that can be tagged using stndcost=1/0 method
      - df_non_776: raw dump of non-776 parts with stndcost
    """

    # ============================================================
    # 1. STNDCOST FLAG ITEMS
    # ============================================================
    print("\n▶️ Fetching records to tag via stndcost...")
    df_stndcost_flag = pd.read_sql_query(sql_stndcost, conn)

    if df_stndcost_flag.empty:
        print("⚠️ No stndcost flag items returned.")
    else:
        summarize_df_updates("standard cost tag", df_stndcost_flag)
        if summary_output_dir:
            export_stndcost_flag_csv(df_stndcost_flag, summary_output_dir)

    # ============================================================
    # 2. CHUNKED LIST ITEMS
    # ============================================================
    print("\n▶️ Fetching non-standard-cost (chunked) update items...")
    df_chunked = pd.read_sql_query(sql_chunked, conn)

    if df_chunked.empty:
        print("⚠️ No chunked records returned.")
        chunk_details = []
    else:
        print("✔️ Building chunk summaries...")
        
        # *** This is the NEW integration ***
        chunk_summary = summarize_chunk_updates("update via product list", df_chunked)
        chunk_details = chunk_summary["chunks"]
        
        if summary_output_dir:
            outdir = Path(summary_output_dir)
            outdir.mkdir(parents=True, exist_ok=True)

            exported_files = []
            for chunk in chunk_details:
                # chunk is the dict produced earlier by summarize_chunk_updates()
                exported = export_chunk_summary(chunk, outdir, prefix="chunk")
                exported_files.append(exported)

            # optional: export an index file listing all chunk summaries
            # index_path = outdir / f"chunks_index_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            # with open(index_path, "w", encoding="utf-8") as ix:
            #     for ef in exported_files:
            #         ix.write(f"{ef['csv'].name}\t{ef['summary'].name}\n")
            # print(f"✔️ Written index: {index_path.name}")


    # ============================================================
    # 3. NON-776 STNDCOST RAW EXPORT
    # ============================================================
    print("\n▶️ Fetching NON-776 products with stndcost...")
    df_non_776 = pd.read_sql_query(sql_non_776_stndcost, conn)

    if df_non_776.empty:
        print("No non-776 items with existing stndcost returned.")
    else:
        print("Non-776 items with existing stndcost returned.  Send resulting CSV to Caleb.")
        if summary_output_dir:
            export_non_776_stndcost_csv(df_non_776, summary_output_dir)

    # ============================================================
    # RETURN ALL 3
    # ============================================================
    return df_chunked, df_stndcost_flag, df_non_776,  chunk_details


def sync_remote_to_local(JDBC, sqlite_db_path, query, table_name, index_cols=None):
    """
    Connects to remote DB via JDBC, runs a query, and loads results into a local SQLite table.
    Converts java.math.BigInteger → Python int so SQLite can store it.
    """
    import jpype
    # Start JVM if not already running
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JDBC["jar"]])

    # Java BigInteger class for type checking
    BigInteger = jpype.JClass("java.math.BigInteger")

    def normalize(value):
        """Convert JDBC values → Python primitives safe for SQLite."""
        try:
            # Convert BigInteger or any Java number to Python int
            if hasattr(value, 'longValue'):
                return int(value.longValue())

            # Convert anything Java-ish (strings, timestamps, etc.)
            if hasattr(value, 'toString'):
                return str(value.toString())

        except Exception:
            pass

        return value


    conn_remote = conn_sqlite = None

    try:
        print(f"🔌 Connecting to remote DB for {table_name}...")

        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )

        cursor_remote = conn_remote.cursor()

        print("▶️ Running remote query...")
        cursor_remote.execute(query)

        raw_results = cursor_remote.fetchall()
        columns = [str(desc[0]) for desc in cursor_remote.description]

        if not raw_results:
            print(f"⚠️ No data returned for {table_name}.")
            return

        # 🔧 Normalize EACH row and EACH column value
        results = [
            tuple(normalize(v) for v in row)
            for row in raw_results
        ]

        # Connect to SQLite and create/replace table
        os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
        conn_sqlite = sqlite3.connect(sqlite_db_path)
        cursor_sqlite = conn_sqlite.cursor()

        cursor_sqlite.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create table with TEXT columns (same as your original code)
        column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
        cursor_sqlite.execute(f"CREATE TABLE {table_name} ({column_defs})")

        # Insert rows
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'

        cursor_sqlite.executemany(insert_sql, results)
        conn_sqlite.commit()

        print(f"✅ {table_name} updated with {len(results):,} rows")

        # Optional index creation
        if index_cols:
            for col in index_cols:
                idx_name = f"idx_{table_name}_{col}"
                cursor_sqlite.execute(
                    f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}("{col}")'
                )
            conn_sqlite.commit()
            print(f"🧱 Indexes created on {', '.join(index_cols)}")

    finally:
        if conn_remote:
            conn_remote.close()
        if conn_sqlite:
            conn_sqlite.close()

def sync_remote_to_local_v2(JDBC, sqlite_db_path, query, table_name, index_cols=None):
    import jpype, jaydebeapi, sqlite3, os

    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JDBC["jar"]])

    BigInteger = jpype.JClass("java.math.BigInteger")

    def normalize(value):
        try:
            if hasattr(value, 'longValue'):
                return int(value.longValue())
            if hasattr(value, 'toString'):
                return str(value.toString())
        except Exception:
            pass
        return value

    conn_remote = conn_sqlite = None

    try:
        print(f"🔌 Connecting to remote DB for {table_name}...")
        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )

        cursor_remote = conn_remote.cursor()
        print("▶️ Running remote query...")
        cursor_remote.execute(query)

        columns = [str(desc[0]) for desc in cursor_remote.description]

        os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
        conn_sqlite = sqlite3.connect(sqlite_db_path)
        cursor_sqlite = conn_sqlite.cursor()

        # 🚀 Speed pragmas
        cursor_sqlite.execute("PRAGMA journal_mode = WAL;")
        cursor_sqlite.execute("PRAGMA synchronous = OFF;")
        cursor_sqlite.execute("PRAGMA temp_store = MEMORY;")

        cursor_sqlite.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        column_defs = ", ".join([f'"{col}" TEXT' for col in columns])
        cursor_sqlite.execute(f'CREATE TABLE "{table_name}" ({column_defs})')

        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f'''
            INSERT INTO "{table_name}"
            ({", ".join(columns)})
            VALUES ({placeholders})
        '''

        BATCH_SIZE = 50_000
        batch = []
        total = 0

        print("📥 Streaming rows into SQLite...")
        while True:
            rows = cursor_remote.fetchmany(10_000)
            if not rows:
                break

            for row in rows:
                    batch.append(tuple(normalize(v) for v in row))
                    if len(batch) >= BATCH_SIZE:
                        cursor_sqlite.executemany(insert_sql, batch)
                        conn_sqlite.commit()
                        total += len(batch)
                        print(f"  → {total:,} rows loaded")
                        batch.clear()

        if batch:
            cursor_sqlite.executemany(insert_sql, batch)
            conn_sqlite.commit()
            total += len(batch)

        print(f"✅ {table_name} updated with {total:,} rows")

        if index_cols:
            for col in index_cols:
                cursor_sqlite.execute(
                    f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}'
                    f' ON "{table_name}"("{col}")'
                )
            conn_sqlite.commit()

    finally:
        if conn_remote:
            conn_remote.close()
        if conn_sqlite:
            conn_sqlite.close()




def table_has_rows(conn, table_name):
    """Check if a table exists and has at least one row."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0] > 0
    except Exception:
        return False


def validate_table_not_empty(conn, table_name):
    """Abort if a table is missing or empty."""
    if not table_has_rows(conn, table_name):
        raise SystemExit(f"❌ ERROR: {table_name} missing/empty. Aborting.")


def _load_dataframe_to_sqlite(conn, df, table_name):
    """Helper to write dataframe to SQLite (replace if exists)."""
    if df is None or df.empty:
        print(f"⚠️ {table_name} is empty — skipping load.")
        return
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()
    print(f"✅ {table_name} table updated ({len(df):,} rows).")
    validate_table_not_empty(conn, table_name)


def load_excel_to_sqlite(conn, filepath, sheet_name, table_name):
    """Load data from an Excel sheet into a SQLite table (replace if exists)."""
    try:
        if not os.path.exists(filepath):
            print(f"⚠️ File not found: {filepath}")
            return

        df = pd.read_excel(filepath, sheet_name=sheet_name)
        _load_dataframe_to_sqlite(conn, df, table_name)

    except Exception as e:
        print(f"⚠️ Error loading {table_name} from Excel: {e}")


def load_csv_to_sqlite(conn, filepath, table_name, **read_csv_kwargs):
    """Load data from a CSV file into a SQLite table (replace if exists)."""
    try:
        if not os.path.exists(filepath):
            print(f"⚠️ File not found: {filepath}")
            return

        df = pd.read_csv(filepath, **read_csv_kwargs)
        _load_dataframe_to_sqlite(conn, df, table_name)

    except Exception as e:
        print(f"⚠️ Error loading {table_name} from CSV: {e}")

import datetime
from pathlib import Path

# wip below?

def run_local_audit_sql(conn, sql, summary_output_dir=None):
    """
    Run a hardcoded SQL query against the local SQLite DB, summarize results,
    print the summary, and write it to a timestamped text file.
    """
    print("▶️ Running local audit SQL...")
    df = pd.read_sql_query(sql, conn)

    if df.empty:
        print("⚠️ No records returned by query.")
        return None

    total_records = len(df)
    distinct_prods = sorted(df["prod"].dropna().unique())
    distinct_whse = sorted(df["whse"].dropna().unique())
    prod_count = len(distinct_prods)
    whse_count = len(distinct_whse)
        # --- Step 1: determine dynamic chunking rules ---
    MAX_PAYLOAD_CHARS = 29000   # safe ceiling for CSD’s XML/JSON limit
    BASE_CHUNK_SIZE = max(1, int(10000 / max(1, whse_count)))  # still caps runaway groups

    def chunk_products(products, max_chars=MAX_PAYLOAD_CHARS, max_items=BASE_CHUNK_SIZE * 5):
        """
        Split product list into chunks limited by both total character length
        and a rough max item count safeguard.
        """
        chunks = []
        current_chunk = []
        current_length = 0

        for p in products:
            add_len = len(p) + 1  # include comma separator
            if (current_length + add_len > max_chars or len(current_chunk) >= max_items) and current_chunk:
                chunks.append(current_chunk)
                current_chunk = [p]
                current_length = add_len
            else:
                current_chunk.append(p)
                current_length += add_len

        if current_chunk:
            chunks.append(current_chunk)
        return chunks

    # --- Step 2: build chunks ---
    prod_chunks = chunk_products(distinct_prods)
            
    # --- Build the summary text ---
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    summary_lines = [
        f"=== LOCAL AUDIT SQL SUMMARY ({timestamp}) ===",
        f"Total records: {total_records:,}",
        f"Distinct products: {prod_count:,}",
        f"Distinct warehouses: {','.join(map(str, distinct_whse))}",
        f"Number of product groups: {len(prod_chunks)} "
        f"(split by max payload length of ~{MAX_PAYLOAD_CHARS} chars)",
        "",
        "Full product list by group (with estimated update counts):"
        ]


    for idx, chunk in enumerate(prod_chunks, start=1):
        group_df = df[df["prod"].isin(chunk)]
        update_count = len(group_df)
        summary_lines.append(f"\n-- Group {idx} ({len(chunk)} products, ~{update_count:,} updates) --")
        summary_lines.append(",".join(chunk))

    summary_text = "\n".join(summary_lines)

    # --- Print to console ---
    print("\n" + summary_text) 

    # --- Save to file ---
    summary_dir = Path(summary_output_dir) if summary_output_dir else Path.cwd()
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_file = summary_dir / f"local_audit_summary_{timestamp}.txt"

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(summary_text)

    print(f"\n📝 Summary written to: {summary_file}")

    return df


def apply_sql_results_to_textfiles_OLDEST_VERSION(
    df_updates,
    input_dir,
    output_dir=None,
    key_cols=["prod", "whse"]
):
    """
    Apply updates from df_updates (SQL results) to all text files in input_dir.
    Match on key_cols (e.g. prod + whse), normalize whse to 2 digits,
    and prioritize 'new_' columns (e.g. new_value overwrites value).
    """

    input_dir = Path(input_dir)
    if not input_dir.exists():
        print(f"❌ Input folder not found: {input_dir}")
        return

    output_dir = Path(output_dir) if output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    files = list(input_dir.glob("*.txt"))
    if not files:
        print(f"⚠️ No text files found in {input_dir}")
        return

    print(f"🧾 Found {len(files)} text files to process.\n")

    # --- Normalize df_updates keys and handle "new_" columns ---
    df_updates = df_updates.copy()

    # Standardize warehouse format (e.g. 1 -> '01', 25 -> '25')
    if "whse" in df_updates.columns:
        df_updates["whse"] = (
            df_updates["whse"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("0")
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )

    # Standardize prod (uppercase + trim)
    if "prod" in df_updates.columns:
        df_updates["prod"] = df_updates["prod"].astype(str).str.strip().str.upper()

    # Handle "new_" column priority
    for col in df_updates.columns:
        if col.startswith("new_"):
            base_col = col[4:]
            if base_col in df_updates.columns:
                df_updates[base_col] = df_updates[col].combine_first(df_updates[base_col])
            else:
                df_updates[base_col] = df_updates[col]

    print(f"✅ Normalized SQL results for merge ({len(df_updates)} rows)\n")

    for file_path in files:
        print(f"🔍 Processing: {file_path.name}")

        try:
            df_file = pd.read_csv(
                file_path,
                dtype=str,
                sep="\t",           # 👈 explicitly tab-delimited
                engine="python",    # more forgiving with weird quotes or spacing
                on_bad_lines="skip" # skip malformed lines gracefully
            )
        except Exception as e:
            print(f"⚠️ Skipping {file_path.name}: {e}")
            continue

        # Normalize key columns in the text file
        if "whse" in df_file.columns:
            df_file["whse"] = (
                df_file["whse"]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
                .fillna("0")
                .astype(int)
                .astype(str)
                .str.zfill(2)
            )
        if "prod" in df_file.columns:
            df_file["prod"] = df_file["prod"].astype(str).str.strip().str.upper()

        # Merge SQL updates into text file
        df_merged = pd.merge(
            df_file,
            df_updates,
            on=key_cols,
            how="left",
            suffixes=("", "_new")
        )

        # Collect update stats
        updated_cols = [c for c in df_updates.columns if c not in key_cols]
        update_count = 0

        for col in updated_cols:
            new_col = f"{col}_new"
            if new_col in df_merged.columns:
                mask = df_merged[new_col].apply(lambda x: not (pd.isna(x) or x is None))
                df_merged.loc[mask, col] = df_merged.loc[mask, new_col]
                update_count += mask.sum()
                df_merged.drop(columns=new_col, inplace=True, errors="ignore")

        output_path = output_dir / file_path.name
        # Ensure column order and clean values
        # --- Clean & normalize for Infor import ---
        df_merged.columns = df_merged.columns.str.strip()      # strip weird spaces in headers
        df_merged = df_merged.fillna("")                       # preserve empty tabs, avoid NaN
        df_merged["whse"] = df_merged["whse"].astype(str).str.zfill(2)  # ensure 2-digit warehouse

        if set(df_file.columns).issubset(df_merged.columns):
            df_merged = df_merged[df_file.columns]

        df_merged.to_csv(
            output_path,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            quoting=csv.QUOTE_NONE,       # ❗ No quotes at all
            escapechar="\\"     
        )                                   
        print(f"✅ Updated {update_count:,} fields in {file_path.name}")

    print(f"\n🎉 All files processed successfully.\nOutput folder: {output_dir}")

import pandas as pd
import csv
from pathlib import Path

def apply_sql_results_to_textfiles_OLD_VERSION(
    df_updates,
    input_dir,
    output_dir=None,
    key_cols=["prod", "whse"]
):
    """
    Apply updates from df_updates (SQL results) to all text files in input_dir.
    Match on key_cols (e.g. prod + whse), normalize whse to 2 digits,
    and prioritize 'new_' columns (e.g. new_value overwrites value).
    Match on uppercase PROD but retain original case in output.
    Preserve blank tab fields and backslashes.
    """

    import pandas as pd
    import csv
    from pathlib import Path

    input_dir = Path(input_dir)
    if not input_dir.exists():
        print(f"❌ Input folder not found: {input_dir}")
        return

    output_dir = Path(output_dir) if output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    files = list(input_dir.glob("*.txt"))
    if not files:
        print(f"⚠️ No text files found in {input_dir}")
        return

    print(f"🧾 Found {len(files)} text files to process.\n")

    # --- Normalize df_updates keys and handle "new_" columns ---
    df_updates = df_updates.copy()

    # Normalize whse to 2 digits
    if "whse" in df_updates.columns:
        df_updates["whse"] = (
            df_updates["whse"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("0")
            .astype(int)
            .astype(str)
            .str.zfill(2)
        )

    # Create uppercase merge key (preserves original prod)
    if "prod" in df_updates.columns:
        df_updates["_prod_upper"] = df_updates["prod"].astype(str).str.strip().str.upper()

    # Handle "new_" column priority
    for col in df_updates.columns:
        if col.startswith("new_"):
            base_col = col[4:]
            if base_col in df_updates.columns:
                df_updates[base_col] = df_updates[col].combine_first(df_updates[base_col])
            else:
                df_updates[base_col] = df_updates[col]

    print(f"✅ Normalized SQL results for merge ({len(df_updates)} rows)\n")

    for file_path in files:
        print(f"🔍 Processing: {file_path.name}")

        try:
            df_file = pd.read_csv(
                file_path,
                dtype=str,
                sep="\t",
                engine="python",
                keep_default_na=False,   # keeps blanks as ""
                na_values=[],
                on_bad_lines="skip"
            )
        except Exception as e:
            print(f"⚠️ Skipping {file_path.name}: {e}")
            continue

        # Normalize keys in file (keep original prod for output)
        if "whse" in df_file.columns:
            df_file["whse"] = (
                df_file["whse"]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
                .fillna("0")
                .astype(int)
                .astype(str)
                .str.zfill(2)
            )

        if "prod" in df_file.columns:
            df_file["_prod_upper"] = df_file["prod"].astype(str).str.strip().str.upper()

        # --- Merge on uppercase prod + normalized whse ---
        df_merged = pd.merge(
            df_file,
            df_updates,
            on=["_prod_upper", "whse"],
            how="left",
            suffixes=("", "_new")
        )

        # Drop merge helper
        df_merged.drop(columns=["_prod_upper"], inplace=True, errors="ignore")

        # Collect update stats
        updated_cols = [c for c in df_updates.columns if c not in key_cols and not c.startswith("_")]
        update_count = 0

        for col in updated_cols:
            new_col = f"{col}_new"
            if new_col in df_merged.columns:
                mask = df_merged[new_col].apply(lambda x: str(x).strip() not in ("", "nan", "None"))
                df_merged.loc[mask, col] = df_merged.loc[mask, new_col]
                update_count += mask.sum()
                df_merged.drop(columns=new_col, inplace=True, errors="ignore")

        # --- Final cleanup ---
        df_merged = df_merged.fillna("")  # ensure blank stays blank
        df_merged["whse"] = df_merged["whse"].astype(str).str.zfill(2)

        # Force PROD to keep original from file, but clean up "\" -> "//"
        if "prod" in df_merged.columns:
            df_merged["prod"] = (
                df_merged["prod"]
                .astype(str)
                .str.replace("\\", "//", regex=False)
                .str.strip()
            )

        # Keep original column order
        if set(df_file.columns).issubset(df_merged.columns):
            df_merged = df_merged[df_file.columns]

        # --- Export ---
        output_path = output_dir / file_path.name
        expected_cols = ["extractseqno", "prod", "whse", "source-desc-name", "arptype", "arpwhse",
            "class", "frozenltty", "frozenmmyy", "frozenmos", "frozentype",
            "leadtmavg", "ordcalcty", "pricetype", "safealldays", "safeallty",
            "statustype", "usagectrl", "usgmths", "usmthsfrzfl", "rowpointer"]

        # Keep only the expected columns that exist
        df_merged = df_merged[[c for c in expected_cols if c in df_merged.columns]]
        df_merged.to_csv(
            output_path,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep=""
        )

        print(f"✅ Updated {update_count:,} fields in {file_path.name}")

    print(f"\n🎉 All files processed successfully.\nOutput folder: {output_dir}")

def remote_to_csv(
    title,
    JDBC,
    query,
    table_name,
    output_folder,
    log_items=None,
    params=None
):
    """
    Connects to a remote DB via JDBC, runs a query, writes results to CSV,
    and logs specified info to both console and a text file.

    Params:
        JDBC: JDBC connection dict {class, url, user, password, jar}
        query: SQL query to run
        table_name: logical name for the extract (used for filenames)
        output_folder: directory to save CSV + log file
        log_items: list of (label, value) tuples to add to log and console
    """
    import os
    import csv
    import jaydebeapi
    from datetime import datetime

    os.makedirs(output_folder, exist_ok=True)

    # Output paths
    csv_path = os.path.join(output_folder, f"{title}.csv")
    log_path = os.path.join(output_folder, f"{title}_log.txt")

    log_lines = []

    def log(msg):
        """Write to console and buffer the message for log file."""
        print(msg)
        log_lines.append(msg)

    conn_remote = None

    try:
        log(f"🔌 Connecting to remote DB for '{table_name}'...")
        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )
        cursor = conn_remote.cursor()

        log("▶️ Running remote query...")
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            log(f"⚠️ No data returned for {table_name}.")
            return

        # --- Write CSV ---
        log("💾 Writing CSV...")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(results)

        log(f"✅ CSV written: {csv_path}")
        log(f"📦 Rows exported: {len(results):,}")

        # Additional user-specified metadata
        if log_items:
            log("📄 Additional info:")
            for label, value in log_items:
                log(f"   - {label}: {value}")

    except Exception as e:
        log(f"❌ Error processing {title}: {e}")
        raise

    finally:
        try:
            if conn_remote:
                conn_remote.close()
        except:
            pass

        log(f"🔒 Remote connection closed for {title}")

        # --- Write log file ---
        with open(log_path, "w", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"Log created {ts}\n\n")
            for line in log_lines:
                f.write(line + "\n")

        print(f"📝 Log written to: {log_path}")
        print("Done.")
        return csv_path
    
def sqlite_to_file(
    title,
    db_path,
    query,
    table_name,
    output_folder,
    log_items=None,
    params=None,
    output_type="csv"   
):
    """
    Connects to a local SQLite DB, runs a query, writes results to CSV or Excel,
    and logs specified info to both console and a text file.

    Params:
        db_path: path to .db / .sqlite file
        query: SQL query to run
        table_name: logical name for the extract (used for filenames)
        output_folder: directory to save output + log file
        log_items: list of (label, value) tuples to add to log and console
        output_type: "csv" or "excel"
    """
    import os
    import csv
    import sqlite3
    from datetime import datetime

    os.makedirs(output_folder, exist_ok=True)

    # Determine output path
    if output_type == "xlsx":
        file_path = os.path.join(output_folder, f"{title}.xlsx")
    else:
        file_path = os.path.join(output_folder, f"{title}.csv")

    log_path = os.path.join(output_folder, f"{title}_log.txt")

    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    conn = None

    try:
        log(f"🔌 Connecting to SQLite DB for '{table_name}'...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        log("▶️ Running query...")
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            log(f"⚠️ No data returned for {table_name}.")
            return

        # --- Write Output ---
        log(f"💾 Writing {output_type.upper()}...")

        if output_type == "xlsx":
            from openpyxl import Workbook

            wb = Workbook()
            ws = wb.active
            ws.title = "Data"

            ws.append(columns)
            for row in results:
                ws.append(row)

            # --- Excel usability enhancements ---
            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes = "A2"

            wb.save(file_path)

        else:  # default CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(results)

        log(f"✅ File written: {file_path}")
        log(f"📦 Rows exported: {len(results):,}")

        # Additional user-specified metadata
        if log_items:
            log("📄 Additional info:")
            for label, value in log_items:
                log(f"   - {label}: {value}")

    except Exception as e:
        log(f"❌ Error processing {title}: {e}")
        raise

    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

        log(f"🔒 SQLite connection closed for {table_name}")

        # --- Write log file ---
        with open(log_path, "w", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"Log created {ts}\n\n")
            for line in log_lines:
                f.write(line + "\n")

        print(f"📝 Log written to: {log_path}")
        print("Done.")
        return file_path

def remote_scalar(
    JDBC,
    query,
    params=None
):
    """
    Executes a SQL query against a remote DB via JDBC
    and returns a single scalar value.

    Intended for queries like:
        SELECT COUNT(*)
        SELECT MAX(date)
        SELECT price
    """

    import jaydebeapi

    conn_remote = None

    try:
        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )

        cursor = conn_remote.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        result = cursor.fetchone()

        if result is None:
            return None

        return result[0]  # return first column of first row

    finally:
        if conn_remote:
            conn_remote.close()


def sqlite_query_to_table(
    db_path: str,
    query: str,
    table_name: str,
    log_folder: str,
    log_items: list[tuple[str, str]] = None,
    mode: str = "replace",
    unique_keys: list[str] = None
):
    """
    Runs a query and writes results to a SQLite table with flexible write mode.

    Params:
        db_path: path to SQLite database file
        query: SQL query to run
        table_name: table to create/modify
        log_folder: folder to save log
        log_items: optional metadata for logging
        mode: 'replace', 'append', or 'upsert'
        unique_keys: list of column names for upsert (required if mode='upsert')
    """
    import os
    import sqlite3
    import pandas as pd
    from datetime import datetime

    os.makedirs(log_folder, exist_ok=True)
    log_lines = []
    log_path = os.path.join(log_folder, f"{table_name}_load_log.txt")

    def log(msg):
        print(msg)
        log_lines.append(msg)

    conn = None
    try:
        log(f"🔌 Connecting to SQLite DB at '{db_path}'...")
        conn = sqlite3.connect(db_path)

        log(f"▶️ Running query to populate '{table_name}'...")
        df = pd.read_sql_query(query, conn)

        if df.empty:
            log(f"⚠️ Query returned no rows — skipping table '{table_name}'.")
            return

        if mode == "replace":
            log(f"💾 Replacing table '{table_name}' with {len(df):,} rows...")
            df.to_sql(table_name, conn, if_exists="replace", index=False)

        elif mode == "append":
            log(f"💾 Appending {len(df):,} rows to table '{table_name}'...")
            df.to_sql(table_name, conn, if_exists="append", index=False)

        elif mode == "upsert":
            if not unique_keys:
                raise ValueError("unique_keys must be provided for upsert mode.")
            log(f"💾 Upserting {len(df):,} rows into table '{table_name}'...")
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cols = row.index.tolist()
                vals = [row[c] for c in cols]
                placeholders = ", ".join("?" for _ in vals)
                updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in unique_keys])
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(cols)})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(unique_keys)}) DO UPDATE SET {updates};
                """
                cursor.execute(sql, vals)
            conn.commit()
        else:
            raise ValueError(f"Unknown mode '{mode}'. Use 'replace', 'append', or 'upsert'.")

        log(f"✅ Table '{table_name}' updated successfully.")

        if log_items:
            log("📄 Additional info:")
            for label, value in log_items:
                log(f"   - {label}: {value}")

    except Exception as e:
        log(f"❌ Error creating/updating table '{table_name}': {e}")
        raise

    finally:
        if conn:
            conn.close()
            log(f"🔒 SQLite connection closed for '{db_path}'")

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"Log created {ts}\n\n")
            for line in log_lines:
                f.write(line + "\n")
        print(f"📝 Log written to: {log_path}")
        print("Done.")


def load_list(file_path: str, col_name: str = None) -> List[str]:
    """
    Load a list of strings from a text, CSV, or Excel file.

    Args:
        file_path: Path to the file.
        col_name: Required for CSV/Excel, ignored for TXT.

    Returns:
        List of strings (lines or column values)
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = path.suffix.lower()
    
    if ext == ".txt":
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        return lines

    elif ext == ".csv":
        if not col_name:
            raise ValueError("col_name must be specified for CSV")
        df = pd.read_csv(path)
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in {file_path}")
        return df[col_name].dropna().astype(str).tolist()

    elif ext in [".xls", ".xlsx"]:
        if not col_name:
            raise ValueError("col_name must be specified for Excel")
        df = pd.read_excel(path)
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in {file_path}")
        return df[col_name].dropna().astype(str).tolist()

    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def write_list(items: List[str], file_path: str, col_name: str = "value", overwrite: bool = True) -> None:
    """
    Write a list of strings to a text, CSV, or Excel file.

    Args:
        items: List of strings to write.
        file_path: Output file path.
        col_name: Column name for CSV/Excel.
        overwrite: Overwrite existing file if True, append if False (only for TXT).
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = path.suffix.lower()

    if ext == ".txt":
        mode = "w" if overwrite else "a"
        with open(path, mode, encoding="utf-8") as f:
            for item in items:
                f.write(f"{item}\n")

    elif ext == ".csv":
        df = pd.DataFrame({col_name: items})
        df.to_csv(path, index=False)

    elif ext in [".xls", ".xlsx"]:
        df = pd.DataFrame({col_name: items})
        df.to_excel(path, index=False, sheet_name="Sheet1")

    else:
        raise ValueError(f"Unsupported file extension: {ext}")

def Load_Google_Sheet_To_Dataframe(sheet_ID: str):
    """
    Load a Google Sheet into a pandas DataFrame.

    Args:
        sheet_ID: The ID of the Google Sheet.

    Returns:
        pandas.DataFrame
    """

    import pandas as pd
    import time

    start_time = time.time()

    if not sheet_ID:
        raise ValueError("sheet_ID cannot be empty")

    print("[INFO] Starting Google Sheet load...")

    url = f"https://docs.google.com/spreadsheets/d/{sheet_ID}/export?format=csv"

    try:
        print(f"[INFO] Fetching sheet: {sheet_ID}")

        df = pd.read_csv(url)

        row_count = len(df)
        col_count = len(df.columns)

        elapsed = round(time.time() - start_time, 2)

        print(f"[SUCCESS] Loaded {row_count} rows x {col_count} columns ({elapsed}s)")

        return df

    except pd.errors.EmptyDataError:
        print("[ERROR] Sheet returned no data.")
        raise

    except pd.errors.ParserError as e:
        print(f"[ERROR] CSV parsing failed: {e}")
        raise

    except Exception as e:
        print(f"[ERROR] Failed to load Google Sheet: {e}")
        raise

