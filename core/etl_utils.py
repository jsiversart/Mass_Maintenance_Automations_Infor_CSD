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
