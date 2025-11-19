#!/usr/bin/env python3
import os
import glob
import io
import pandas as pd
import psycopg2
from psycopg2 import extras
from pathlib import Path

# ---------------- CONFIG ----------------
DATA_ROOT = Path(__file__).resolve().parents[2] / "data" / "Project Dataset"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5555")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASS = os.getenv("DB_PASS", "bo_is_dabest")

TABLE_MAPPING = {
    'staging_product_list': ['Business Department/product_list.xlsx'],
    'staging_user_data': ['Customer Management Department/user_data.json'],
    'staging_user_job': ['Customer Management Department/user_job.csv'],
    'staging_user_credit_card': ['Customer Management Department/user_credit_card.pickle'],
    'staging_merchant_data': ['Enterprise Department/merchant_data.html'],
    'staging_staff_data': ['Enterprise Department/staff_data.html'],
    'staging_order_with_merchant_data': ['Enterprise Department/order_with_merchant_data*'],
    'staging_campaign_data': ['Marketing Department/campaign_data.csv'],
    'staging_transactional_campaign_data': ['Marketing Department/transactional_campaign_data.csv'],
    'staging_order_data': ['Operations Department/order_data_*'],
    'staging_line_item_prices': ['Operations Department/line_item_data_prices*'],
    'staging_line_item_products': ['Operations Department/line_item_data_products*'],
    'staging_order_delays': ['Operations Department/order_delays.html']
}

# ---------------- UTILS ----------------
def sanitize_column(col):
    col = col.strip().lower()
    col = col.replace(" ", "_").replace(":", "_").replace("-", "_")
    col = col.replace(".", "_").replace("/", "_")
    col = "".join(c for c in col if c.isalnum() or c == "_")
    return col if col else "col"

def make_columns_unique(columns):
    seen = {}
    result = []
    for c in columns:
        if c in seen:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            result.append(c)
    return result

def map_dtype_to_pg(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"

# ---------------- CREATE TABLE ----------------
def create_table_from_df(table_name, df, conn, force_text_cols=None):
    if force_text_cols is None:
        force_text_cols = []

    cols = []
    for c in df.columns:
        if c in force_text_cols:
            cols.append(f"{c} TEXT")
        else:
            cols.append(f"{c} {map_dtype_to_pg(df[c].dtype)}")
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)});"
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

# ---------------- COPY ----------------
def copy_to_postgres(df, table_name, conn):
    df = df.where(pd.notna(df), None)
    buffer = df.to_csv(index=False, header=False)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL ''", io.StringIO(buffer))
    conn.commit()

# ---------------- PICKLE INSERT ----------------
def insert_pickle(df, table_name, conn):
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    # Force all columns to string to avoid integer overflow
    df = df.astype(str).where(pd.notna(df), None)

    # Force TEXT for all columns in pickle tables
    create_table_from_df(table_name, df, conn, force_text_cols=df.columns.tolist())

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
        conn.commit()

    records = df.to_dict(orient="records")
    str_records = [tuple(str(v) if v is not None else None for v in r.values()) for r in records]

    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s",
            str_records,
            page_size=1000
        )
    conn.commit()
    print(f"   ✅ Pickle loaded into {table_name} as TEXT")

# ---------------- LOAD FILE ----------------
def load_file(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if suffix == ".json":
        try:
            return pd.read_json(path)
        except:
            return pd.read_json(path, lines=True)
    if suffix in [".pkl", ".pickle"]:
        return pd.read_pickle(path)
    if suffix in [".html", ".htm"]:
        return pd.read_html(path)[0]
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise Exception(f"Unsupported file format → {path}")

# ---------------- INGEST TABLE ----------------
def ingest_table(table_name, patterns, base_path, conn):
    print(f"\n📦 Processing {table_name}")
    files = []
    for pattern in patterns:
        files.extend(glob.glob(str(base_path / pattern), recursive=True))
    if not files:
        print(f"   ⚠ No files found for {table_name}")
        return

    frames = []
    is_pickle = False
    for f in files:
        print(f"   → Loading {os.path.basename(f)}")
        try:
            df = load_file(Path(f))
            if f.lower().endswith((".pkl", ".pickle")):
                is_pickle = True
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = make_columns_unique([sanitize_column(c) for c in df.columns])
            frames.append(df)
        except Exception as e:
            print(f"      ❌ Failed reading file: {e}")

    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)

    try:
        if is_pickle:
            insert_pickle(df, table_name, conn)
        else:
            create_table_from_df(table_name, df, conn)
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                conn.commit()
            copy_to_postgres(df, table_name, conn)
            print(f"   ✅ Loaded into {table_name}")
    except Exception as e:
        conn.rollback()  # <- Rollback the failed table
        print(f"      ❌ Failed to ingest {table_name}: {e}")

# ---------------- MAIN ----------------
def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    print(f"Data root detected → {DATA_ROOT}")

    for table_name, patterns in TABLE_MAPPING.items():
        ingest_table(table_name, patterns, DATA_ROOT, conn)

    conn.close()
    print("\n🎉 Ingestion Complete — All tables attempted!")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import glob
import io
import pandas as pd
import psycopg2
from psycopg2 import extras
from pathlib import Path

# ---------------- CONFIG ----------------
DATA_ROOT = Path(__file__).resolve().parents[2] / "data" / "Project Dataset"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5555")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASS = os.getenv("DB_PASS", "bo_is_dabest")

TABLE_MAPPING = {
    'staging_product_list': ['Business Department/product_list.xlsx'],
    'staging_user_data': ['Customer Management Department/user_data.json'],
    'staging_user_job': ['Customer Management Department/user_job.csv'],
    'staging_user_credit_card': ['Customer Management Department/user_credit_card.pickle'],
    'staging_merchant_data': ['Enterprise Department/merchant_data.html'],
    'staging_staff_data': ['Enterprise Department/staff_data.html'],
    'staging_order_with_merchant_data': ['Enterprise Department/order_with_merchant_data*'],
    'staging_campaign_data': ['Marketing Department/campaign_data.csv'],
    'staging_transactional_campaign_data': ['Marketing Department/transactional_campaign_data.csv'],
    'staging_order_data': ['Operations Department/order_data_*'],
    'staging_line_item_prices': ['Operations Department/line_item_data_prices*'],
    'staging_line_item_products': ['Operations Department/line_item_data_products*'],
    'staging_order_delays': ['Operations Department/order_delays.html']
}

# ---------------- UTILS ----------------
def sanitize_column(col):
    col = col.strip().lower()
    col = col.replace(" ", "_").replace(":", "_").replace("-", "_")
    col = col.replace(".", "_").replace("/", "_")
    col = "".join(c for c in col if c.isalnum() or c == "_")
    return col if col else "col"

def make_columns_unique(columns):
    seen = {}
    result = []
    for c in columns:
        if c in seen:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            result.append(c)
    return result

def map_dtype_to_pg(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"

# ---------------- CREATE TABLE ----------------
def create_table_from_df(table_name, df, conn, force_text_cols=None):
    if force_text_cols is None:
        force_text_cols = []

    cols = []
    for c in df.columns:
        if c in force_text_cols:
            cols.append(f"{c} TEXT")
        else:
            cols.append(f"{c} {map_dtype_to_pg(df[c].dtype)}")
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)});"
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

# ---------------- COPY ----------------
def copy_to_postgres(df, table_name, conn):
    df = df.where(pd.notna(df), None)
    buffer = df.to_csv(index=False, header=False)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL ''", io.StringIO(buffer))
    conn.commit()

# ---------------- PICKLE INSERT ----------------
def insert_pickle(df, table_name, conn):
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    # Force all columns to string to avoid integer overflow
    df = df.astype(str).where(pd.notna(df), None)

    # Force TEXT for all columns in pickle tables
    create_table_from_df(table_name, df, conn, force_text_cols=df.columns.tolist())

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
        conn.commit()

    records = df.to_dict(orient="records")
    str_records = [tuple(str(v) if v is not None else None for v in r.values()) for r in records]

    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s",
            str_records,
            page_size=1000
        )
    conn.commit()
    print(f"   ✅ Pickle loaded into {table_name} as TEXT")

# ---------------- LOAD FILE ----------------
def load_file(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if suffix == ".json":
        try:
            return pd.read_json(path)
        except:
            return pd.read_json(path, lines=True)
    if suffix in [".pkl", ".pickle"]:
        return pd.read_pickle(path)
    if suffix in [".html", ".htm"]:
        return pd.read_html(path)[0]
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise Exception(f"Unsupported file format → {path}")

# ---------------- INGEST TABLE ----------------
def ingest_table(table_name, patterns, base_path, conn):
    print(f"\n📦 Processing {table_name}")
    files = []
    for pattern in patterns:
        files.extend(glob.glob(str(base_path / pattern), recursive=True))
    if not files:
        print(f"   ⚠ No files found for {table_name}")
        return

    frames = []
    is_pickle = False
    for f in files:
        print(f"   → Loading {os.path.basename(f)}")
        try:
            df = load_file(Path(f))
            if f.lower().endswith((".pkl", ".pickle")):
                is_pickle = True
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = make_columns_unique([sanitize_column(c) for c in df.columns])
            frames.append(df)
        except Exception as e:
            print(f"      ❌ Failed reading file: {e}")

    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)

    try:
        if is_pickle:
            insert_pickle(df, table_name, conn)
        else:
            create_table_from_df(table_name, df, conn)
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                conn.commit()
            copy_to_postgres(df, table_name, conn)
            print(f"   ✅ Loaded into {table_name}")
    except Exception as e:
        conn.rollback()  # <- Rollback the failed table
        print(f"      ❌ Failed to ingest {table_name}: {e}")

# ---------------- MAIN ----------------
def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    print(f"Data root detected → {DATA_ROOT}")

    for table_name, patterns in TABLE_MAPPING.items():
        ingest_table(table_name, patterns, DATA_ROOT, conn)

    conn.close()
    print("\n🎉 Ingestion Complete")

if __name__ == "__main__":
    main()
