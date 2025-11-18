#!/usr/bin/env python3
from pathlib import Path
import os
import sys
import glob
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- DB Config ----------
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASS = os.getenv('DB_PASS', 'bo_is_dabest')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5555')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------- Table mapping ----------
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

# ---------- Helpers ----------
def find_project_data_dir(script_path: Path) -> Path:
    cur = script_path.resolve()
    for parent in [cur] + list(cur.parents):
        cand = parent / 'data' / 'Project Dataset'
        if cand.exists() and cand.is_dir():
            return cand
    raise FileNotFoundError("Could not find 'data/Project Dataset'")

def smart_read_file(filepath: str, chunksize=None):
    ext = Path(filepath).suffix.lower().lstrip('.')
    try:
        if ext == 'csv':
            return pd.read_csv(filepath, dtype=str, chunksize=chunksize)
        elif ext in ('xls', 'xlsx'):
            return pd.read_excel(filepath, dtype=str)
        elif ext == 'parquet':
            return pd.read_parquet(filepath).astype(str)
        elif ext in ('pickle', 'pkl'):
            obj = pd.read_pickle(filepath)
            df = pd.DataFrame(obj) if not isinstance(obj, pd.DataFrame) else obj
            return df.astype(str)
        elif ext == 'json':
            try:
                df = pd.read_json(filepath, dtype=str)
            except ValueError:
                try:
                    df = pd.read_json(filepath, lines=True, dtype=str)
                except ValueError as e:
                    print(f"❌ Failed to read JSON {filepath}: {e}")
                    return None
            return df.astype(str)
        elif ext == 'html':
            dfs = pd.read_html(filepath)
            if not dfs:
                return None
            return dfs[0].astype(str)
        else:
            print(f"⚠️ Unsupported extension: {ext}")
            return None
    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")
        return None

def execute_sql_file(engine, sql_path: Path):
    if not sql_path.exists():
        print(f"⚠️ SQL schema file {sql_path} not found, skipping.")
        return
    sql = sql_path.read_text(encoding='utf8')
    with engine.connect() as conn:
        for stmt in filter(None, [s.strip() for s in sql.split(';')]):
            conn.execute(text(stmt))
        conn.commit()
    print(f"✅ Applied SQL schema from {sql_path}")

# ---------- Ingestion ----------
def fallback_json_insert(engine, df, table_name, batch_size=500):
    fallback_table = f"{table_name}_json"
    print(f"      ❌ Insert failed, falling back to {fallback_table}")
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {fallback_table} (
                id SERIAL PRIMARY KEY,
                payload JSONB,
                ingested_at TIMESTAMP DEFAULT now()
            );
        """))
        payloads = df.to_dict(orient='records')
        for i in range(0, len(payloads), batch_size):
            batch = payloads[i:i+batch_size]
            batch_clean = [{k: (None if pd.isna(v) else v) for k,v in r.items()} for r in batch]
            conn.execute(
                text(f"INSERT INTO {fallback_table} (payload) VALUES (:payload)"),
                [{"payload": json.dumps(r)} for r in batch_clean]
            )
        conn.commit()
    print(f"      ✅ Stored {len(df)} rows into {fallback_table}")

def ingest_table(engine, table_name, patterns, data_root: Path, json_batch_size=500):
    files_to_process = []
    for p in patterns:
        glob_path = str((data_root / p).resolve())
        files_to_process.extend(glob.glob(glob_path))
    files_to_process = sorted(set(files_to_process))
    if not files_to_process:
        print(f"⚠️ No files found for {table_name}")
        return
    print(f"\nProcessing {table_name} ({len(files_to_process)} files)...")
    
    for fp in files_to_process:
        print(f"   -> Reading {Path(fp).name} ...")
        # Chunk only for CSV/JSON
        chunksize = 100_000 if fp.lower().endswith(('.csv', '.json')) else None
        data = smart_read_file(fp, chunksize=chunksize)
        
        if data is None:
            print("      (no data or failed to read)")
            continue
        
        if chunksize:
            for chunk in data:
                if not isinstance(chunk, pd.DataFrame):
                    continue
                chunk.columns = chunk.columns.str.strip()
                chunk = chunk.where(pd.notna(chunk), None)
                try:
                    chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
                except SQLAlchemyError:
                    fallback_json_insert(engine, chunk, table_name, batch_size=json_batch_size)
        else:
            if isinstance(data, pd.DataFrame):
                data.columns = data.columns.str.strip()
                data = data.where(pd.notna(data), None)
                try:
                    data.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
                except SQLAlchemyError:
                    fallback_json_insert(engine, data, table_name, batch_size=json_batch_size)
            else:
                print(f"⚠️ Skipping file {Path(fp).name}: not a valid DataFrame")
        
        print(f"      ✅ Finished file {Path(fp).name}")

# ---------- Main ----------
def main():
    script_path = Path(__file__).resolve()
    try:
        DATA_ROOT_DIR = find_project_data_dir(script_path)
    except FileNotFoundError as e:
        print("❌", e)
        sys.exit(1)
    print(f"Data root: {DATA_ROOT_DIR}")

    engine = create_engine(CONN_STR, future=True)
    execute_sql_file(engine, script_path.parent / 'staging_tables.sql')

    for table_name, patterns in TABLE_MAPPING.items():
        ingest_table(engine, table_name, patterns, DATA_ROOT_DIR)
    
    print("\n--- Ingestion Complete ---")

if __name__ == '__main__':
    main()
