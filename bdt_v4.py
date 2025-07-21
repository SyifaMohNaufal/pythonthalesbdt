import os
import json
import requests
import urllib3
import time
import argparse
import httpx
import asyncio
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Helper Functions ===
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(level, message, row_id=None):
    prefix = f"[{level}] {get_timestamp()}"
    suffix = f" Row {row_id}" if row_id is not None else ""
    print(f"{prefix}{suffix} - {message}")

def get_optimal_workers():
    return (os.cpu_count() or 1) * 4

# === Argument Parsing ===
parser = argparse.ArgumentParser(description="BDT Transformation Script")
parser.add_argument("-p", "--policy", required=True, help="Path to the transformation policy JSON file")
parser.add_argument("-c", "--config", default="bdt.config", help="Path to the config JSON file")
args = parser.parse_args()

# === Load Policy and Config ===
with open(args.policy, 'r', encoding='utf-8') as f:
    policy = json.load(f)
with open(args.config, 'r', encoding='utf-8') as f:
    config = json.load(f)

# === Extract Policy Info ===
source_url = policy['source']['connectionurl']
dest_url = policy['destination']['connectionurl']
schema_src = policy['tables'][0]['sourceSchema']
schema_dest = policy['tables'][0]['destinationSchema']
table_src = policy['tables'][0]['sourceTable']
table_dest = policy['tables'][0]['destinationTable']
columns_config = policy['tables'][0]['columns']

# === VTS Configuration ===
vts = config['vts']
batch_size = config['batchSize']
vts_auth = HTTPBasicAuth(vts['userName'], vts['password'])
headers = {"Content-Type": "application/json"}

# === DB Engines ===
engine_src = create_engine(source_url)
engine_dest = create_engine(dest_url)

# === Retry post ===
async def post_with_retry(client, url, payload, headers, retries=3):
    for attempt in range(retries):
        try:
            return await client.post(url, json=payload, headers=headers)
        except httpx.ConnectTimeout as e:
            if attempt < retries - 1:
                await asyncio.sleep(5 ** attempt)
            else:
                # You can also log the row number here if passed in
                raise RuntimeError(f"ConnectTimeout after {retries} retries") from e

# === Tokenize / Detokenize Logic ===
async def process_row(idx, row):
    try:
        log("INFO", "Start processing", idx)
        original = row.copy()
        tok_payload, detok_payload, actions = [], [], []

        for col in columns_config:
            name, action = col['name'], col['action']
            cfg = col['config'][0]
            if action == 'TOKENIZE':
                tok_payload.append({"tokengroup": cfg['tokenGroup'], "data": row[name], "tokentemplate": cfg['tokenTemplate']})
            elif action == 'DETOKENIZE':
                detok_payload.append({"tokengroup": cfg['tokenGroup'], "token": row[name], "tokentemplate": cfg['tokenTemplate']})

        timeout = httpx.Timeout(10.0, connect=5.0)  # 10s total timeout, 5s to connect
        async with httpx.AsyncClient(verify=False, auth=vts_auth, timeout=timeout) as client:
            if tok_payload:
                # print(tok_payload)
                res = await post_with_retry(client, vts['tokenUrl'], tok_payload, headers)
                try:
                    res.raise_for_status()
                except httpx.HTTPStatusError as e:
                    log("ERROR", f"Row {idx} HTTP status {e.response.status_code}: {e.response.text}")
                    raise
                for col, val in zip([c for c in columns_config if c['action'] == 'TOKENIZE'], res.json()):
                    row[col['name']] = val['token']
                    actions.append(f"[TOKENIZE] {col['name']} → {val['token']}")

            if detok_payload:
                res = await post_with_retry(client, vts['detokenUrl'], detok_payload, headers)
                try:
                    res.raise_for_status()
                except httpx.HTTPStatusError as e:
                    log("ERROR", f"Row {idx} HTTP status {e.response.status_code}: {e.response.text}")
                    raise
                for col, val in zip([c for c in columns_config if c['action'] == 'DETOKENIZE'], res.json()):
                    row[col['name']] = val['data']
                    actions.append(f"[DETOKENIZE] {col['name']} → {val['data']}")

        log("SUCCESS", "Processed successfully", idx)
        for a in actions:
            print(f"    {a}")
        return (idx, row)
    except httpx.HTTPStatusError as e:
        log("ERROR", f"Row {idx} HTTP error: {e.response.status_code} - {e.response.text}")
        return (idx, None)
    except Exception as e:
        import traceback
        log("ERROR", f"Row {idx} unexpected error: {str(e)}")
        traceback.print_exc()
        return (idx, None)

# === Main Execution ===
async def main():
    start = time.time()
    total = 0
    error = 0
    try:
        # Run blocking DB connection check in thread pool
        def test_connections():
            with engine_src.connect() as src_conn, engine_dest.connect() as dst_conn:
                log("INFO", "Testing connections")
                src_conn.execute(text("SELECT 1"))
                dst_conn.execute(text("SELECT 1"))

        await asyncio.get_event_loop().run_in_executor(None, test_connections)

        # Fetch source data
        def fetch_rows():
            with engine_src.connect() as conn:
                log("INFO", "Fetching data")
                return list(conn.execute(text(f"SELECT * FROM {schema_src}.{table_src} ORDER BY created_at ASC")).mappings())
        
        rows = await asyncio.get_event_loop().run_in_executor(None, fetch_rows)
        if not rows:
            log("INFO", "No rows to process")
            return

        # Process rows concurrently using asyncio.gather
        semaphore = asyncio.Semaphore(100)
        async def limited_process_row(i, r):
            async with semaphore:
                return await process_row(i, r)
        tasks = [limited_process_row(i, dict(r)) for i, r in enumerate(rows)]
        results = await asyncio.gather(*tasks)

        # Insert to destination
        success_indexes = [i for i, r in results if r]
        error = set(range(len(rows))) - set(success_indexes)

        data_ready = [r for i, r in results if r]
        if data_ready:
            def insert_rows():
                with engine_dest.connect() as conn:
                    cols = list(data_ready[0].keys())
                    insert_sql = text(f"INSERT INTO {schema_dest}.{table_dest} ({', '.join(cols)}) VALUES ({', '.join([f':{c}' for c in cols])})")
                    conn.execute(insert_sql, data_ready)
                    conn.commit()

            await asyncio.get_event_loop().run_in_executor(None, insert_rows)
            total = len(data_ready)
            error = len(error)

    except KeyboardInterrupt:
        log("INFO", "Interrupted by user")
    except SQLAlchemyError as e:
        log("ERROR", f"DB Error: {e}")
    except Exception as e:
        log("ERROR", f"Unexpected Error: {e}")
    finally:
        if total > 0:
            log("SUCCESS", f"Completed. Processed {total} rows in {round(time.time() - start)}s with {error} missing rows")
        else:
            log("WARNING", f"No rows processed successfully. Took {round(time.time() - start)}s")


if __name__ == "__main__":
    asyncio.run(main())
