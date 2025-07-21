import os
import json
import requests
import urllib3
import time
import argparse
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

# === Tokenize / Detokenize Logic ===
def process_row(idx, row):
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

        if tok_payload:
            res = requests.post(vts['tokenUrl'], json=tok_payload, headers=headers, auth=vts_auth, verify=False)
            res.raise_for_status()
            for col, val in zip([c for c in columns_config if c['action'] == 'TOKENIZE'], res.json()):
                row[col['name']] = val['token']
                actions.append(f"[TOKENIZE] {col['name']} → {val['token']}")

        if detok_payload:
            res = requests.post(vts['detokenUrl'], json=detok_payload, headers=headers, auth=vts_auth, verify=False)
            res.raise_for_status()
            for col, val in zip([c for c in columns_config if c['action'] == 'DETOKENIZE'], res.json()):
                row[col['name']] = val['data']
                actions.append(f"[DETOKENIZE] {col['name']} → {val['data']}")

        log("SUCCESS", "Processed successfully", idx)
        for a in actions:
            print(f"    {a}")
        return (idx, row)
    except Exception as e:
        log("ERROR", f"{e}", idx)
        return (idx, None)

# === Main Execution ===
def main():
    start = time.time()
    total = 0
    try:
        with engine_src.connect() as src_conn, engine_dest.connect() as dst_conn:
            log("INFO", "Testing connections")
            src_conn.execute(text("SELECT 1"))
            dst_conn.execute(text("SELECT 1"))

            # Fetch source data
            log("INFO", "Fetching data")
            rows = list(src_conn.execute(text(f"SELECT * FROM {schema_src}.{table_src} ORDER BY created_at ASC")).mappings())
            if not rows:
                log("INFO", "No rows to process")
                return

            # Process rows in parallel
            results = [None] * len(rows)
            with ThreadPoolExecutor(max_workers=get_optimal_workers()) as executor:
                futures = [executor.submit(process_row, i, dict(r)) for i, r in enumerate(rows)]
                for future in as_completed(futures):
                    idx, processed = future.result()
                    if processed:
                        results[idx] = processed

            # Insert to destination
            data_ready = [r for r in results if r]
            if data_ready:
                cols = list(data_ready[0].keys())
                insert_sql = text(f"INSERT INTO {schema_dest}.{table_dest} ({', '.join(cols)}) VALUES ({', '.join([f':{c}' for c in cols])})")
                dst_conn.execute(insert_sql, data_ready)
                dst_conn.commit()
                total = len(data_ready)
    except KeyboardInterrupt:
        log("INFO", "Interrupted by user")
    except SQLAlchemyError as e:
        log("ERROR", f"DB Error: {e}")
    except Exception as e:
        log("ERROR", f"Unexpected Error: {e}")
    finally:
        log("SUCCESS", f"Completed. Processed {total} rows in {round(time.time() - start)}s")

if __name__ == "__main__":
    main()
