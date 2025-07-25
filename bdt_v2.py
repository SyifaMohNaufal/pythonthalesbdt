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



# Argument parser
parser = argparse.ArgumentParser(description="BDT Transformation Script")
parser.add_argument(
    "-p", "--policy",
    required=True,
    help="Path to the transformation policy JSON file"
)
parser.add_argument(
    "-c", "--config",
    default="bdt.config",
    help="Path to the config JSON file (default: bdt.config)"
)
args = parser.parse_args()  # ✅ harus setelah semua add_argument()

# Load transformation policy
with open(args.policy, 'r', encoding='utf-8') as f:
    policy_data = json.load(f)

# Load config
with open(args.config, 'r', encoding='utf-8') as f:
    config_data = json.load(f)

policy_id = policy_data["id"]
policy_name = policy_data["name"]
policy_source = policy_data["source"]
policy_destination = policy_data["destination"]
policy_tables = policy_data["tables"]
policy_columns = policy_tables[0]["columns"]

tokenized_column = [p["name"] for p in policy_columns]
source_connectionurl = policy_source["connectionurl"]
destination_connectionurl = policy_destination["connectionurl"]
policy_sourceSchema = policy_tables[0]["sourceSchema"]
policy_destinationSchema = policy_tables[0]["destinationSchema"]
policy_sourceTable = policy_tables[0]["sourceTable"]
policy_destinationTable = policy_tables[0]["destinationTable"]

config_batch_size = config_data["batchSize"]
config_vts = config_data["vts"]
vts_host = config_vts["hostName"]
vts_user = config_vts["userName"]
vts_pass = config_vts["password"]
vts_tokenUrl = config_vts["tokenUrl"]
vts_detokenUrl = config_vts["detokenUrl"]

# Set up DB connections
source_engine = create_engine(source_connectionurl)
destination_engine = create_engine(destination_connectionurl)

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- Tokenization Function ---
def process_row(row_idx, row_dict):
    tokenize_payload = []
    detokenize_payload = []
    col_action_map = {}

    print(f"[INFO] {get_timestamp()} Row {row_idx} start processing")

    # Prepare payloads per action per column
    for col_config in policy_columns:
        col_name = col_config["name"]
        action = col_config["action"]
        col_action_map[col_name] = action
        if col_name in row_dict:
            config = col_config["config"][0]
            if action == "TOKENIZE":
                tokenize_payload.append({
                    "tokengroup": config["tokenGroup"],
                    "data": row_dict[col_name],
                    "tokentemplate": config["tokenTemplate"]
                })
            elif action == "DETOKENIZE":
                detokenize_payload.append({
                    "tokengroup": config["tokenGroup"],
                    "token": row_dict[col_name],
                    "tokentemplate": config["tokenTemplate"]
                })

    headers = {"Content-Type": "application/json"}

    try:
        # Process TOKENIZE
        if tokenize_payload:
            response = requests.post(
                vts_tokenUrl,
                json=tokenize_payload,
                headers=headers,
                auth=HTTPBasicAuth(vts_user, vts_pass),
                verify=False
            )
            response.raise_for_status()
            tokens = response.json()
            for col_config, token_data in zip([c for c in policy_columns if c["action"] == "TOKENIZE"], tokens):
                row_dict[col_config["name"]] = token_data["token"]

        # Process DETOKENIZE
        if detokenize_payload:
            response = requests.post(
                vts_detokenUrl,
                json=detokenize_payload,
                headers=headers,
                auth=HTTPBasicAuth(vts_user, vts_pass),
                verify=False
            )
            response.raise_for_status()
            detokens = response.json()
            for col_config, token_data in zip([c for c in policy_columns if c["action"] == "DETOKENIZE"], detokens):
                row_dict[col_config["name"]] = token_data["data"]
        print(f"[INFO] {get_timestamp()} Row {row_idx} processed successfully")
        return (row_idx, row_dict)

    except Exception as e:
        print(f"❌ Error processing row {row_idx}: {e}")
        return (row_idx, None)
    
def get_optimal_workers(io_bound=True):
    cpu_cores = os.cpu_count() or 1
    if io_bound:
        return cpu_cores * 5  # or 2x, 4x depending on your use case
    else:
        return cpu_cores

# --- Main Execution ---
try:
    start_time = time.time()
    print("🔌 Connecting to source and destination databases...")
    with source_engine.connect() as sconn, destination_engine.connect() as dconn:
        print("Source connection: ", sconn.execute(text("SELECT 1")).scalar())
        print("Destination connection: ", dconn.execute(text("SELECT 1")).scalar())
        print("✅ Connection successful")

        count_result = sconn.execute(text(f"SELECT COUNT(*) FROM {policy_sourceTable}"))
        count = count_result.scalar()
        if int(count) > config_batch_size:
            print("⚠️ Record total exceeds the batch size limit")
        
        # Get all rows
        source_data = sconn.execute(text(f"SELECT * FROM {policy_sourceTable} ORDER BY created_at ASC"))
        source_data = list(source_data.mappings())  # For indexable access

        # Prepare multithread processing
        results = [None] * len(source_data)
        print("optimal workers:", get_optimal_workers(io_bound=True))
        with ThreadPoolExecutor(max_workers=get_optimal_workers(io_bound=True)) as executor:
            futures = [
                executor.submit(process_row, idx, dict(row))
                for idx, row in enumerate(source_data)
            ]
            for future in as_completed(futures):
                idx, result = future.result()
                if result:
                    results[idx] = result

        # Insert results into destination
        total_count = 0
        for row_dict in results:
            if not row_dict:
                continue  # Skip failed
            insert_columns = list(row_dict.keys())
            placeholders = [f":{col}" for col in insert_columns]
            insert_sql = text(f"""
                INSERT INTO {policy_destinationSchema}.{policy_destinationTable}
                ({', '.join(insert_columns)})
                VALUES ({', '.join(placeholders)})
            """)
            dconn.execute(insert_sql, row_dict)
            total_count += 1
        dconn.commit()

except KeyboardInterrupt:
    print("\n🛑 Process interrupted by user (Ctrl+C). Exiting gracefully...")        

except SQLAlchemyError as e:
    print("❌ Database connection failed:", e)

except Exception as e:
    print(f"❌ Unexpected error: {e}")

end_time = time.time()
elapsed_time = round(end_time - start_time)
print(f"✅ BDT transformation completed: {total_count} rows processed in {elapsed_time} seconds.")
