
import json
import requests
import urllib3
import time
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

start_time = time.time()

with open('DbToDbTransformation01.policy', 'r', encoding='utf-8') as f:
    policy_data = json.load(f)

# print("policy:", policy_data)
policy_id = policy_data["id"]
policy_name = policy_data["name"]

policy_source = policy_data["source"]
source_connectionurl=policy_source["connectionurl"]

policy_destination = policy_data["destination"]
destination_connectionurl = policy_destination["connectionurl"]
policy_tables = policy_data["tables"]
policy_sourceSchema = policy_tables[0]["sourceSchema"]
policy_destinationSchema = policy_tables[0]["destinationSchema"]
policy_columns = policy_tables[0]["columns"]
tokenized_column = [p["name"] for p in policy_columns]

policy_sourceTable = policy_tables[0]["sourceTable"]
policy_destinationTable = policy_tables[0]["destinationTable"]


# Example for a .config file
with open('bdt.config', 'r', encoding='utf-8') as f:
    config_data = json.load(f)

# print("config:", config_data)
config_batch_size= config_data["batchSize"]

config_vts= config_data["vts"]
vts_host=config_vts["hostName"]
vts_user=config_vts["userName"]
vts_pass=config_vts["password"]
vts_tokenUrl=config_vts["tokenUrl"]
vts_detokenUrl=config_vts["detokenUrl"]

vts_sslConfig=config_vts["sslConfig"]
ssl_serverConfig=vts_sslConfig["serverConfig"]
ssl_clientConfig=vts_sslConfig["clientConfig"]

config_crypto= config_data["crypto"]
config_characterSets= config_data["characterSets"]


# TODO: Make db connection
source_engine = create_engine(source_connectionurl)
destination_engine = create_engine(destination_connectionurl)

try:
    print("Connecting to source and destination database...")
    print("Source Connection string:", source_engine.url)
    print("Destination Connection string:", destination_engine.url)
    
    with source_engine.connect() as sconn, destination_engine.connect() as dconn:
        result = sconn.execute(text("SELECT 1"))
        print("✅Source Connection successful:", result.scalar())
        result = dconn.execute(text("SELECT 1"))
        print("✅Destination Connection successful:", result.scalar())

        count_record = sconn.execute(text(f"select count(*) from {policy_sourceTable}"))
        count=count_record.scalar()
        if int(count) > config_batch_size:
            print("record total exceed the batch size limit")
        else:
            # TODO: get the source table by the batch limit 
            source_data = sconn.execute(text(f"select * from {policy_sourceTable} order by create_date asc"))
            tokenized_column = [p["name"] for p in policy_columns]
            
            total_count = 0
            for row in source_data.mappings():
                row_dict = dict(row)
                print("row:", row_dict)
                payload = []
                for col in row_dict:
                    if col in tokenized_column:
                        payload.append({
                            "tokengroup":policy_columns[0]["config"][0]["tokenGroup"],
                            "data":row_dict[col],
                            "tokentemplate":policy_columns[0]["config"][0]["tokenTemplate"]
                        })
                headers = {
                    "Content-Type": "application/json"
                }
                response = requests.post(vts_tokenUrl, json=payload, headers=headers, auth=HTTPBasicAuth(vts_user, vts_pass), verify=False)
                response_json=response.json()
                print("json:", response_json)
                if "status" in response_json:
                    print("Transformation skipped")
                    continue
                response_json = [
                    {'column': col, **data}
                    for col, data in zip(tokenized_column, response_json)
                ]
                
                for r in response_json:
                    for col in row_dict:
                        if col == r['column']:
                            row_dict[col] = r['token']
                        else:
                            continue
                
                insert_columns = list(row_dict.keys())
                placeholders = [f":{col}" for col in insert_columns]

                insert_sql = text(f"""
                    INSERT INTO {policy_destinationSchema}.{policy_destinationTable}
                    ({', '.join(insert_columns)})
                    VALUES ({', '.join(placeholders)})
                """)

                insert_to_table = dconn.execute(insert_sql, row_dict)
                dconn.commit()
                total_count += 1

except SQLAlchemyError as e:
    print("❌ Connection failed:", e)

end_time = time.time()
elapsed_time = round(end_time-start_time)
print(f"✅ BDT transformation completed: {total_count} rows processed in {elapsed_time} seconds.")
# TODO: add a log of the result of the process (success or error)
# TODO: finish the with a log of a summary of the process
