from google.cloud import bigquery
from pathlib import Path
import os
import json
import pandas as pd
import sqlite3
from dotenv import load_dotenv

def dataset_exists(client,dataset_id):
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
        return True
    except:
        return False

def table_exists(client,table_id):
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists")
        return True
    except:
        return False

def create_dataset(client,dataset_id):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "northamerica-northeast2"
    dataset = client.create_dataset(dataset,timeout=30)
    print(f"Created dataset {dataset_id}")

def load_data(schema_path,conn,sql_path):
    # load data from database into json format
    with open(schema_path) as f:
        schema = json.load(f)
    
    cols = [item["name"] for item in schema]
    data_types = [item["type"] for item in schema]
    cols_types = zip(cols,data_types)

    with open(sql_path,"r") as sql:
        df = pd.read_sql_query(sql.read(),conn)

    df = df[cols]
    for col, type in cols_types:
        if type == "DATE":
            df[col] = pd.to_datetime(df[col])

    return df

def create_or_overwrite_table(df, client,table_id,schema_path):
    schema = client.schema_from_json(schema_path)
    job_config = bigquery.LoadJobConfig(schema=schema,
                                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                                        )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded table {table_id}")


def main():
    print("Start BigQuery")
    script_folder = Path(__file__).resolve().parent
    bq_schemas_folder = os.path.join(script_folder,"bq_schemas")
    sql_scripts_folder = os.path.join(script_folder,"sql_scripts")

    env_path = os.path.join(script_folder.parent,".env")
    load_dotenv(env_path)
    data_folder = os.environ.get("DATA_FOLDER")
    db_file = os.path.join(data_folder,"stock.db")
    conn = sqlite3.connect(db_file)

    client = bigquery.Client()
    dataset_name = "stock_dataset"
    small_daily_table_name = "small_daily"
    dataset_id = f"{client.project}.{dataset_name}"
    small_daily_table_id = f"{dataset_id}.{small_daily_table_name}"

    if not dataset_exists(client,dataset_id):
        print(f"Dataset {dataset_name} does not exist")
        create_dataset(client,dataset_id)

    # load/overwrite small_daily table
    small_daily_schema_path = os.path.join(bq_schemas_folder,"small_daily.json")
    sql_path = os.path.join(sql_scripts_folder,"query_small_daily.sql")
    df = load_data(small_daily_schema_path,conn,sql_path)
    create_or_overwrite_table(df, client,small_daily_table_id,small_daily_schema_path)
    print("End BigQuery")
    
if __name__ == "__main__":
    main()
    

    