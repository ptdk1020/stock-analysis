import os
import time
import datetime
import pandas as pd
from dotenv import load_dotenv
from api_models import GroupedDailyRequest
from google.cloud import storage
from pathlib import Path

def blob_exists(blob_name,bucket_name="stock-data-project-khoa"):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket,name=blob_name).exists(storage_client)
    return stats

def get_grouped_daily():
    # get environment variables
    script_folder = Path(__file__).resolve().parent # can also use os.path.dirname(__file__)
    env_path = os.path.join(script_folder.parent,".env")
    load_dotenv(env_path)
    api_key = os.environ.get("API_KEY")
    data_folder = os.environ.get("DATA_FOLDER")
    outdir = os.path.join(data_folder,"grouped_daily_json")

    # make dirs
    os.makedirs(data_folder,exist_ok=True)
    os.makedirs(outdir,exist_ok=True)

    # start point and end point
    start_date = os.environ.get("START_DATE")
    end_date = datetime.date.today()-datetime.timedelta(days=1)

    timelist = pd.date_range(start=start_date,end=end_date).tolist()
    # timelist = [ts for ts in timelist if ts.weekday() < 5]
    datelist = [datetime.datetime.strftime(ts,r"%Y-%m-%d") for ts in timelist]


    for date in datelist:
        if blob_exists(blob_name=f"grouped_daily_json/{date}.json"):
            print(f"{date} file already exists")
            time.sleep(0.25)
            continue
        else:
            print(f"{date} file to be requested")
            GroupedDailyRequest(date=date,api_key=api_key,outdir=outdir)
        if date != end_date:
            time.sleep(13)

if __name__ == "__main__":
    get_grouped_daily()
