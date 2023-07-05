"""
This is a standalone script to obtain the list of tickers
"""
import os
import time
import pandas as pd
from dotenv import load_dotenv
from api_models import TickerListRequest
from pathlib import Path

def get_ticker_json():
    # get environment variables
    script_folder = Path(__file__).resolve().parent # can also use os.path.dirname(__file__)
    env_path = os.path.join(script_folder.parent,".env")
    load_dotenv(env_path)
    api_key = os.environ.get("API_KEY")
    data_folder = os.environ.get("DATA_FOLDER")
    outdir = os.path.join(data_folder,"ticker_json")

    # make dirs
    os.makedirs(data_folder,exist_ok=True)
    os.makedirs(outdir,exist_ok=True)

    # initialize variables
    page_num = 1
    cursor = None
    next_page = True

    # run requests
    while next_page:
        print(f"Get tickers page: {page_num}")
        ticker_request = TickerListRequest(api_key=api_key,page_num=page_num,cursor=cursor,outdir=outdir)
        cursor = ticker_request.get_next_cursor()
        page_num += 1
        if page_num > 1 and cursor is None:
            print("No more page to query")
            next_page = False
        
        if next_page:
            time.sleep(13)
    
    print("API calls done!")

    json_files = os.listdir(outdir)
    json_files = [file for file in json_files if "ticker" in file ]
    json_files = [os.path.join(outdir,file) for file in json_files]

    df = pd.DataFrame()
    for file in json_files:
        tmp = pd.read_json(file)
        df = pd.concat([df,tmp],ignore_index=True)
    
    df.to_csv(os.path.join(data_folder,"tickers.csv"),index=False)

if __name__ == "__main__":
    get_ticker_json()
