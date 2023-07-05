"""
Script to insert/combine into a database table
The database is simulated by a sqllite file
"""
import os
import pandas as pd
import sqlite3
from dotenv import load_dotenv
from pathlib import Path

class ETL:
    def __init__(self, data_folder,script_folder):
        self.data_folder = data_folder
        self.script_folder = script_folder
        # json folders
        self.ticker_json_folder = os.path.join(data_folder,"ticker_json")
        self.grouped_daily_json_folder = os.path.join(data_folder,"grouped_daily_json")

        # sql scripts folder
        self.sql_scripts = os.path.join(self.script_folder, "sql_scripts")

        # make dirs if not exists
        os.makedirs(self.data_folder,exist_ok=True)
        os.makedirs(self.ticker_json_folder,exist_ok=True)
        os.makedirs(self.grouped_daily_json_folder,exist_ok=True)

        # sqlite tablenames
        self.ticker_tablename = "tickers"
        self.daily_tablename = "daily"

        # database
        self.database = "stock.db"
        self.conn = None
        
    def connectOrCreateDatabase(self):
        # Assuming that the db file is stored on the VM
        db_file = os.path.join(self.data_folder,self.database)
        self.conn = sqlite3.connect(db_file)
    
    def createTable(self,sql_script):
        self.connectOrCreateDatabase()
        
        sql_path = os.path.join(self.sql_scripts,sql_script)
        with open(sql_path,"r") as f:
            self.conn.cursor().executescript(f.read())
        
        self.conn.commit()
        self.conn.close()

    def updateTickerTable(self):
        self.connectOrCreateDatabase()
        json_files = os.listdir(self.ticker_json_folder)
        json_files = [file for file in json_files if "ticker" in file]
        json_files = [os.path.join(self.ticker_json_folder,file) for file in json_files]

        for file in json_files:
            df = pd.read_json(file)
            if len(df) == 0:
                continue
            try:
                df.to_sql(name="tickers",con=self.conn, if_exists="append",index=False)
            except:
                print("Upsert debug: key exists")
        
        self.conn.commit()
        self.conn.close()

    def updateDailyTable(self):
        self.connectOrCreateDatabase()
        json_files = os.listdir(self.grouped_daily_json_folder)
        json_files = [file for file in json_files if "full" not in file]
        json_files = [os.path.join(self.grouped_daily_json_folder,file) for file in json_files]

        rename_dict = {
            "T":"ticker"
            , "o":"open_price"
            , "c":"close_price"
            , "h":"highest_price"
            , "l":"lowest_price"
            , "n":"number_of_transactions"
            , "v":"trading_volume"
            , "vw":"volume_weighted_average_price"
            , "otc":"otc_sticker"
            , "t":"window_start_timestamp"
        }

        for file in json_files:
            df = pd.read_json(file)
            if len(df) == 0:
                continue
            df.rename(columns=rename_dict,inplace=True)
            df["date"] = Path(file).stem
            df.to_sql(name="daily",con=self.conn, if_exists="append",index=False)


if __name__ == "__main__":
    print("Start etl grouped daily")
    script_folder = Path(__file__).resolve().parent # can also use os.path.dirname(__file__)
    env_path = os.path.join(script_folder.parent,".env")
    load_dotenv(env_path)
    data_folder = os.environ.get("DATA_FOLDER")
    etl = ETL(data_folder=data_folder,script_folder=script_folder)
    # etl.createTable("create_tickers.sql")
    # etl.updateTickerTable()
    etl.createTable("create_daily.sql")
    etl.updateDailyTable()
    print("End etl grouped daily")

    


