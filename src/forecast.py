"""
This script creates/updates small daily together with model inference step
"""
import torch
import json
import os
import sqlite3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime as dt, timedelta

from forecasting.data_prep import DataPrep
from forecasting.models import LSTMModel


def load_db_data(conn,sql_path):
    with open(sql_path,"r") as sql:
        df = pd.read_sql_query(sql.read(),conn)
    return df

def trainer(df,model_config_path,model_path,models_folder,train_config_path):
    with open(train_config_path,"r") as f:
        train_config = json.load(f)
    if dt.today().weekday() != train_config["train_start"]:
        return
    window_size = train_config["window_size"]
    epochs = train_config["epochs"]
    with open(train_config_path,"w") as f:
        json.dump(train_config,f)
    
    model = load_model(model_config_path)
    train_dataset = DataPrep(df,window_size)
    train_dataset.save_tickers(models_folder)
    train_dataloader = train_dataset.get_loader()
    print("Training starts")
    model.train_fn(train_dataloader,epochs)
    model.save_model(models_folder)
    print("Training ends")
    




def load_inference_data(df,tickers_config_path,train_config_path):
    with open(train_config_path,"r") as f:
        train_config = json.load(f)
    window_size = train_config["window_size"]
    
    inference_dataset = DataPrep(df,window_size=window_size,tickers_config_path=tickers_config_path)
    return inference_dataset

def load_model(model_config_path,model_path=None):
    with open(model_config_path) as f:
        model_config = json.load(f)
    model = LSTMModel(**model_config)
    if model_path:
        model.load_model(model_path)
    return model

def predict_historical_data(model, dataset):
    # historical index
    historical_index = dataset.data_index[len(dataset.data_index)-len(dataset.windows):]

    # get full loader
    loader = dataset.get_loader(batch_size=len(dataset.windows),shuffle=False)
    pred = model.predict(next(iter(loader)))

    # convert to row format
    tickers = list(dataset.tickers_config.keys())
    pred = pd.DataFrame(pred.numpy(),columns=tickers,index=historical_index)
    cols = ["date","ticker","close_price"]
    pred_historical = pd.DataFrame(columns=cols)
    for ix, row in pred.iterrows():
        for ticker in tickers:
            tmp = pd.DataFrame([[ix,ticker+"_pred",row[ticker]]],columns=cols)
            pred_historical = pd.concat([pred_historical,tmp],ignore_index=True,axis=0)

    return pred_historical

def forecast_next_n(model,dataset,n=5):
    # generate next n business day (assume Monday through Friday)
    def convert_to_datetime(s):
        format = '%Y-%m-%d'
        datetime_str = dt.strptime(s, format)
        return datetime_str
    def convert_from_datetime(dt_str):
        format = '%Y-%m-%d'
        s = dt.strftime(dt_str, format)
        return s
    day = convert_to_datetime(dataset.data_index[-1])
    inference_index = []
    for _ in range(n):
        business_check = False
        while not business_check:
            day = day + timedelta(days=1)
            if day.weekday() <= 4:
                inference_index.append(convert_from_datetime(day))
                business_check = True

    cols = ["date","ticker","close_price"]
    forecast = pd.DataFrame(columns=cols)

    window_size = dataset[0].shape[0] # (T,C)
    forecast_window = dataset.data[-window_size:]
    tickers = list(dataset.tickers_config.keys())
    for i, date in enumerate(inference_index):
        if i != 0:
            # update forecast_window
            forecast_window = torch.cat([forecast_window,pred])[1:,:]
        
        # print(forecast_window.shape)
        # normalize forecast_window
        norm = forecast_window.detach().clone()
        for j, ticker in enumerate(tickers):
            norm[:,j] = (forecast_window[:,j] - dataset.tickers_config[ticker]["mean"])/dataset.tickers_config[ticker]["std"]
        pred = model.predict(norm.unsqueeze(0))

        pred_frame = pd.DataFrame(pred.numpy(),columns=tickers) # tmp only has one like
        for j, ticker in enumerate(tickers):
            tmp = pd.DataFrame([[date,ticker+"_pred",pred_frame.iloc[0,j]]],columns=cols)
            forecast = pd.concat([forecast,tmp],ignore_index=True,axis=0)
    
    return forecast


def write_to_db(conn,dataset,df,forecast,pred_historical = None):
    # write to db
    if pred_historical is None:
        last = dataset.data_index[-1]
        cols = ["date","ticker","close_price"]
        tickers = list(dataset.tickers_config.keys())
        for ticker in tickers:
            row_id = df.index[(df["date"]==last) & (df["ticker"]==ticker)].tolist()[0]
            tmp = pd.DataFrame([[last,ticker+"_pred",df.loc[row_id,"close_price"]]],columns=cols)
            df = pd.concat([df,tmp],ignore_index=True,axis=0)

    out = pd.concat([df,forecast],ignore_index=True,axis=0)

    if pred_historical is not None:
        out = pd.concat([out,pred_historical],ignore_index=True,axis=0)
    
    out.to_sql(name="small_daily",con=conn, if_exists="replace",index=False)
    return out

def main():
    print("Start forecasting")
    script_folder = Path(__file__).resolve().parent
    sql_scripts_folder = os.path.join(script_folder,"sql_scripts")
    env_path = os.path.join(script_folder.parent,".env")
    load_dotenv(env_path)
    data_folder = os.environ.get("DATA_FOLDER")
    models_folder = os.path.join(data_folder,"models")

    db_file = os.path.join(data_folder,"stock.db")
    conn = sqlite3.connect(db_file)
    sql_path = os.path.join(sql_scripts_folder,"query_daily.sql")
    tickers_config_path = os.path.join(models_folder,"tickers_config.json")
    train_config_path = os.path.join(models_folder,"train_config.json")
    model_path = os.path.join(models_folder,"model.pt")
    model_config_path = os.path.join(models_folder,"model_config.json")

    # load data and create initial inference dataset
    df = load_db_data(conn,sql_path)

    # trainer will also check date
    trainer(df,model_config_path,model_path,models_folder,train_config_path)
    
    # load inference dataset
    dataset = load_inference_data(df,tickers_config_path,train_config_path)
    # load model artifact
    model = load_model(model_config_path,model_path)

    # predict historical data
    pred_historical = predict_historical_data(model,dataset)
    # forecast
    forecast = forecast_next_n(model,dataset)
    # write to db
    write_to_db(conn,dataset,df,forecast,pred_historical)

    print("End forecasting")


if __name__ == "__main__":
    main()






