from torch.utils.data import Dataset, DataLoader
import torch
import os
import json

def normalize(s):
    return (s-s.mean())/s.std(), s.mean(), s.std()

class DataPrep(Dataset):
    def __init__(self,df,window_size,tickers_config_path=None):
        self.window_size = window_size
        self.data_index = df["date"].sort_values().unique().tolist()
        self.tickers_config_path = tickers_config_path
        if not tickers_config_path:
            self.tickers_config, self.data, self.data_normalized = self._generate_train_data(df)
            self.windows_targets = self._generate_windowstargets()
        else:
            with open(tickers_config_path) as f:
                self.tickers_config = json.load(f)
            # in inference time, we don't need the data for target
            self.data, self.data_normalized = self._generate_inference_data(df)
            self.windows = self._generate_windows()

        
    
    def _generate_inference_data(self,df):
        tensors = []
        tensors_normalized = []
        for ticker in self.tickers_config:
            series = df[df["ticker"]==ticker][["date","close_price"]]
            series.sort_values(by="date",inplace=True)
            series.set_index("date",inplace=True)
            series = series["close_price"]

            tensor = torch.tensor(series.values, dtype=torch.float32).unsqueeze(1)
            tensors.append(tensor)

            series_normalized = (series - self.tickers_config[ticker]["mean"])/self.tickers_config[ticker]["std"]
            tensor_normalized = torch.tensor(series_normalized.values, dtype=torch.float32).unsqueeze(1)
            tensors_normalized.append(tensor_normalized)
        return torch.cat(tensors, dim = -1),torch.cat(tensors_normalized, dim = -1)
    
    def _generate_windows(self):
        return [self.data_normalized[i:i+self.window_size] for i in range(len(self.data_normalized)-self.window_size)]

    def _generate_train_data(self, df):
        tickers = sorted(df["ticker"].unique().tolist())
        tickers_config = {}
        tensors = []
        tensors_normalized = []
        for ticker in tickers:
            series = df[df["ticker"]==ticker][["date","close_price"]]
            series.sort_values(by="date",inplace=True)
            series.set_index("date",inplace=True)
            series = series["close_price"]
            self.data_index = series.index.tolist()
            
            tensor = torch.tensor(series.values, dtype=torch.float32).unsqueeze(1)
            tensors.append(tensor)
            series_normalized, series_mean, series_std = normalize(series)
            # record mean and std for later inference
            tickers_config[ticker] = {"mean":series_mean,"std":series_std}

            tensor_normalized = torch.tensor(series_normalized.values, dtype=torch.float32).unsqueeze(1)
            tensors_normalized.append(tensor_normalized)
        return tickers_config, torch.cat(tensors, dim = -1), torch.cat(tensors_normalized, dim = -1)
    
    def _generate_windowstargets(self):
        return [
            (self.data_normalized[i:i+self.window_size],
             self.data[i+self.window_size]
            ) for i in range(len(self.data_normalized)-self.window_size)
        ]
    
    def __len__(self):
        if not self.tickers_config_path:
            return len(self.windows_targets)
        else:
            return len(self.windows)

    def __getitem__(self,ix):
        if not self.tickers_config_path:
            window, target = self.windows_targets[ix]
            return window, target
        else:
            window = self.windows[ix]
            return window
        
    
    def get_loader(self,batch_size=8):
        dataloader = DataLoader(self, batch_size=batch_size, shuffle=True)
        return dataloader
    
    def save_tickers(self,save_folder):
        with open(os.path.join(save_folder,"tickers_config.json"),"w") as f:
            json.dump(self.tickers_config,f)
        
        




        
    
