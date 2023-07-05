import torch
from torch import nn
import os
import json

class LSTMModel(nn.Module):
    def __init__(self, input_size,hidden_size):
        torch.manual_seed(0)
        super().__init__()
        self.config = {"input_size":input_size,"hidden_size":hidden_size}
        self.lstm_layer = nn.LSTM(input_size=input_size, hidden_size=hidden_size,batch_first=True)
        self.fc = nn.Linear(hidden_size, input_size)

    def forward(self, x):
        _, x = self.lstm_layer(x)
        x = self.fc(x[0]).squeeze(0)
        return x

    def train_fn(self,dataloader,epochs=1000):
        torch.manual_seed(0)
        self.train()
        loss_fn = nn.L1Loss()
        optimizer = torch.optim.Adam(params=self.parameters(),lr=0.01)
        for epoch in range(epochs):
            train_iter = iter(dataloader)
            epoch_loss = 0
            for batch, (X,y) in enumerate(train_iter):
                optimizer.zero_grad()
                yhat = self(X)
                loss = loss_fn(yhat,y)
                epoch_loss += loss.item()
                loss.backward()
                optimizer.step()
            epoch_loss = epoch_loss/len(train_iter)
            if epoch % 100 == 0:
                print(f"Epoch {epoch} loss: {epoch_loss}")

    def predict(self,x):
        self.eval()
        with torch.inference_mode():
            pred = self(x)
        return pred
    
    def save_model(self,save_folder):
        # save state_dict
        torch.save(self.state_dict(),os.path.join(save_folder,"model.pt"))
        # save config
        with open(os.path.join(save_folder,"model_config.json"),"w") as f:
            json.dump(self.config,f)
        


    def load_model(self,model_path):
        self.load_state_dict(torch.load(model_path))
    
    