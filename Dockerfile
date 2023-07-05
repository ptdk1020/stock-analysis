FROM google/cloud-sdk:slim

WORKDIR /stockapp
COPY .env requirements.txt ./
ADD /src ./src
ADD /shell_scripts ./shell_scripts

RUN mkdir -p ./data ./data/ticker_json ./data/grouped_daily_json ./data/models
RUN pip3 install -r requirements.txt
RUN pip3 install torch --index-url https://download.pytorch.org/whl/cpu

RUN chmod +x ./shell_scripts/*.sh

CMD ["./shell_scripts/run.sh"]

