#!/bin/sh
echo $(date +"Container run started at: %c")
python3 /stockapp/src/get_grouped_daily.py

gsutil -m cp -r -n /stockapp/data/grouped_daily_json gs://stock-data-project-khoa
gsutil cp gs://stock-data-project-khoa/stock.db /stockapp/data/stock.db
gsutil -m cp -r gs://stock-data-project-khoa/models /stockapp/data

python3 /stockapp/src/etl_grouped_daily.py
python3 /stockapp/src/forecast.py
python3 /stockapp/src/bigquery.py


gsutil cp -r /stockapp/data/models gs://stock-data-project-khoa
gsutil cp /stockapp/data/stock.db gs://stock-data-project-khoa

rm /stockapp/data/grouped_daily_json/*
rm /stockapp/data/models/*
rm /stockapp/data/stock.db
echo $(date +"Container run ended at: %c")