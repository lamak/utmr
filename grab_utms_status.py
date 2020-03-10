import os
from time import sleep

from dotenv import load_dotenv
from pymongo import MongoClient

from app import Configs, grab_utm_check_results_to_db

# Mongo Setup
load_dotenv()
mongo_conn = os.environ.get('MONGODB_CONN', 'localhost:27017')
client = MongoClient(mongo_conn)
mongodb = client.tempdb
cfg = Configs(mongodb)

while True:
    results = grab_utm_check_results_to_db(cfg.utms, mongodb)
    print(f'Записаны результы {len(results)}')
    sleep(60)
