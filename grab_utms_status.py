import os
from datetime import datetime
from time import sleep

from pymongo import MongoClient

from app import Configs, grab_utm_check_results_to_db

# Mongo Setup
mongo_conn = os.environ.get('MONGODB_CONN', 'localhost:27017')
database = os.environ.get('MONGO_DB', 'tempdb')

with MongoClient(mongo_conn) as client:
    db = client[database]
    cfg = Configs(db)
    while True:
        results = grab_utm_check_results_to_db(cfg.utms, db)
        print(f'{datetime.now()} DONE: {len(results)}')
        sleep(60)
