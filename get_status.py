from datetime import datetime
from time import sleep

from pymongo import MongoClient

from app import Configs, grab_utm_check_results_to_db
from config import AppConfig

with MongoClient(AppConfig.MONGO_CONN) as cl:
    db = cl[AppConfig.MONGO_DB]
    cfg = Configs(db)

    while True:
        results = grab_utm_check_results_to_db(cfg.utms, db[AppConfig.MONGO_COL_RES])
        print(f'{datetime.now()} DONE: {len(results)}')
        sleep(60)
