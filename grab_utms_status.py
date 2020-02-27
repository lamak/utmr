import os
from time import sleep

from pymongo import MongoClient

from app import Configs, parse_utm, mongo_prev_results

# Mongo Setup
mongo_conn = os.environ.get('MONGODB_CONN', 'localhost:27017')
client = MongoClient(mongo_conn)
mongodb = client.tempdb
cfg = Configs(mongodb)

while True:
    results = [parse_utm(utm) for utm in cfg.utms]
    try:
        mongo_prev_results(mongodb)
        print(f'Предыдущие результаты помечены архивным')
        [r.to_db(mongodb) for r in results]
        print(f'Записаны результы {len(results)}')
    except:
        print(f"Не удалось записать результаты в БД")
    sleep(30)