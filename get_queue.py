from datetime import datetime
from glob import glob
from os import listdir, path
from time import sleep

from pymongo import MongoClient

from config import AppConfig


def get_files(storage: str):
    """ Необработанные файлы в обмене за сегодняший день
    """
    subs = ['in', 'out']
    results = dict()
    mask = f'{datetime.now().strftime("%y%m%d")}*.xml'
    for entry in listdir(storage):
        shop_path = storage + entry
        if path.isdir(shop_path):
            result = {act: [file.split('\\')[-1] for file in glob(f'{shop_path}/{act}/{mask}')] for act in subs
                      if glob(f'{shop_path}/{act}/{mask}')}
            if result:
                results[entry] = result
    return results


def check_queue(xml_path: str):
    results = dict()
    unprocessed_files = get_files(xml_path)
    results['files'] = unprocessed_files
    results['total'] = len(unprocessed_files)
    results['date'] = datetime.now()
    return results


def main():
    with MongoClient(AppConfig.MONGO_CONN) as cl:
        col = cl[AppConfig.MONGO_DB][AppConfig.MONGO_COL_QUE]

        while True:
            try:
                result = check_queue(AppConfig.DEFAULT_XML_PATH)
                col.insert_one(result)
                print(f'{result["date"]} DONE {result["total"]}')

            except Exception as e:
                print(f'PostMan: не удалось записать в БД: {e}')
            sleep(5 * 60)


main()
