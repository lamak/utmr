from datetime import datetime
from glob import glob
from os import listdir, path, environ
from time import sleep

from dotenv import load_dotenv
from pymongo import MongoClient


def get_files_queue(storage: str):
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


def process_res(xml_path):
    results = dict()
    tmp = get_files_queue(xml_path)
    results['files'] = tmp
    results['total'] = len(tmp)
    results['date'] = datetime.now()
    return results


def main():
    load_dotenv()
    mongo_conn = environ.get('MONGODB_CONN', 'localhost:27017')
    xml_path = environ.get('DEFAULT_XML_PATH', 'egais-exch/')
    with MongoClient(mongo_conn) as cl:
        col = cl['tempdb']['queue']

        while True:
            try:
                scan_queue = process_res(xml_path)
                col.insert_one(scan_queue)
                print(f'{scan_queue["date"]} DONE {scan_queue["total"]}')

            except Exception as e:
                print(f'PostMan: не удалось записать в БД: {e}')
            sleep(5 * 60)


main()
