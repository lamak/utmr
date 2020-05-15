import logging
from collections import OrderedDict
from datetime import datetime, timedelta
from os import listdir, path, environ
from re import compile

from pymongo import MongoClient
from xmltodict import parse

from app import Utm


def humanize_date(iso_date: str) -> str:
    try:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S')

    return iso_date + timedelta(hours=7)


def add_code_to_rests(pos, rst):
    if isinstance(pos, str):
        logging.error(f'ReplyRests POS IS STR {pos}')
    else:
        alc_code = pos.get('rst:Product').get('pref:AlcCode')
        quantity = pos.get('rst:Quantity')
        rst[alc_code] = float(quantity)


def main():
    start = datetime.now()
    mongo = MongoClient()
    valid_regexp = environ.get('RESTS_REGEXP', f'({datetime.now().strftime("%y%m%d")}).*(ReplyRests)')
    valid_filename = compile(valid_regexp)
    logging.info(f'ReplyRests Processing files with REGEXP: {valid_regexp}')

    results = []

    for u in Utm.get_active():
        print(u.host, u)
        logging.info(f'ReplyRests Processing UTM: {u} {u.host}')
        try:
            files = [f for f in listdir(u.path) if valid_filename.match(f)]
        except Exception as e:
            logging.error(f'ReplyRests CANT FIND DIR {e}')
            files = []
        # legacy, in case we need error subdir too
        # for _, _, files in walk('.'):
        #     files = [fi for fi in files if valid_filename.match(fi)]
        #     files.sort(reverse=True)

        logging.info(f'ReplyRests to process: {files}')

        for reply_rests in files:
            logging.info(f'ReplyRests processing: {reply_rests}')
            print('.', end='')
            res = dict()
            if 'ReplyRestsShop' in reply_rests:
                is_retail = True
                rests_name = 'ns:ReplyRestsShop_v2'
                position_name = 'rst:ShopPosition'

            elif 'ReplyRests' in reply_rests:
                is_retail = False
                rests_name = 'ns:ReplyRests_v2'
                position_name = 'rst:StockPosition'

            else:
                raise Exception(f'Unexpected filename {reply_rests}')
            
            with open(path.join(u.path, reply_rests), encoding="utf8") as f:
                try:

                    rests_dict = parse(f.read())
                    document = rests_dict.get('ns:Documents').get('ns:Document')
                    doc_rests = document.get(rests_name)
                    rests_date = humanize_date(doc_rests.get('rst:RestsDate'))
                    doc_products = doc_rests.get('rst:Products')

                    del rests_dict
                    del doc_rests
                    del document

                    if isinstance(doc_products, OrderedDict):
                        rests = dict()
                        for position in doc_products.get(position_name):
                            add_code_to_rests(position, rests)
                    else:
                        logging.warning(f'ReplyRests {u.host} {reply_rests} not a dict')

                    res['date'] = rests_date
                    res['fsrar'] = u.fsrar
                    res['is_retail'] = is_retail
                    res['rests'] = rests

                    if not mongo.utmr.rests.find_one({'fsrar': res['fsrar'], 'date': res['date'], 'is_retail': res['is_retail']}):
                        mongo.utmr.rests.insert_one(res)
                        del res
                
                except Exception as e:
                    logging.error(f'ReplyRests SKIPPED {reply_rests} {e}')
        print(' ')

    logging.info(f'ReplyRests Done in {datetime.now() - start}')


main()
