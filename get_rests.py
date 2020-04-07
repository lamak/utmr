import os

import cx_Oracle
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def add_or_update(d, element, value):
    val = d.get(element)
    if val is None:
        d[element] = value
    else:
        val.update(value)


conn = os.environ.get('ORA_CONN')
con = cx_Oracle.connect(conn)
cur = con.cursor()

fsrar = os.environ.get('FSRAR')
fsrar = fsrar.split(',')
fsrar = [el.strip() for el in fsrar]

print(f'FSRAR to SAVE: {fsrar}')
for fsrar_id in fsrar:

    sql = f"""
            SELECT distinct sp.PRODUCTALCCODE,
                            sp.INFORMBREGID,
                            SUM(CASE WHEN DOCTYPE = 'WBTransferToShop' THEN quantity ELSE -QUANTITY END) AS ttl
            from SMEGAISDOCHEADER hd
                left join SMEGAISDOCSPEC sp on hd.GLID = sp.GLID and hd.BORNIN = sp.BORNIN
            where hd.OURFSRARID = '{fsrar_id}'
                and hd.doctype in ('WBTransferFromShop', 'WBTransferToShop')
                and hd.DOCSTATE in (32, 42)
                and sp.PRODUCTVCODE in (500, 510, 520, 261, 262, 263)
            GROUP BY sp.PRODUCTALCCODE, sp.INFORMBREGID
            order by sp.PRODUCTALCCODE, ttl desc
            """
    cur.execute(sql)
    rests = cur.fetchall()
    res = {}
    for row in rests:
        add_or_update(res, row[0], {row[1]: row[2]})
    total_source_list = sum([x[2] for x in rests])
    total_result_dict = sum([sum([qty for qty in code.values()]) for code in res.values()])

    if total_result_dict != total_source_list:
        print(f'ERROR SOURCE QTY != RES QTY [ {total_source_list} != {total_result_dict}', end='... ')
    else:
        print(f'{fsrar_id}: {total_result_dict}', end='... ')

    record = {
        'fsrar': fsrar_id,
        'quantity': total_result_dict,
        'rests': res,
    }
    try:
        with MongoClient(os.environ.get('MONGO_CONN', 'localhost:27017')) as client:
            col = client[os.environ.get('MONGO_DB', 'utm')][os.environ.get('MONGO_COL', 'tts')]
            col.insert_one(record)
            print('RESTS SAVED SUCCESS')

    except Exception as e:
        print(f'ERROR CANT SAVE RESULTS TO DB: {e}')
