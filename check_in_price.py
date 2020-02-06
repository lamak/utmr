import os
import re
from decimal import Decimal
from typing import List, Optional, Tuple

import cx_Oracle

os.environ["NLS_LANG"] = ".CL8MSWIN1251"

or_user = 'supermag'
or_password = 'qqq'
sm_host = '192.168.9.24'
con = cx_Oracle.connect(f'{or_user}/{or_password}@{sm_host}/STORGCO')
cur = con.cursor()

# список марок и их цен
pdf_query = """
select tms.article, smcard.name, tms.pdf417, tms.totalsum,tms.locid, tms.desknum, tms.znum, tms.checknum, tms.item, chk.cashier from SMCASHCHECKITEMS tms
left join smcashchecks chk on chk.locid = tms.locid and chk.desknum = tms.desknum and chk.znum = tms.znum and chk.checknum = tms.checknum
join smcard on tms.article = smcard.article
where tms.pdf417 like '__N%' and chk.printtime > to_date('01.10.2019', 'DD.MM.YYYY') and chk.printtime < to_date('01.01.2020', 'DD.MM.YYYY') and tms.locid in (186, 181, 214, 217, 190, 193, 194,198, 189)
"""

query_all_goods = """
select  egais.alccode, egais.article, smcard.name  from SMEGAISARTICLES egais
left join smcard on egais.article = smcard.article
where PRODUCTVCODE not in (262, 263, 261, 500, 520, 510) or productvcode is null
order by egais.article, egais.alccode
"""

cur.execute(pdf_query)
pdf_results = cur.fetchall()
print('pdf results:', len(pdf_results))

cur.execute(query_all_goods)
codes_results = cur.fetchall()
print('goods results:', len(codes_results))


def get_alccode_from_pdf417(pdf417: str) -> str:
    """ Получаем алкокод [3:19] символ из марки в формате base36 """
    return str(int(pdf417[3:19], 36)).zfill(19)


def get_volume(split_name: list) -> Optional[str]:
    """ Получаем объем, забирая последнее слово с Л на конце """
    split_name.reverse()
    for word in split_name:
        if word[-1] == 'Л':
            return word

    return None


def get_name(mask_name: str) -> Tuple[str, Optional[str]]:
    """ Получаем имена-маски из товаров (название + объем) """
    splits = mask_name.split()

    return splits[0], get_volume(splits)


# словарь ограничений
validation_dict = {
    # re.compile('^ВОДКА.*0,5Л.*'): 215,
    # re.compile('^КОНЬЯК.*0,5Л.*'): 389,
    # re.compile('^БРЕНДИ.*0,5Л.*'): 307,
    # re.compile('^ШАМПАНСКОЕ.*'): 164,
    # re.compile('^ВИНО.*ИГРИСТОЕ.*'): 164,
    # re.compile('^НАСТОЙКА.*38.*0,5Л.*'): 215,
    # re.compile('^НАСТОЙКА.*40.*0,5Л.*'): 215,
    # re.compile('^НАСТОЙКА.*39.*0,5Л.*'): 215,
    # re.compile('^ЛИКЕР.*38.*0,5Л.*'): 215,
    # re.compile('^ЛИКЕР.*40.*0,5Л.*'): 215,
    # re.compile('^ДЖИН.*40.*0,5Л.*'): 215,
    # re.compile('^БАЛЬЗАМ.*40.*0,5Л.*'): 215,
    re.compile('^АБСЕНТ.*0,5Л.*'): 308,
    re.compile('^БАЛЬЗАМ.*0,5Л.*'): 216,
    re.compile('^БАЛЬЗАМ.*0,35Л.*'): 151.50,
    re.compile('^БРЕНДИ.*0,5Л.*'): 308,
    re.compile('^БРЕНДИ.*0,7Л.*'): 430.80,
    re.compile('^ВЕРМУТ.*1Л.*'): 165,
    re.compile('^ВЕРМУТ.*0,5Л.*'): 165,
    re.compile('^ВИННЫЙ.*1Л.*'): 165,
    re.compile('^ВИНО.*0,7Л.*'): 165,
    re.compile('^ВИНО.*0,75Л.*'): 165,
    re.compile('^ВИНО.*1Л.*'): 165,
    re.compile('^ВИСКИ.*0,5Л.*'): 308,
    re.compile('^ВИСКИ.*0,35Л.*'): 215.90,
    re.compile('^ВИСКИ.*0,7Л.*'): 430.80,
    re.compile('^ВИСКИ.*0,375Л.*'): 231.25,
    re.compile('^ВИСКИ.*1Л.*'): 615,
    re.compile('^ВИСКИ.*0,75Л.*'): 461.50,
    re.compile('^ВОДКА.*0,5Л.*'): 216,
    re.compile('^ВОДКА.*0,25Л.*'): 108.50,
    re.compile('^ВОДКА.*0,45Л.*'): 216,
    re.compile('^ВОДКА.*1Л.*'): 431,
    re.compile('^ВОДКА.*0,375Л.*'): 162.25,
    re.compile('^ВОДКА.*0,7Л.*'): 302,
    re.compile('^ВОДКА.*0,1Л.*'): 44,
    re.compile('^ДЖИН.*0,5Л.*'): 308,
    re.compile('^ДЖИН.*0,75Л.*'): 461.50,
    re.compile('^КОНЬЯК.*0,35Л.*'): 272.60,
    re.compile('^КОНЬЯК.*0,5Л.*'): 389,
    re.compile('^КОНЬЯК.*0,375Л.*'): 292,
    re.compile('^КОНЬЯК.*0,25Л.*'): 195,
    re.compile('^КОНЬЯК.*0,7Л.*'): 544.20,
    re.compile('^КОНЬЯК.*0,1Л.*'): 78.60,
    re.compile('^ЛИКЕР.*0,35Л.*'): 151.50,
    re.compile('^ЛИКЕР.*0,7Л.*'): 302,
    re.compile('^ЛИКЕР.*0,5Л.*'): 216,
    re.compile('^ЛИКЕР.*1Л.*'): 431,
    re.compile('^РОМ.*0,5Л.*'): 308,
    re.compile('^РОМ.*0,7Л.*'): 430.80,
    re.compile('^ТЕКИЛА.*0,75Л.*'): 461.50,
    re.compile('^ТЕКИЛА.*0,7Л.*'): 430.80,
    re.compile('^ТЕКИЛА.*0,5Л.*'): 308,
    re.compile('^ШАМПАНСКОЕ.*0,75Л.*'): 155,
}
# собираем словарь алкокод: {артикул : название, }
alccodes_dict = dict()
for a in codes_results:
    if alccodes_dict.get(a[0]) is None:
        alccodes_dict[a[0]] = {a[1]: a[2]}
    else:
        alccodes_dict[a[0]][a[1]]: a[2]

# список марок с выделенными алкокодами
pdfs_alccodes_list = list()
for pdf in pdf_results:
    article = pdf[0]
    name = pdf[1]
    mark = pdf[2]
    price = pdf[3]
    loc = pdf[4]
    cash = pdf[5]
    znum = pdf[6]
    receipt = pdf[7]
    pos = pdf[8]
    cashier = pdf[9]
    alccode = get_alccode_from_pdf417(mark)
    pdfs_alccodes_list.append([alccode, price, article, name, mark, loc, cash, znum, receipt, pos, cashier])

# список всех пересечений алкокодов + артикулов + цена продажи
full_alc_articles_list = list()
for pdf417 in pdfs_alccodes_list:
    code = pdf417[0]
    price = pdf417[1]
    articles = alccodes_dict.get(code)
    if articles is not None:
        for k, v in articles.items():
            full_alc_articles_list.append([code, price, k, v, pdf417[4]])
    else:
        print('Алкокод не найден: ', code)

# проверка по словарю ограничений
for reg_exp, min_price in validation_dict.items():
    res = [p for p in full_alc_articles_list if (reg_exp.match(p[3]) and Decimal(p[1]) <= min_price)]
    for i in res:
        print(i)

