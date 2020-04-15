import os
import sys
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime
from pprint import pprint
from typing import Tuple
from uuid import uuid4

import click
import cx_Oracle
import pandas as pd
import xml.dom.minidom
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

DEBUG = os.environ.get('DEBUG')


@click.command()
@click.option('--invent', prompt='Process ID', help='PID')
def allocate_rests(invent):
    """Генерация XML перевода Р2 -> Р1 на основе имеющихся остатков и факт пересчета
    * R2 — регистр 2, торговый зал, обычно это остатки в разрезе алкокод : кол-во
    * RFU2 — справка для перевода Р2 -> Р1, пара алкокод-рфу2 дает остаток нет в Р2, нет в Ивентаризации
    * Invent — инвентаризация, пересчет — фактическое кол-во алкокодов и марок
    * FSRAR — 12 значный идентификатор, строка
    *
    """

    def setting_cursor():
        oracle_host = os.environ.get('ORACLE_HOST')
        oracle_name = os.environ.get('ORACLE_NAME')
        oracle_user = os.environ.get('ORACLE_USER')
        oracle_pass = os.environ.get('ORACLE_PASS')

        ora_params = [oracle_name, oracle_host, oracle_pass, oracle_user]
        if not all(ora_params):
            print(f'SET ORACLE ENV')
            sys.exit(1)

        conn = f'{oracle_user}/{oracle_pass}@{oracle_host}/{oracle_name}'
        if DEBUG:
            print(f'USING DB: {conn}')
        try:
            con = cx_Oracle.connect(conn)
            return con.cursor()

        except Exception as e:
            print(f'CANT CONNECT {e}')
            sys.exit(1)

    def fetch_results(sql: str, cursor: cx_Oracle.Cursor):
        cursor.execute(sql)
        return cursor.fetchall()

    def indexed_df_to_nested_dict(frame):
        """ Приводим датафрейм к словарю вида {alc_code : {rfu2 : quantity, ...}, ... } """
        if len(frame.columns) == 1:
            if frame.values.size == 1:
                return frame.values[0][0]
            return frame.values.squeeze()
        grouped = frame.groupby(frame.columns[0])
        d = {k: indexed_df_to_nested_dict(g.iloc[:, 1:]) for k, g in grouped}
        return d

    def merge_rests_dicts(current: dict, extra: dict) -> dict:
        """ Объединение текущих и дополнительных остатков """
        print(f'ORIGIN RESTS QTY {sum([sum(x.values()) for x in current.values()])}')
        print(f'EXTRA  RESTS QTY {sum([sum(x.values()) for x in extra.values()])}')
        for code, f2_qty in extra.items():
            current_code = current.get(code)
            if current_code is not None:
                # merge 2 dicts rfu2 : qty
                for extra_rfu2, extra_q in f2_qty.items():
                    current_rfu2 = current_code.get(extra_rfu2)
                    if current_rfu2 is not None:
                        current_code[extra_rfu2] = extra_q + current_rfu2
                        if DEBUG:
                            print(f'+++ ADDED QTY {extra_q} TO {code} : {extra_rfu2}')
                    else:
                        current_code[extra_rfu2] = extra_q
                        if DEBUG:
                            print(f'++ ADDED RFU {extra_rfu2} : {extra_q} TO {code} ')

            else:
                current[code] = f2_qty
                if DEBUG:
                    print(f'+ ADDED CODE {code} WITH F2 {len(f2_qty)} QTY {sum(f2_qty.values())}')
        print(f'TOTAL  RESTS QTY {sum([sum(x.values()) for x in current.values()])}')

        return current

    def allocation_rests_on_rfu2(total_rests: dict, invent_rests: dict, ) -> Tuple[dict, dict]:
        """ Распределяем имеющиеся остатки (из инвентаризации, invent) на расчитанные остатки по справкам  (rests)"""
        total_rests = deepcopy(total_rests)
        result = {}
        out_stock = {}

        print("PROCESSING CODES...")
        for alc_code, qty in invent_rests.items():
            if DEBUG:
                print(f'ACODE {alc_code} : {qty}')
            qty = int(qty)
            result[alc_code] = {}
            rest_alc = total_rests.get(alc_code)
            if rest_alc is not None:
                for f2, f2_qty in rest_alc.items():
                    if qty and f2_qty:
                        if qty >= f2_qty:
                            qty = qty - f2_qty
                            rest_alc[f2] = 0
                            result[alc_code][f2] = f2_qty
                            if DEBUG:
                                print(f'++ ADDED {f2} DEPLETED WITH {f2_qty}, REMAIN TO ALLOCATE {qty}')
                        else:
                            rest_alc[f2] = f2_qty - qty
                            result[alc_code][f2] = qty
                            if DEBUG:
                                print(f'+ ADDED {f2} ACODE FULLFILED WITH {qty} (WAS {f2_qty})')
                            qty = 0
                if qty > 0:
                    out_stock[alc_code] = qty
                    if DEBUG:
                        print(f'- NOT DONE: {qty}')
            else:
                out_stock[alc_code] = qty
                print(f'WARNING {alc_code} : {qty} pcs. NOT IN RESTS AT ALL')

        return result, out_stock

    def allocate_mark_codes_to_rfu2(rests: dict, mark_codes: dict) -> Tuple[dict, dict]:
        """ Распределяем марки на справки РФУ2, вида {alccode: {rfu2 : [mark, ...], ...},...} """
        rests = deepcopy(rests)
        rfu2_marks = {}
        out_stock = {}

        print("PROCESSING MARKS...")
        for alc_code, marks in mark_codes.items():
            rfu2_marks[alc_code] = {}
            rfu2_rests = rests.get(alc_code)
            if rfu2_rests is not None:
                rfu2_total = sum(rfu2_rests.values())
                alc_code_qty = len(marks)
                if DEBUG:
                    print(f'ACODE: {alc_code}, MARKS {alc_code_qty}, RFU2s: {len(rfu2_rests)}, AVL: {rfu2_total}')

                    if rfu2_total < alc_code_qty:
                        print(f'WARNING AVL: {rfu2_total}, REQUIRED {alc_code_qty}')

                for rfu, qty in rfu2_rests.items():
                    qty = int(qty)
                    if marks and qty:
                        rfu2_marks[alc_code][rfu] = marks[:qty]
                        marks = marks[qty:]

            if marks:
                out_stock[alc_code] = marks
                print(f'WARNING OUTSTOCK {alc_code} with {len(marks)}: {marks}')

        return rfu2_marks, out_stock

    def r2_rests_control(r2: dict, invented: dict, marks: dict) -> Tuple[dict, dict, dict]:
        """ Проверка количества посчитанного на превышение фактических остатков на Р2,
        При превышении отсекаем лишнее (кол-во + марки), выводя в лог """
        out_rests = {}
        for alc_code, qty in invented.items():
            r2_qty = r2.get(alc_code)
            if r2_qty is not None:
                r2_qty = int(r2_qty)
                if r2_qty < qty:
                    lack = int(qty) - r2_qty
                    invented[alc_code] = r2_qty
                    out_rests[alc_code] = {
                        'quantity': lack,
                        'marks': marks[alc_code][r2_qty:]
                    }

                    marks[alc_code] = marks[alc_code][:r2_qty]
                    print(f'WARNING NOT ENOUGH RESTS: {alc_code} DIFF QTY {lack}, MARKS {out_rests[alc_code]["marks"]}')

            else:
                print(f"WARNING NOT IN RESTS: {alc_code}, QTY {qty}")

        for k, v in invented.items():
            if not v:
                del invented[k]

        return invented, marks, out_rests

    def process_rests_data(fsrar_id: str, process_id: str) -> Tuple[dict, dict, dict, dict]:
        """ Получаем остатки и пересчет из Oracle, обрабатываем и возвращаем словари доступны остатки, факт

         Описание процесса:
         1. Получаем РФУ2 остатки по TransferToShop - TransferFromShop
           1.1 Опционально подгружаем остатки сохраненные из другого хранилища
           1.2 Если TTS-TFS не в полном объемЕ, получаем из разница TTN приход - расход - марки
         2. Сверяем полученные РФУ2 остатки с фактом Р2, лишнее вывводим в лог, отсекаем
         3. С РФУ2 остатками сопоставляем инвентаризацию
         4. Формируем TransferFromShop Р2 -> Р1
         5. Формируем ActBarCodeFix для фиксации марок на Р3
         6. Формируетм SQL Insert для отображения посчитанных марок в Супермаге

         Шаги 5,6 опциональны, если расхожедний не выявлено, можно обновить остатки и закончить в Супермаг
        """

        tts_tfs = f"""
        select distinct sp.PRODUCTALCCODE,
                        sp.INFORMBREGID,
                        SUM(CASE WHEN DOCTYPE = 'WBTransferToShop' THEN quantity ELSE -QUANTITY END) AS ttl
        from SMEGAISDOCHEADER hd
            left join SMEGAISDOCSPEC sp on hd.GLID = sp.GLID and hd.BORNIN = sp.BORNIN
        where hd.OURFSRARID = '{fsrar_id}'
            and hd.doctype in ('WBTransferFromShop', 'WBTransferToShop')
            and hd.DOCSTATE in (32, 42)
            and sp.PRODUCTVCODE not in (500, 510, 520, 261, 262, 263)
        GROUP BY sp.PRODUCTALCCODE, sp.INFORMBREGID
        order by sp.PRODUCTALCCODE, ttl desc, sp.INFORMBREGID desc
        """

        income_ttn = f"""
        select distinct productalccode, informbregid, sum(quantity) quantity  from smegaisdocspec spec
        left join smegaisdocheader hd on spec.glid = hd.glid and spec.bornin = hd.BORNIN-- шапка с фсрарид, датой, хедером
        left join SMEGAISDOCSPECACT act on spec.glid = act.glid and spec.identity = act.identity and spec.BORNIN = act.BORNIN-- марки
        where hd.ourfsrarid = '{fsrar_id}' 
            and docstate = 6  -- успешно завершенные
            and doctype = 'WBInvoiceToMe' -- приходные накладные от поставщика
            and informbregid is not Null -- обязательно указанием справки 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) -- слабоалкогольная продукция

        group by productalccode, informbregid
        order by quantity desc, productalccode, informbregid
        """

        return_ttn = f"""
        select distinct productalccode,  f2regid, sum(quantity) quantity from smegaisdocspec spec
        left join smegaisdocheader hd on spec.glid = hd.glid and spec.BORNIN = hd.BORNIN
        left join smegaisdocspecf2 f2 on spec.glid=f2.glid and spec.identity = f2.identity and spec.BORNIN = f2.BORNIN
        where hd.ourfsrarid = '{fsrar_id}' 
            and docstate = 17 -- завершенные
            and doctype = 'WBReturnFromMe' -- возвраты
            and f2regid is not Null -- обязательно указана справка 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) 
        group by productalccode, f2regid
        order by quantity desc, productalccode, f2regid
        """

        invent_codes = f"""
        select alccode, sum(quantity) from smegaisprocessegoabheader hd
        left join SMEGAISPROCESSEGOABSPEC rst on hd.processid = rst.processid and hd.processtype = rst.processtype
        where hd.processid = {process_id} 
            and hd.processtype = 'EGOA' -- процесс инвентаризации крепкоалкогольной
            and length(rst.markcode) = 68 -- длина старой АМ
            AND rst.markcode not in (select markcode from SMEGAISRESTSPIECE) -- исключаем марки на Р3
        group by alccode
        """

        invent_marks = f"""
        SELECT alccode, rst.markcode
        FROM smegaisprocessegoabheader hd 
        LEFT JOIN smegaisprocessegoabspec rst ON hd.processid = rst.processid AND hd.processtype = rst.processtype
        WHERE hd.processid = {process_id}
            AND hd.processtype = 'EGOA'
            AND length(rst.markcode) = 68
            AND rst.markcode not in (select markcode from SMEGAISRESTSPIECE)
        """

        rests_r2_codes = f"""
        SELECT alccode, quantity
        FROM smegaisrests
        WHERE ourfsrarid = {fsrar_id} 
            and isretail = 1 -- остатки на Р2 (торговый зал)
            and productvcode NOT IN (500, 510, 520, 261, 262, 263) -- (слабоалкогольная продукция)
            """

        rests_r3_marks = f"""
        select alccode, informbregid, count(markcode) from SMEGAISRESTSPIECE
        where ourfsrarid = '{fsrar_id}'
        group by alccode, informbregid
        order by informbregid, alccode
        """

        # инвентаризация: алкокода и количество
        inv_pd = pd.DataFrame.from_records(fetch_results(invent_codes, cur))
        if not inv_pd.empty:
            inv_pd.columns = ['alccode', 'quantity']
            inv_pd.set_index(['alccode'])
        else:
            print("В инвентаризации нет алкококодов со старыми марками, продолжение невозможно")
            sys.exit(1)

        # инвентаризация: алкокода и марки
        inv_marks_pd = pd.DataFrame.from_records(fetch_results(invent_marks, cur))
        if not inv_marks_pd.empty:
            # нет старых марок в инвентаризации, нет фиксации и актов
            inv_marks_pd.columns = ['alccode', 'markcode']
            inv_marks_pd.set_index(['alccode', ])
        else:
            print("В инвентаризации нет алкококодов со старыми марками, продолжение невозможно")
            sys.exit(1)

        extra_tts_rests = {}

        if os.environ.get('TTS'):
            # переводы в зал, с учетом возвратов
            print('USING TTS')
            rests_rfu2_pd = pd.DataFrame.from_records(fetch_results(tts_tfs, cur))
            if rests_rfu2_pd.empty:
                print("Не переводов Р1 -> Р2, продолжение невозможно")
                sys.exit(1)

            try:
                print(f'TRYING GET EXTRA RESTS FOR {fsrar_id}')
                with MongoClient(os.environ.get('MONGO_CONN', 'localhost:27017')) as client:
                    col = client[os.environ.get('MONGO_DB', 'utm')][os.environ.get('MONGO_TTS', 'tts')]
                    extra_tts_rests = col.find_one({'fsrar': fsrar_id})

            except Exception as e:
                print(f'CANT CONNECT TO DB NO EXTRA RESTS')

        else:
            # ttn: приход, алкокода, справки, количество
            print('USING TTN')
            rests_rfu2_pd = pd.DataFrame.from_records(fetch_results(income_ttn, cur))
            if not rests_rfu2_pd.empty:
                rests_rfu2_pd.columns = ['alccode', 'f2', 'total', ]
                rests_rfu2_pd.set_index(['alccode', 'f2'])

                # ttn: расход, алкокода справки и количество
                out_pd = pd.DataFrame.from_records(fetch_results(return_ttn, cur))
                if not out_pd.empty:
                    out_pd.columns = ['alccode', 'f2', 'quantity']
                    out_pd.set_index(['alccode', 'f2'])
                    rests_rfu2_pd = rests_rfu2_pd.merge(out_pd, on=['alccode', 'f2'], how='outer')
                    rests_rfu2_pd['total'] = rests_rfu2_pd['total'].fillna(0) - rests_rfu2_pd['quantity'].fillna(0)
                    rests_rfu2_pd = rests_rfu2_pd.drop(columns=['quantity', ])
                else:
                    print("Нет расходов по крепко алкогольной продукции")

                # остатки Р3: алкокода, справки, количество
                f3_pd = pd.DataFrame.from_records(fetch_results(rests_r3_marks, cur))
                if not f3_pd.empty:
                    f3_pd.columns = ['alccode', 'f2', 'quantity']
                    f3_pd.set_index(['alccode', 'f2'])
                    rests_rfu2_pd = rests_rfu2_pd.merge(f3_pd, on=['alccode', 'f2'], how='outer')
                    rests_rfu2_pd['total'] = rests_rfu2_pd['total'].fillna(0) - rests_rfu2_pd['quantity'].fillna(0)
                    rests_rfu2_pd = rests_rfu2_pd.drop(columns=['quantity', ])
                else:
                    print("Нет помарочных остатков")
                rests_rfu2_pd = rests_rfu2_pd[rests_rfu2_pd['total'] > 0]

            else:
                print("Не найдено приходов со старыми марками, продолжение невозможно")
                sys.exit(1)

        # получаем остатки по Р2, чтобы не превысить доступное кол-во
        rests_r2_pd = pd.DataFrame.from_records(fetch_results(rests_r2_codes, cur))
        if rests_r2_pd.empty:
            print("Не найдено остатков продукции на Р2 для перевода, продолжение невозможно")
            sys.exit(1)

        # приводим датафреймы к вложенным словарям для удобства
        rests_r2 = indexed_df_to_nested_dict(rests_r2_pd)
        rests_r2 = {k: int(v) for k, v in rests_r2.items()}
        rests_fact = indexed_df_to_nested_dict(rests_r2_pd if os.environ.get('RST') else inv_pd)
        rests_fact = {k: int(v) for k, v in rests_fact.items()}
        rests_rfu2 = indexed_df_to_nested_dict(rests_rfu2_pd)
        rests_rfu2 = {k: {k1: int(v1) for k1, v1 in v.items()} for k, v in rests_rfu2.items()}

        if extra_tts_rests:
            print(f'EXTRA TTS FOUND QTY: {extra_tts_rests.get("quantity")}')
            rests_rfu2 = merge_rests_dicts(rests_rfu2, extra_tts_rests.get("rests"))

        rfu2_count = sum([len(v) for v in rests_rfu2.values()])
        rfu2_quantity = sum([sum(v.values()) for v in rests_rfu2.values()])

        print(f'AVAILABLE RFU2: CODES: {len(rests_rfu2.keys())}, RFU2: {rfu2_count} TOTAL QTY: {rfu2_quantity}')
        print(f'FACT INVENTED: CODES: {len(rests_fact.keys())}, QTY: {sum(rests_fact.values())}')

        # приводим список марок из инвентаризации к виду {alccode: [mark, ...], ...}
        invent_mark_codes = inv_marks_pd.groupby('alccode')['markcode'].apply(list).to_dict()

        return rests_r2, rests_rfu2, rests_fact, invent_mark_codes

    def fill_xml_header(root: ET.Element, fsrar_id: str):
        """ Заполняем заголовки"""
        fsrar_el = root[0][0]
        fsrar_el.text = fsrar_id

        # произвольный идектификатор (без валидации)
        identity_id = str(uuid4().int)[:6]
        act_identity_num_el = root[1][0][0]
        act_identity_num_el.text = identity_id

        act_header_num_el = root[1][0][1][0]
        act_header_num_el.text = identity_id

        act_date_el = root[1][0][1][1]
        act_date_el.text = datetime.now().strftime("%Y-%m-%d")
        return root

    def generate_tfs_xml(rests: dict, fsrar_id: str, invent_id: str, template: str = 'xml/tfs.xml') -> Tuple[str, int]:
        """ Формирование XML TransferFromShop на основе п 1.17 документации """

        tree = ET.parse(template)
        root = tree.getroot()
        root = fill_xml_header(root, fsrar_id)

        # content
        content_section = root[1][0][2]

        ns1 = '{http://fsrar.ru/WEGAIS/TransferFromShop}'
        ns2 = '{http://fsrar.ru/WEGAIS/ProductRef_v2}'

        identity_counter = 1

        for alc_code, f2_quantity in rests.items():
            for f2, qty in f2_quantity.items():
                position = ET.SubElement(content_section, f'{ns1}Position')

                identity = ET.SubElement(position, f'{ns1}Identity')
                identity.text = str(identity_counter)

                product_code = ET.SubElement(position, f'{ns1}ProductCode')
                product_code.text = alc_code

                quantity = ET.SubElement(position, f'{ns1}Quantity')
                quantity.text = str(int(qty))

                f2_sec = ET.SubElement(position, f'{ns1}InformF2')
                f2_reg = ET.SubElement(f2_sec, f'{ns2}F2RegId')
                f2_reg.text = f2

                identity_counter += 1

        filename = f'{invent_id}_tfs_{uuid4()}.xml'

        try:
            pretty_print_xml(root, filename)
        except Exception as e:
            print(f"CANT WRITE DOWN RESULT, EXITED {e}")
            sys.exit(1)

        return filename, identity_counter

    def generate_afbc_xml(marks: dict, fsrar_id: str, invent_id: str, template: str = 'xml/actfixbarcode.xml') -> Tuple[
        str, int]:
        """ Формируем ActFixBarCode из посчитанных марок согласно документации п 3.8 и шаблона """

        tree = ET.parse(template)
        root = tree.getroot()
        root = fill_xml_header(root, fsrar_id)

        # content
        content_section = root[1][0][2]

        ns1 = '{http://fsrar.ru/WEGAIS/ActFixBarCode}'
        ns2 = '{http://fsrar.ru/WEGAIS/CommonV3}'

        identity_counter = 1

        for alc_code, f2_marks in marks.items():
            for f2, marks in f2_marks.items():
                position = ET.SubElement(content_section, f'{ns1}Position')

                identity = ET.SubElement(position, f'{ns1}Identity')
                identity.text = str(identity_counter)

                f2_reg = ET.SubElement(position, f'{ns1}Inform2RegId')
                f2_reg.text = f2

                mark_info = ET.SubElement(position, f'{ns1}MarkInfo')
                for mark in marks:
                    mark_el = ET.SubElement(mark_info, f'{ns2}amc')
                    mark_el.text = mark

                identity_counter += 1

        filename = f'{invent_id}_actfixbarcode_{uuid4()}.xml'

        try:
            pretty_print_xml(root, filename)
        except Exception as e:
            print(f"CANT WRITE DOWN RESULT, EXITED {e}")
            sys.exit(1)

        return filename, identity_counter

    def pretty_print_xml(root, output_xml):
        """ Форматирование XML """
        xml_string = xml.dom.minidom.parseString(ET.tostring(root)).toprettyxml()
        xml_string = os.linesep.join([s for s in xml_string.splitlines() if s.strip()])
        with open(output_xml, "w") as file_out:
            file_out.write(xml_string)

    def get_fsrar_id(process_id: str) -> str:
        """ Получение ФСРАР из процесса инвентаризации """
        invent_header = f"""
                select ourfsrarid, location from smegaisprocessegoabheader
                where processid = {process_id} and processtype = 'EGOA'
                """

        header_res = fetch_results(invent_header, cur)
        if header_res:
            fsrar_id = header_res[0][0]
            loc_id = header_res[0][1]
            print(f'PROCESS: {process_id} [FSRAR: {fsrar_id} STOCK: {loc_id}]')
            return fsrar_id
        else:
            print(f'Не найден процесс инвентаризации #{process_id}, продолжение невозможно')
            sys.exit(1)

    def generate_sql_insert_mark(marks: dict, fsrar_id: str, invent_id: str):
        """ Формируем SQL INSERT  вида {alccode: {rfu2 : [mark, ...], ...},...} """

        sql = []
        today = datetime.now().strftime("%d.%m.%y")
        header = 'INSERT ALL'
        into = 'INTO SMEGAISRESTSPIECE (OURFSRARID, MARKCODE, ALCCODE, INFORMBREGID, EXISTINGCOUNT, TTNGLID, RESTSDATE)'
        footer = 'SELECT 1 FROM DUAL;'
        sql.append(header)
        for alccode, rests in marks.items():
            for f2, mark_list in rests.items():
                for mark in mark_list:
                    sql.append(into)
                    sql.append(f"VALUES ('{fsrar_id}', '{mark}', '{alccode}', '{f2}', 1, 0, '{today}')")

        sql.append(footer)

        filename = f'{invent_id}_import_marks_{uuid4()}.sql'
        with open(filename, "w") as outfile:
            outfile.write("\n".join(sql))

        return filename

    cur = setting_cursor()

    # получаем фсрар ид по номеру процесса инвентаризации
    fsrar = get_fsrar_id(invent)

    # остатки и пересчет
    r2_rests, rfu_rests, fact_rests, fact_marks = process_rests_data(fsrar, invent)
    total_r2_codes = len(r2_rests)
    total_r2_qty = sum([int(v) for v in r2_rests.values()])

    print(f"EGAIS R2 RESTS: CODES: {len(r2_rests)}, QTY: {total_r2_qty}")
    if DEBUG:
        print(' === R2 RESTS === ')
        pprint(r2_rests)

        print(' === RFU2 RESTS === ')
        pprint(rfu_rests)

        print(' === INVNT === ')
        pprint(fact_rests)

        print(' === MARKS === ')
        pprint(fact_marks)

    r2_out_rests = {}

    if os.environ.get('RESTS_VALIDATION'):
        fact_rests, fact_marks, r2_out_rests = r2_rests_control(r2_rests, fact_rests, fact_marks)

    total_r2_outrests_codes = len(r2_out_rests)
    total_r2_outrests_qty = sum([v["quantity"] for v in r2_out_rests.values()])

    if r2_out_rests:
        print(f'TOTAL OUTREST: CODES {total_r2_outrests_codes}, QTY {total_r2_outrests_qty}')

    # размещаем результаты инвентаризации на остатки по алкокодам-справкам
    allocated_rests, rfu2_out_rests = allocation_rests_on_rfu2(rfu_rests, fact_rests)

    total_r2_outstock_codes = len(rfu2_out_rests)
    total_r2_outstock_qty = sum(rfu2_out_rests.values())

    if rfu2_out_rests:
        print(f'TOTAL OUTSTOCK CODES {total_r2_outstock_codes} : {total_r2_outstock_qty} pcs LIST: {rfu2_out_rests}')

    # формируем файл выгрузки Р2->Р1
    transfer_from_shop_filename, total_identities = generate_tfs_xml(allocated_rests, fsrar, invent)

    # размещаем марки на алкокоды-справки
    allocated_marks, marks_out_stock = allocate_mark_codes_to_rfu2(rfu_rests, fact_marks)

    if marks_out_stock:
        print(f'TOTAL OUTSTOCK CODES {len(marks_out_stock)} : {sum([len(m) for m in marks_out_stock.values()])} pcs')

    act_fix_barcode_filename, total_identities = generate_afbc_xml(allocated_marks, fsrar, invent)

    sql_import = generate_sql_insert_mark(allocated_marks, fsrar, invent)
    print(f'FILES SAVED: {transfer_from_shop_filename} {act_fix_barcode_filename} {sql_import}')

    # запишем все
    db_record = {
        'date': datetime.now(),
        'fsrar': fsrar,
        'process': invent,  # процесс инвентаризации
        'rests_validation': bool(os.environ.get('RESTS_VALIDATION')),  # валидация посчитанного с отстатками Р2
        'rests_r2': r2_rests,  # остатки Р2, последний ЕГАИС запрос
        'rests_rfu2': rfu_rests,  # доступные остатки по РФУ2 справкам
        'rests_fact': fact_rests,  # фактически посчитанное количество
        'rests_marks': fact_marks,  # фактические марки
        'rests_r2_lack': r2_out_rests,  # излишек посчитанного от ЕГАИС
        'rests_rfu2_lack': rfu2_out_rests,  # излишек посчитанного от расчетных справок РФУ2
        'rests_mark_lack': marks_out_stock,  # излишек марок
        'total_r2_codes': total_r2_codes,  # кодов на остатке
        'total_r2_qty': total_r2_qty,  # шт на остатке Р2
        'total_fact_codes': len(fact_rests),  # количество алкокодов
        'total_fact_qty': sum([int(v) for v in fact_rests.values()]),  # количество старых марок для перевода
        'total_r2_out_codes': total_r2_outrests_codes,
        'total_r2_out_qty': total_r2_outrests_qty,
        'total_rfu2_out_codes': total_r2_outstock_codes,
        'total_rfu2_out_qty': total_r2_outstock_qty,
        'res_codes': allocated_rests,
        'res_marks': allocated_marks,

    }

    try:
        with MongoClient(os.environ.get('MONGO_CONN', 'localhost:27017')) as client:
            col = client[os.environ.get('MONGO_DB', 'utm')][os.environ.get('MONGO_COL', 'rests')]
            col.insert_one(db_record)
            print('RESULTS SAVED SUCCESS')

    except Exception as e:
        print(f'ERROR CANT SAVE RESULTS TO DB: {e}')

    return transfer_from_shop_filename, act_fix_barcode_filename


if __name__ == '__main__':
    allocate_rests()
