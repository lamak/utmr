import os
import sys
import xml.dom.minidom
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime
from pprint import pprint
from typing import Tuple
from uuid import uuid4

import click
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DEBUG = os.environ.get('DEBUG')


@click.command()
@click.option('--invent', prompt='Process ID', help='PID')
def allocate_rests(invent):
    """Генерация XML перевода Р2 -> Р1 на основе имеющихся остатков и факт пересчета"""

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

    def allocation_rests_on_rfu2(total_rests: dict, invent_rests: dict, ) -> dict:
        """ Распределяем имеющиеся остатки (из инвентаризации, invent) на расчитанные остатки по справкам  (rests)"""
        total_rests = deepcopy(total_rests)
        result = {}
        out_stock = {}

        print("PROCESSING CODES...")
        for alc_code, qty in invent_rests.items():
            if DEBUG:
                print(f'ACODE {alc_code} : {qty}')
            result[alc_code] = {}
            rest_alc = total_rests.get(alc_code)
            # todo: check for none and qty to be int always
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

        if out_stock:
            print(f'TOTAL OUTSTOCK CODES {len(out_stock)} : {sum(out_stock.values())} pcs. LIST: {out_stock}')

        return result

    def process_rests_data(fsrar_id: str, process_id: str):
        """ Получаем остатки и пересчет из Oracle, обрабатываем и возвращаем словари доступны остатки, факт """
        income_ttn = f"""
        select distinct productalccode, informbregid, sum(quantity) quantity  from smegaisdocspec spec
        left join smegaisdocheader header on spec.glid = header.glid -- шапка с фсрарид, датой, хедером
        left join SMEGAISDOCSPECACT act on spec.glid = act.glid and spec.identity = act.identity -- марки
        where header.ourfsrarid = '{fsrar_id}' 
            and docstate = 6  -- успешно завершенные
            and doctype = 'WBInvoiceToMe' -- приходные накладные от поставщика
            and informbregid is not Null -- обязательно указанием справки 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) -- слабоалкогольная продукция

        group by productalccode, informbregid
        order by quantity desc, productalccode, informbregid
        """

        return_ttn = f"""
        select distinct productalccode,  f2regid, quantity from smegaisdocspec spec
        left join smegaisdocheader header on spec.glid = header.glid
        left join smegaisdocspecf2 f2 on spec.glid=f2.glid and spec.identity = f2.identity
        where header.ourfsrarid = '{fsrar_id}' 
            and docstate = 17 -- завершенные
            and doctype = 'WBReturnFromMe' -- возвраты
            and f2regid is not Null -- обязательно указана справка 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) 
        order by productalccode, f2regid, quantity desc
        """

        invent_data = f"""
        select alccode, sum(quantity) from smegaisprocessegoabheader header
        left join SMEGAISPROCESSEGOABSPEC rst on header.processid = rst.processid and header.processtype = rst.processtype
        where header.processid = {process_id} 
            and header.processtype = 'EGOA' -- процесс инвентаризации крепкоалкогольной
            and length(rst.markcode) = 68 -- длина старой АМ
            AND rst.markcode not in (select markcode from SMEGAISRESTSPIECE) -- исключаем марки на Р3
        group by alccode
        """

        invent_marks = f"""
        SELECT alccode, rst.markcode
        FROM smegaisprocessegoabheader hdr 
        LEFT JOIN smegaisprocessegoabspec rst ON hdr.processid = rst.processid AND hdr.processtype = rst.processtype
        WHERE hdr.processid = {process_id}
            AND hdr.processtype = 'EGOA'
            AND length(rst.markcode) = 68
            AND rst.markcode not in (select markcode from SMEGAISRESTSPIECE)
        """

        r2_rests = f"""
        SELECT alccode, quantity
        FROM smegaisrests
        WHERE ourfsrarid = {fsrar_id} 
            and isretail = 1 -- остатки на Р2 (торговый зал)
            and productvcode NOT IN (500, 510, 520, 261, 262, 263) -- (слабоалкогольная продукция)
            """

        f3_marks = f"""
        select alccode, informbregid, count(markcode) from SMEGAISRESTSPIECE
        where ourfsrarid = '{fsrar_id}'
        group by alccode, informbregid
        order by informbregid, alccode
        """
        # Описание процесса:
        # 1. Получаем остатки по TTN приход, в разрезе алкокод-справка-количество (завершенные, со справками, крепкий алкоголь)
        # 2. Вычитаем TTN расходы
        # 3. Вычитаем помарочные остатки
        # 4. С полученными остатками сопоставляем инвентаризацию (марки 68 символов, не числящиеся на помарочном учете)
        # 5. Формируем TransferFromShop Р2 -> Р1
        # 6. Формируем ActBarCodeFix для фиксации марок на Р3
        # 7. Формируетм SQL Insert для отображения посчитанных марок в Супермаге
        #
        # Шаги 6,7 опциональны, т.к. можно обновить остатки на Р1, и закончить штатно инвентаризацию в Супермаге
        # тем самым зафиксировов Р3 в ЕГАИС и в Супермаг
        #
        # Любые данные могут быть опциональн, их может не быть, это надо обрабатывать

        # инвентаризация: алкокода и количество
        inv_pd = pd.DataFrame.from_records(fetch_results(invent_data, cur))
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

        # ttn: приход, алкокода, справки, количество
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
            f3_pd = pd.DataFrame.from_records(fetch_results(f3_marks, cur))
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
        rests_pd = pd.DataFrame.from_records(fetch_results(r2_rests, cur))
        if rests_pd.empty:
            print("Не найдено остатков продукции на Р2 для перевода, продолжение невозможно")
            sys.exit(1)

        # приводим датафреймы к вложенным словарям для удобства
        rests = indexed_df_to_nested_dict(rests_pd)
        counted = indexed_df_to_nested_dict(inv_pd)
        calculated = indexed_df_to_nested_dict(rests_rfu2_pd)
        calculated_rfu2 = sum([len(v) for v in calculated.values()])
        calculated_qty = sum([sum(v.values()) for v in calculated.values()])

        print(f'RESTS: CODES: {len(calculated.keys())}, RFU2: {calculated_rfu2} TOTAL QTY: {calculated_qty}')
        print(f'INVENT: CODES: {len(counted.keys())}, QTY: {sum(counted.values())}')

        # приводим список марок из инвентаризации к виду {alccode: [mark, ...], ...}
        invent_mark_codes = inv_marks_pd.groupby('alccode')['markcode'].apply(list).to_dict()

        return rests, calculated, counted, invent_mark_codes

    def allocate_mark_codes_to_rfu2(rests: dict, mark_codes: dict) -> dict:
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

        if out_stock:
            print(f'TOTAL OUTSTOCK CODES {len(out_stock)} : {sum([len(m) for m in out_stock.values()])} pcs')

        return rfu2_marks

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
        fsrar_id = header_res[0][0]
        loc_id = header_res[0][1]
        print(f'PROCESS: {process_id} [FSRAR: {fsrar_id} STOCK: {loc_id}]')
        return fsrar_id

    def generate_sql_insert_mark(marks: dict, fsrar_id: str, invent_id: str):
        """ Формируем SQL INSERT  вида {alccode: {rfu2 : [mark, ...], ...},...} """

        sql = []
        today = datetime.now().strftime("%m.%d.%y")
        header = 'INSERT_ALL'
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
    fact_rests, calculated_rests, counted_rests, counted_marks = process_rests_data(fsrar, invent)
    if DEBUG:
        print(' === R2 RESTS === ')
        pprint(fact_rests)

        print(' === RFU2 RESTS === ')
        pprint(calculated_rests)

        print(' === INVNT === ')
        pprint(counted_rests)

        print(' === MARKS === ')
        pprint(counted_marks)

    def r2_rests_control(fact: dict, invented: dict, marks: dict):
        """ Проверка количества посчитанного на превышение фактических остатков на Р2,
        При превышении отсекаем лишнее (кол-во + марки), выводя в лог """
        out_rests = {}
        for alc_code, qty in invented.items():
            r2_qty = fact.get(alc_code)
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
                print(f"WARING NOT IN RESTS: {alc_code}, QTY {qty}")
        if out_rests:
            print(f'TOTAL OUTREST: CODES {len(out_rests)}, QTY {sum([v["quantity"] for v in out_rests.values()])}')
        return invented, marks

    if os.environ.get('RESTS_VALIDATION'):
        counted_rests, counted_marks = r2_rests_control(fact_rests, counted_rests, counted_marks)

    # размещаем результаты инвентаризации на остатки по алкокодам-справкам
    allocated_rests = allocation_rests_on_rfu2(calculated_rests, counted_rests)

    # формируем файл выгрузки Р2->Р1
    transfer_from_shop_filename, total_identities = generate_tfs_xml(allocated_rests, fsrar, invent)
    print(f"TFS SAVED: {transfer_from_shop_filename}, TOTAL LISTINGS: {total_identities}")

    # размещаем марки на алкокоды-справки
    allocated_marks = allocate_mark_codes_to_rfu2(calculated_rests, counted_marks)

    act_fix_barcode_filename, total_identities = generate_afbc_xml(allocated_marks, fsrar, invent)
    print(f"ACTFIXBARCODE SAVED: {act_fix_barcode_filename}, TOTAL LISTINGS: {total_identities}")

    sql_import = generate_sql_insert_mark(allocated_marks, fsrar, invent)
    print(f'SQL INSERT SAVED: {sql_import}')
    return transfer_from_shop_filename, act_fix_barcode_filename


if __name__ == '__main__':
    allocate_rests()
