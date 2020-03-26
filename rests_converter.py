import os
import sys
import xml.dom.minidom
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime
from typing import Tuple
from uuid import uuid4

import click
import cx_Oracle
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


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
        outstock = {}

        print("PROCESSING CODES...")
        for alc_code, qty in invent_rests.items():
            print(f'ACODE {alc_code} : {qty}')
            result[alc_code] = {}
            rest_alc = total_rests.get(alc_code)
            for f2, f2_qty in rest_alc.items():
                if qty and f2_qty:
                    if qty >= f2_qty:
                        qty = qty - f2_qty
                        rest_alc[f2] = 0
                        result[alc_code][f2] = f2_qty
                        print(f'++ ADDED {f2} DEPLETED WITH {f2_qty}, REMAIN TO ALLOCATE {qty}')
                    else:
                        rest_alc[f2] = f2_qty - qty
                        result[alc_code][f2] = qty
                        print(f'+ ADDED {f2} ACODE FULLFILED WITH {qty} (WAS {f2_qty})')
                        qty = 0
            if qty > 0:
                outstock[alc_code] = qty
                print(f'- NOT DONE: {qty}')

        print(f"{'ALL DONE...' if not outstock else f'WARNING OUT STOCK: {outstock}'}")

        return result

    def process_rests_data(fsrar_id: str, process_id: str):
        """ Получаем остатки и пересчет из Oracle, обрабатываем и возвращаем словари доступны остатки, факт """
        # список марок и их цен
        income_ttn = f"""
        select ourfsrarid, egaisfixdate, productalccode, quantity, informbregid  from smegaisdocspec spec
        left join smegaisdocheader header on spec.glid = header.glid -- шапка с фсрарид, датой, хедером
        left join SMEGAISDOCSPECACT act on spec.glid = act.glid and spec.identity = act.identity -- марки
        where header.ourfsrarid = '{fsrar_id}' 
            and docstate = 6  -- успешно завершенные
            and doctype = 'WBInvoiceToMe' -- приходные накладные от поставщика
            and informbregid is not Null -- обязательно указанием справки 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) -- слабоалкогольная продукция
        group by ourfsrarid, egaisfixdate, wbregid, productalccode, quantity, informbregid
        order by ourfsrarid, egaisfixdate desc , quantity desc, productalccode, informbregid
        """

        return_ttn = f"""
        select distinct ourfsrarid, productalccode, quantity, f2regid from smegaisdocspec spec
        left join smegaisdocheader header on spec.glid = header.glid
        left join smegaisdocspecf2 f2 on spec.glid=f2.glid and spec.identity = f2.identity
        where header.ourfsrarid = '{fsrar_id}' 
            and docstate = 17 -- завершенные
            and doctype = 'WBReturnFromMe' -- возвраты
            and f2regid is not Null -- обязательно указана справка 
            and spec.productvcode not in (500, 510, 520, 261, 262, 263) 
        order by ourfsrarid, productalccode, f2regid, quantity desc
        """

        invent_data = f"""
        select ourfsrarid, alccode, sum(quantity) from smegaisprocessegoabheader header
        left join SMEGAISPROCESSEGOABSPEC rst on header.processid = rst.processid and header.processtype = rst.processtype
        where header.processid = {process_id} 
            and header.processtype = 'EGOA' -- процесс инвентаризации крепкоалкогольной
            and length(rst.markcode) = 68 -- длина старой АМ
        group by ourfsrarid, alccode
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
        SELECT ourfsrarid, alccode, quantity
        FROM smegaisrests
        WHERE ourfsrarid = {fsrar_id} 
            and isretail = 1 -- остатки на Р2 (торговый зал)
            and productvcode NOT IN (500, 510, 520, 261, 262, 263) -- (слабоалкогольная продукция)
            """

        f3_marks = f"""
        select ourfsrarid, alccode, informbregid, count(markcode) from SMEGAISRESTSPIECE
        where ourfsrarid = '{fsrar_id}'
        group by ourfsrarid, alccode, informbregid
        order by ourfsrarid, informbregid, alccode
        """

        inv_pd = pd.DataFrame.from_records(fetch_results(invent_data, cur))
        inv_pd.columns = ['fsrar', 'alccode', 'quantity']
        inv_pd.set_index(['fsrar', 'alccode'])
        inv_pd = inv_pd.drop(columns='fsrar')

        inv_marks_pd = pd.DataFrame.from_records(fetch_results(invent_marks, cur))
        inv_marks_pd.columns = ['alccode', 'markcode']
        inv_marks_pd.set_index(['alccode', ])

        in_pd = pd.DataFrame.from_records(fetch_results(income_ttn, cur))
        in_pd.columns = ['fsrar', 'date', 'alccode', 'quantity', 'f2']
        in_pd.set_index(['fsrar', 'alccode', 'f2'])

        out_pd = pd.DataFrame.from_records(fetch_results(return_ttn, cur))
        out_pd.columns = ['fsrar', 'alccode', 'quantity', 'f2']
        out_pd.set_index(['fsrar', 'alccode', 'f2'])

        f3_pd = pd.DataFrame.from_records(fetch_results(f3_marks, cur))
        f3_pd.columns = ['fsrar', 'alccode', 'f2', 'quantity']
        f3_pd.set_index(['fsrar', 'alccode', 'f2'])

        # собираем вместе все таблицы
        in_out_pd = in_pd.merge(out_pd, on=['fsrar', 'alccode', 'f2'], how='outer')
        all_pd = in_out_pd.merge(f3_pd, on=['fsrar', 'alccode', 'f2'], how='outer')

        # считаем общее кол-во
        all_pd['total'] = all_pd['quantity_x'].fillna(0) - all_pd['quantity_y'].fillna(0) - all_pd['quantity'].fillna(0)

        # убираем неполные результаты
        clean_pd = all_pd.drop(columns=['fsrar', 'date', 'quantity_x', 'quantity_y', 'quantity'])
        result_pd = clean_pd[clean_pd['total'] > 0]

        # приводим датафреймы к вложенным словарям для удобства
        counted = indexed_df_to_nested_dict(inv_pd)
        calculated = indexed_df_to_nested_dict(result_pd)
        calculated_rfu2 = sum([len(v) for v in calculated.values()])
        calculated_qty = sum([sum(v.values()) for v in calculated.values()])

        print(f'RESTS: CODES: {len(calculated.keys())}, RFU2: {calculated_rfu2} TOTAL QTY: {calculated_qty}')
        print(f'INVENT: CODES: {len(counted.keys())}, QTY: {sum(counted.values())}')

        # приводим список марок из инвентаризации к виду {alcode: [mark, ...], ...}
        invent_mark_codes = inv_marks_pd.groupby('alccode')['markcode'].apply(list).to_dict()

        return calculated, counted, invent_mark_codes

    def allocate_mark_codes_to_rfu2(rests: dict, mark_codes: dict) -> dict:
        """ Распределяем марки на справки РФУ2, вида {alccode: {rfu2 : [mark, ...], ...},...} """
        rests = deepcopy(rests)
        rfu2_marks = {}

        print("PROCESSING MARKS...")
        for alc_code, marks in mark_codes.items():
            rfu2_marks[alc_code] = {}
            rfu2_rests = rests.get(alc_code)
            rfu2_total = sum(rfu2_rests.values())
            alc_code_qty = len(marks)

            print(f'ACODE: {alc_code}, MARKS {alc_code_qty}, RFU2s: {len(rfu2_rests)}, AVL: {rfu2_total}')

            if rfu2_total < alc_code_qty:
                print(f'WARNING AVL: {rfu2_total}, REQUIRED {alc_code_qty}')

            for rfu, qty in rfu2_rests.items():
                qty = int(qty)
                if marks and qty:
                    rfu2_marks[alc_code][rfu] = marks[:qty]
                    marks = marks[qty:]
                    if not marks:
                        print(f'DONE {alc_code}')

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

    def generate_tfs_xml(rests: dict, fsrar_id: str, template: str = 'xml/tfs.xml') -> Tuple[str, int]:
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

        filename = f'tfs_{uuid4()}.xml'

        try:
            pretty_print_xml(root, filename)
        except Exception as e:
            print(f"CANT WRITE DOWN RESULT, EXITED {e}")
            sys.exit(1)

        return filename, identity_counter

    def generate_afbc_xml(marks: dict, fsrar_id: str, template: str = 'xml/actfixbarcode.xml') -> Tuple[str, int]:
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

                f2_reg = ET.SubElement(position, f'{ns1}F2RegId')
                f2_reg.text = f2

                mark_info = ET.SubElement(position, f'{ns1}MarkInfo')
                for mark in marks:
                    mark_el = ET.SubElement(mark_info, f'{ns2}amc')
                    mark_el.text = mark

            identity_counter += 1

        filename = f'actfixbarcode_{uuid4()}.xml'

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

        invent_header = f"""
        select ourfsrarid, location from smegaisprocessegoabheader
        where processid = {process_id} and processtype = 'EGOA'
        """

        header_res = fetch_results(invent_header, cur)
        fsrar_id = header_res[0][0]
        loc_id = header_res[0][1]
        print(f'PROCESS: {process_id} [FSRAR: {fsrar_id} STOCK: {loc_id}]')
        return fsrar_id

    cur = setting_cursor()

    # получаем фсрар ид по номеру процесса инвентаризации
    fsrar = get_fsrar_id(invent)

    # остатки и пересчет
    calculated_rests, counted_rests, counted_marks = process_rests_data(fsrar, invent)

    # размещаем результаты инвентаризации на остатки по алкокодам-справкам
    allocated_rests = allocation_rests_on_rfu2(calculated_rests, counted_rests)

    # формируем файл выгрузки Р2->Р1
    transfer_from_shop_filename, total_identities = generate_tfs_xml(allocated_rests, fsrar)
    print(f"TFS SAVED: {transfer_from_shop_filename}, TOTAL LISTINGS: {total_identities}")

    # размещаем марки на алкокоды-справки
    allocated_marks = allocate_mark_codes_to_rfu2(calculated_rests, counted_marks)

    # формируем файл выгрузки Р2->Р1
    act_fix_barcode_filename, total_identities = generate_afbc_xml(allocated_marks, fsrar)
    print(f"ACTFIXBARCODE SAVED: {act_fix_barcode_filename}, TOTAL LISTINGS: {total_identities}")

    return transfer_from_shop_filename, act_fix_barcode_filename


if __name__ == '__main__':
    allocate_rests()
