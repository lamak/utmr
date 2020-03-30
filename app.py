import copy
import inspect
import logging
import os
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from typing import Optional, List

import MySQLdb
import openpyxl
import pandas as pd
import requests
import xmltodict
from bson.son import SON
from flask import Flask, Markup, flash, request, redirect, url_for, send_from_directory, render_template
from grab import Grab
from grab.error import GrabCouldNotResolveHostError, GrabConnectionError, GrabTimeoutError
from pymongo import MongoClient, DESCENDING
from pymongo.database import Database
from weblib.error import DataNotFound
from werkzeug.utils import secure_filename

from forms import FsrarForm, RestsForm, TicketForm, UploadForm, CreateUpdateUtm, StatusSelectOrder, MarkFormError, \
    MarkForm, ChequeForm, WBRepealConfirmForm, RequestRepealForm, TTNForm

app = Flask(__name__)
app.config.from_object('config.AppConfig')
app.secret_key = os.environ.get('FLASK_SECRET_KEY')


class Utm:
    """ УТМ
    Включает в себя название, адрес сервера, заголовок-адрес, путь к XML обмену Супермага
    """

    def __init__(self, fsrar, host, title, path, ukm, active: bool = True):
        self.fsrar = fsrar
        self.host = host
        self.title = title
        self.path = path or f'{app.config["DEFAULT_XML_PATH"]}{host.split("-")[0]}/in/'
        self.ukm = ukm
        self.active: bool = bool(active)

    def __str__(self):
        return f'{self.fsrar} {self.title}'

    def __repr__(self):
        return f'{self.fsrar} {self.title}'

    def url(self):
        return f'http://{self.host}.{app.config["LOCAL_DOMAIN"]}:{app.config["UTM_PORT"]}'

    def ukm_host(self):
        return f'{self.ukm}.{app.config["LOCAL_DOMAIN"]}'

    def build_url(self):
        return self.url() + '/?b'

    def version_url(self):
        return self.url() + '/info/version'

    def reset_filter_url(self) -> str:
        return f'{self.url()}/xhr/filter/reset'

    def gost_url(self):
        return self.url() + '/info/certificate/GOST'

    def docs_in_url(self):
        return self.url() + '/opt/out/waybill_v3'

    def docs_out_url(self):
        return self.url() + '/opt/in'

    def xml_url(self):
        return self.url() + '/xml'

    def log_dir(self):
        return f'//{self.host}.{app.config["LOCAL_DOMAIN"]}/{app.config["UTM_LOG_PATH"]}'

    def to_csv(self):
        return ';'.join(vars(self).values()) + '\n'

    def to_dict(self):
        return vars(self)


class Result:
    """ Результаты опроса УТМ
    С главной страницы получаем:
    * Состояние УТМ и лицензии
    * Сроки ключей ГОСТ, PKI
    * Состояние чеков
    * Организация из сертификата ГОСТ

    Фиксируются все ошибки при парсинге
    Данные УТМ переносятся в результат для вывода в шаблон Jinja2

    """

    def __init__(self, **kwargs):
        self.utm: Optional[Utm] = kwargs.get('utm')  # fsrar, server, title
        self.legal: str = kwargs.get('legal', '')
        self.surname: str = kwargs.get('surname', '')
        self.given_name: str = kwargs.get('given_name', '')
        self.gost: str = kwargs.get('gost', '')
        self.pki: str = kwargs.get('pki', '')
        self.cheques: str = kwargs.get('cheques', '')
        self.status: bool = kwargs.get('status', False)
        self.licence: bool = kwargs.get('licence', False)
        self.error: list = kwargs.get('errors', [])
        self.fsrar: str = kwargs.get('fsrar', self.utm.fsrar if self.utm else '')
        self.host: str = kwargs.get('host', self.utm.host if self.utm else '')
        self.url: str = kwargs.get('url', self.utm.url() if self.utm else '')
        self.title: str = kwargs.get('title', self.utm.title if self.utm else '')
        self.filter: bool = kwargs.get('filter', False)
        self.docs_in: int = kwargs.get('docs_in', 0)
        self.docs_out: int = kwargs.get('docs_out', 0)
        self.version: str = kwargs.get('cheques', '')
        self.change_set: str = kwargs.get('cheques', '')
        self.build: str = kwargs.get('cheques', '')
        self.date = kwargs.get('cheques', datetime.utcnow())

    def to_dictionary(self):
        tmp_dict = copy.deepcopy(vars(self))
        tmp_dict['last'] = True
        del tmp_dict['utm']
        return tmp_dict


class Configs:
    def __init__(self, database: Optional[Database]):
        self.use_db = bool(app.config['UTM_USE_DB'])
        self.config = app.config['UTM_CONFIG']
        self.db = database
        self.all_utms = self.get_utm_list()
        self.utms = [utm for utm in self.all_utms if utm.active]

    def utm_choices(self):
        return [(u.fsrar, f'{u.title} [{u.fsrar}] [{u.host}]') for u in self.utms]

    def create_update_current(self, utm):
        element = next((u for u in self.all_utms if u.fsrar == utm.fsrar), None)
        if element:
            self.utms.remove(element)

        self.utms.append(utm)

    def create_update_storage(self, utm):
        self.create_update_utm_db(utm) if self.use_db else self.create_update_config_utm(utm)

    def create_or_update_utm(self, utm: Utm):
        self.create_update_current(utm)
        self.create_update_storage(utm)

    def get_utm_list(self):
        """ Выбор источника УТМ, при отсутствии в БД будет попытка заполнить из файла """
        if self.use_db:
            utms = self.get_utm_from_db()
            if not utms:
                self.import_utms_to_db()
            return self.get_utm_from_db()
        else:
            return self.get_utm_from_file()

    def get_utm_from_db(self):
        """ Получение списка УТМ из MongoDB """

        return [Utm(**remove_id(u)) for u in self.db.utm.find().sort('title', 1)]

    def get_utm_from_file(self):
        """ Получение списка УТМ из файла настроек """
        utms = []
        try:
            with open(self.config, 'r', encoding='utf-8') as config_file:
                utms = [Utm(*u.split(';')) for u in config_file.read().splitlines()]
                utms.sort(key=lambda utm: utm.title)
        except FileNotFoundError as e:
            logging.error(e)
        return utms

    def create_update_utm_db(self, utm: Utm):
        """ Создание или обновление УТМ в MongoDB"""
        query = {'fsrar': utm.fsrar}
        if not self.db.utm.find_one(query):
            self.db.utm.insert_one(vars(utm))
            logging.info(f'Добавлен УТМ {utm}')
        else:
            self.db.utm.update_one(query, {
                '$set': vars(utm)
            }, upsert=False)
            logging.info(f'Обновлен УТМ {utm}')

    def import_utms_to_db(self):
        """ Импорт УТМ из файла настроек в MongoDB """
        utms = self.get_utm_from_file()
        utms.sort(key=lambda utm: utm.fsrar)
        [self.create_update_utm_db(u) for u in utms]

    def create_update_config_utm(self, utm: Utm):
        """ Создание или обновление конфиг файла """
        if os.path.isfile(self.config):
            with open(self.config, 'r') as file:
                lines = file.read().splitlines()

            with open(self.config, 'w') as file:
                for line in lines:
                    if line.split(';')[0] != utm.fsrar:
                        file.write(line + '\n')
                file.write(utm.to_csv())
        else:
            with open(self.config, 'w') as file:
                file.write(utm.to_csv())


def create_folder(dirname: str):
    if not os.path.isdir(dirname):
        os.mkdir(dirname)


def remove_id(dictionary):
    """ Удаление идентификатора MongoDB """
    tmp_dict = dict(dictionary)
    del tmp_dict['_id']
    return tmp_dict


def validate_filename(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def get_xml_template(filename: str) -> str:
    return os.path.join('xml/', filename)


def get_instance(fsrar: str) -> Optional[Utm]:
    """ Получаем УТМ из формы по ФСРАР ИД"""
    return next((x for x in cfg.utms if x.fsrar == fsrar), None)


def get_limit(field: str, max_limit: int, default_limit: int) -> int:
    """ Лимитер, валидирует поле или устанавливает значение по умолчанию """
    return int(field) if field.isdigit() and int(field) < max_limit else default_limit


def last_date(date_string: str):
    return re.findall('\d{4}-\d{2}-\d{2}', date_string)[-1]


def humanize_date(iso_date: str) -> str:
    try:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S')

    return (iso_date + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M')


def parse_utm(utm: Utm):
    result = Result(utm=utm)
    div_inc = 0
    homepage, gostpage = Grab(), Grab()

    try:
        homepage.go(utm.build_url())
        gostpage.go(utm.gost_url())
        # версия
        try:
            result.version = homepage.doc.select('//*[@id="home"]/div[1]/div[2]').text()
            result.change_set = homepage.doc.select('//*[@id="home"]/div[2]/div[2]').text()
            result.build = homepage.doc.select('//*[@id="home"]/div[3]/div[2]').text()
        except DataNotFound:
            result.error.append('Не найдена информация о версии\n')

        # ИД
        try:
            fsrar_id = homepage.doc.select('//*[@id="RSA"]/div[2]').text()
            if utm.fsrar != fsrar_id.split(' ')[1].split('-')[2].split('_')[0]:
                result.error.append('ФСРАР не соответствует\n')
        except DataNotFound:
            result.error.append('Не найден ФСРАР ид\n')

        # Самодиагностика
        try:
            status_string = homepage.doc.select('//*[@id="home"]/div[4]/div[2]').text()
            result.status = 'RSA сертификат pki.fsrar.ru соответствует контуру' == status_string
            # если сатус не соотвествует указанному,значит появился блок Проблема с RSA и нумерация блоков увеличилась
            div_inc = not result.status
            license_string = homepage.doc.select(f'//*[@id="home"]/div[{5 + div_inc}]/div[2]').text()
            filter_string = homepage.doc.select('//*[@id="filterMsgDiv"]').text()
            result.filter = 'Обновление настроек не требуется' == filter_string
            result.license = 'Лицензия на вид деятельности действует' == license_string

        except DataNotFound:
            result.error.append('Не найдены все элементы на странице\n')

        # ключи
        try:
            gost_string = homepage.doc.select(f'//*[@id="home"]/div[{9 + div_inc}]/div[2]').text()
            pki_string = homepage.doc.select(f'//*[@id="home"]/div[{8 + div_inc}]/div[2]').text()
            result.pki = last_date(pki_string)
            result.gost = last_date(gost_string)
        except (IndexError, DataNotFound):
            result.error.append('Не найдены сроки ключей\n')

        # Дата отправки последнего чека не должна быть старше одного дня
        try:
            cheque_string = homepage.doc.select(f'// *[@id="home"]/div[{7 + div_inc}]/div[2]').text()
            today = datetime.strftime(datetime.now(), "%Y-%m-%d")

            if cheque_string == 'Отсутствуют неотправленные чеки':
                result.cheques = 'OK'
            elif last_date(cheque_string) == today:
                result.cheques = 'OK'
            else:
                result.cheques = last_date(cheque_string)
        except Exception as e:
            result.error.append(f'Проблема с отправкой чеков: {e}\n')

        try:
            pre = gostpage.doc.select('//pre').text()
            cn = re.compile(r'(?<=CN=)[^,]*')
            name = re.compile(r'(?<=GIVENNAME=)[^,]*')
            surname = re.compile(r'(?<=SURNAME=)[^,]*')

            cn_res = cn.search(pre)
            if cn_res is not None:
                result.legal = cn_res.group().replace('"', '').replace('\\', '').replace('ООО', '')[0:20]

            surname_res = surname.search(pre)
            if surname_res is not None:
                result.surname = surname_res.group()

            name_res = name.search(pre)
            if name_res is not None:
                result.given_name = name_res.group()

        except Exception as e:
            result.error.append(f'Не найден сертификат организации{e}\n')

    # не удалось соединиться
    except GrabTimeoutError:
        result.error.append('Нет связи: время истекло')

    except GrabCouldNotResolveHostError:
        result.error.append('Нет связи: не найден сервер')

    except GrabConnectionError:
        result.error.append('Нет связи: ошибка подключения')

    result.error = ' '.join(result.error)

    return result


def create_unique_xml(fsrar: str, content: str, path: str) -> str:
    tree = ET.parse(path)
    root = tree.getroot()
    root[0][0].text = fsrar
    root[1][0][0][0][1].text = content
    path = os.path.join(app.config['RESULT_FOLDER'], f'TTNQuery_{uuid.uuid4()}.xml')
    tree.write(path)
    return path


def create_unique_mark_xml(fsrar: str, mark: str, path: str) -> str:
    tree = ET.parse(path)
    root = tree.getroot()
    root[0][0].text = fsrar
    root[1][0][0].text = mark
    path = os.path.join(app.config['RESULT_FOLDER'], f'QueryFilter_{uuid.uuid4()}.xml')
    tree.write(path)
    return path


def send_xml(url: str, files):
    err = None
    try:
        r = requests.post(url, files=files)
        if ET.fromstring(r.text).find('sign') is None:
            err = ET.fromstring(r.text).find('error').text

    except requests.ConnectionError:
        err = 'УТМ недоступен'

    return err


def send_xml_cheque(url: str, files) -> str:
    try:
        response = requests.post(url, files=files)
        reply = ET.fromstring(response.text)
        if reply.find('url') is not None:
            return reply.find('url').text
        else:
            return reply.find('error').text

    except requests.ConnectionError:
        return 'Нет связи'


def clean_documents(url: str):
    counter = 0
    url_out = url + '/opt/out'
    doc_types = ('ReplyNATTN', 'TTNHISTORYF2REG')
    response = requests.get(url_out)
    tree = ET.fromstring(response.text)
    for u in tree.findall('url'):
        if any(ext in u.text for ext in doc_types):
            requests.delete(u.text)
            counter += 1
    return counter


def find_last_nattn(url: str) -> str:
    url_out = url + '/opt/out/ReplyNATTN'
    try:
        response = requests.get(url_out)
        tree = ET.fromstring(response.text)
        for nattn_url in reversed(tree.findall('url')):
            if 'ReplyNATTN' in nattn_url.text:
                return nattn_url.text
    except requests.exceptions.ConnectionError:
        flash('Ошибка подключения к УТМ')


def parse_reply_nattn(url: str):
    ttn_list, date_list, doc_list, nattn_list = [], [], [], []
    if url is not None:
        try:
            response = requests.get(url)
            tree = ET.fromstring(response.text)

            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}WbRegID'):
                ttn_list.append(elem.text)
            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnDate'):
                date_list.append(elem.text)
            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnNumber'):
                doc_list.append(elem.text)
            for i, ttn in enumerate(ttn_list):
                nattn_list.append([ttn_list[i], date_list[i], doc_list[i]])
        except requests.exceptions.RequestException as e:
            flash('Ошибка получения списка ReplyNoAnswerTTN', url)

        except Exception as e:
            flash(f'Ошибка обработки XML {e}', url)
    return nattn_list


def get_mysql_data(ukm_hostname: str, query: str) -> Optional[list]:
    """ Выполнение запроса к MySQL """

    try:
        mysql_config = app.config['MYSQL_CONN']
        mysql_config['host'] = ukm_hostname

        connection = MySQLdb.connect(**mysql_config)
        with connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()

        connection.close()

    except (MySQLdb.OperationalError, TypeError) as e:
        logging.error(e)
        data = None

    return data


def get_cheques_from_ukm(host: str, mark: str) -> Optional[list]:
    """ Получение списка чеков"""
    query = f"""
        SELECT 
            trm_out_receipt_header.date,
            trm_out_receipt_item.name,
            trm_out_receipt_header.type,
            trm_out_receipt_footer.result, 
            trm_out_receipt_egais.url
          FROM trm_out_receipt_item_egais
          left outer JOIN trm_out_receipt_item ON trm_out_receipt_item_egais.id = trm_out_receipt_item.id AND trm_out_receipt_item_egais.cash_id = trm_out_receipt_item.cash_id
          left outer JOIN trm_out_receipt_header ON trm_out_receipt_item.receipt_header = trm_out_receipt_header.id AND trm_out_receipt_item.cash_id = trm_out_receipt_header.cash_id
          left outer JOIN trm_out_receipt_footer ON trm_out_receipt_item.receipt_header = trm_out_receipt_footer.id AND trm_out_receipt_item.cash_id = trm_out_receipt_footer.cash_id
          left outer JOIN trm_out_receipt_egais ON trm_out_receipt_item.receipt_header = trm_out_receipt_egais.id AND trm_out_receipt_item.cash_id = trm_out_receipt_egais.cash_id
          where egais_barcode like '%{mark}%'
          order by trm_out_receipt_header.date asc
    """
    return get_mysql_data(host, query)


def compose_cheque_link(ukm_cheque: dict) -> str:
    """ Формирование строки - ссылки для чеков УКМ"""
    txt = f'{ukm_cheque["date"]} {ukm_cheque["name"]} — {"Возврат" if ukm_cheque["type"] == 4 else "Продажа"} — {"Завершен" if ukm_cheque["result"] == 0 else "Аннулирован"}'
    return f'<a href="{ukm_cheque["url"]}">{txt}</a>' if ukm_cheque["url"] else txt


def compose_error_result(error_date: datetime, mark: str, desc: str, cheques: Optional[list]) -> str:
    """ Результат по ошибке вместе с чеками по найденной марке"""
    res = f'{error_date}<br><strong>{desc}</strong>: <code>{mark if mark is not None else ""}</code>'
    if cheques is None:
        res = res + f'<br><strong>Не удалось получить чеки из УКМ</strong>'

    if cheques:
        cheques_text = [compose_cheque_link(c) for c in cheques]
        res = res + f'<br><strong>Сведения из УКМ:</strong><ol><li>{"<li>".join(cheques_text)}</ol>'
    return res


def catch_error_line(line: str, re_err):
    """ Поиск ошибок в строке и возврат текста ошибки"""
    error_result = re_err.search(line)
    return error_result.groups()[0] if error_result else None


def process_errors(errors: list, full: bool, ukm: str):
    """ Обработка,запись, формирование сообщений
    full означает, что нужно получать чеки по маркам УКМ
    """
    current_marks = []
    current_results = []
    for e in errors:

        # Вывести марки без дублей
        if e.mark not in current_marks:
            # Опционально вывести чеки по маркам
            cheques = get_cheques_from_ukm(ukm, e.mark) if full and e.mark is not None else []
            mark_text_result = compose_error_result(e.date, e.mark, e.error, cheques)
            current_results.append(mark_text_result)
            current_marks.append(e.mark)

    return current_results, len(current_marks)


def grab_utm_check_results_to_db(utms: List[Utm], col: Database):
    results: List[Result] = [parse_utm(utm) for utm in utms]
    results_to_dict = [res.to_dictionary() for res in results]
    save_results_to_db(results_to_dict, col)
    return results_to_dict


def save_results_to_db(results: List[dict], col: Database):
    try:
        col.update_many({}, {'$set': {'last': False}})
        col.insert_many(results)

    except Exception as e:
        logging.info(f"Не удалось записать результаты в БД: {e}")


@app.route('/')
def index():
    return redirect(url_for('status'))


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = TTNForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'ttn.html',
        'title': 'Повторный запрос TTN',
        'description': 'Повторная отправка ТТН в супермаг, без изменений в ЕГАИС',
        'form': form,
    }

    if request.method == 'POST':
        file = 'ttn.xml'
        xml = get_xml_template(file)

        wbregid = request.form['wbregid'].strip()
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar

        query = create_unique_xml(utm.fsrar, wbregid, xml)
        url = utm.url() + '/opt/in/QueryResendDoc'

        files = {'xml_file': (file, open(query, 'rb'), 'application/xml')}
        err = send_xml(url, files)
        log = f'QueryResendDoc: {wbregid} отправлена {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        logging.info(log)
        flash(log)

    return render_template(**params)


@app.route('/reject', methods=['GET', 'POST'])
def reject():
    form = TTNForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'reject.html',
        'title': 'Отозвать или отклонить TTN',
        'description': 'Приход будет отклонен, возврат будет отозван',
        'form': form,
    }

    if request.method == 'POST':
        file = 'reject.xml'
        filepath = get_xml_template(file)

        wbregid = request.form['wbregid'].strip()
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar

        url = utm.url() + '/opt/in/WayBillAct_v3'

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0][0].text = 'Rejected'
        root[1][0][0][2].text = str(date.today())
        root[1][0][0][3].text = wbregid
        filepath = os.path.join(app.config['RESULT_FOLDER'], f'TTNReject_{uuid.uuid4()}.xml')
        tree.write(filepath)
        files = {'xml_file': (file, open(filepath, 'rb'), 'application/xml')}
        err = send_xml(url, files)
        log = f'WayBillAct_v3: {wbregid} отправлен отзыв/отказ от {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        logging.info(log)
        flash(log)

    return render_template(**params)


@app.route('/request_repeal', methods=['GET', 'POST'])
def request_repeal():
    form = RequestRepealForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'request_repeal.html',
        'description': 'Запрос распроведения накладной и отмена актов постановки списания со склада',
        'title': 'Запрос распроведения',
        'form': form,
    }
    if request.method == 'POST':
        options = {
            'WB': {
                'file': 'wbrepeal.xml',
                'url': '/opt/in/RequestRepealWB'
            },
            'ACO': {
                'file': 'acorepeal.xml',
                'url': '/opt/in/RequestRepealACO'
            },
            'AWO': {
                'file': 'aworepeal.xml',
                'url': '/opt/in/RequestRepealAWO'
            },
        }
        repeal_type = request.form['r_type']
        repeal_data = options.get(repeal_type)
        filepath = get_xml_template(repeal_data['file'])
        wbregid = request.form['wbregid'].strip()
        request_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        utm = get_instance(request.form['fsrar'])
        url = utm.url() + repeal_data['url']
        form.fsrar.data = utm.fsrar
        form.r_type.data = repeal_type

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0].text = utm.fsrar
        root[1][0][2].text = request_date
        root[1][0][3].text = wbregid
        filepath = os.path.join(app.config['RESULT_FOLDER'], f'{repeal_type}Repeal_{uuid.uuid4()}.xml')
        tree.write(filepath)
        files = {'xml_file': (repeal_data['file'], open(filepath, 'rb'), 'application/xml')}
        err = send_xml(url, files)
        log = f'RequestRepeal{repeal_type}: {wbregid} отправлен запрос на распроведение {repeal_type} {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/confirm_repeal', methods=['GET', 'POST'])
def confirm_repeal():
    form = WBRepealConfirmForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'wbrepealconfirm.html',
        'title': 'Подтверждение распроведения TTN',
        'description': 'Подтверждаем запрос на распроведение накладной (возврат поставщику)',
        'form': form,
    }

    if request.method == 'POST':
        file = 'wbrepealconfirm.xml'
        filepath = get_xml_template(file)
        wbregid = request.form['wbregid'].strip()
        is_confirm = request.form['is_confirm']
        utm = get_instance(request.form['fsrar'])
        form.is_confirm.data = request.form['is_confirm']
        form.fsrar.data = utm.fsrar

        url = utm.url() + '/opt/in/ConfirmRepealWB'

        request_date = datetime.now().strftime("%Y-%m-%d")

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0][0].text = is_confirm
        root[1][0][0][2].text = request_date
        root[1][0][0][3].text = wbregid
        root[1][0][0][4].text = is_confirm
        filepath = os.path.join(app.config['RESULT_FOLDER'], f'WBrepealConfirm_{uuid.uuid4()}.xml')

        tree.write(filepath)

        files = {'xml_file': (file, open(filepath, 'rb'), 'application/xml')}

        err = send_xml(url, files)
        log = f'ConfirmRepealWB: {wbregid} подтверждения распроведения {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/check_nattn', methods=['GET', 'POST'])
def check_nattn():
    form = FsrarForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'check_nattn.html',
        'title': 'Необработанные TTN',
        'description': 'Запрос в ЕГАИС списка необработанных документ, проверить через 5 минут',
        'form': form,
    }

    if request.method == 'POST':
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar
        if 'check' in request.form:
            ttn_list = parse_reply_nattn(find_last_nattn(utm.url()))
            if ttn_list is None:
                flash('Нет запроса необработанных документов')
            elif not ttn_list:
                flash('Все документы обработаны')
            else:
                flash('Необработанные документы в списке результатов')

            params['title'] = utm.title
            params['ttn_list'] = ttn_list

        if 'request' in request.form:
            file = 'nattn.xml'
            xml = get_xml_template(file)

            utm = get_instance(request.form['fsrar'])
            form.fsrar.data = utm.fsrar

            url = utm.url() + '/opt/in/QueryNATTN'

            query = create_unique_xml(utm.fsrar, utm.fsrar, xml)
            files = {'xml_file': (file, open(query, 'rb'), 'application/xml')}
            err = send_xml(url, files)

            log = f'QueryNATTN: Отправлен запрос {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'

            logging.info(log)
            flash(Markup(log))

    return render_template(**params)


@app.route('/service_clean', methods=['GET', 'POST'])
def service_clean():
    def clean(utm: Utm):
        # todo: внести clean_documents
        try:
            return utm.title, clean_documents(utm.url())
        except Exception as e:
            return utm.title, f'недоступен {e}'

    form = FsrarForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'service.html',
        'title': 'Удаление Форм 2 из УТМ',
        'description': 'Удаление Форм 2, ReplyNATTN со всех УТМ',
        'form': form
    }

    if request.method == 'POST':
        results = []
        if 'select' in request.form:
            utm = get_instance(request.form['fsrar'])
            results.append(clean(utm))
            form.fsrar.data = utm.fsrar

        elif 'all' in request.form:
            for utm in cfg.utms:
                results.append(clean(utm))

        params['results'] = results

    return render_template(**params)


@app.route('/cheque', methods=['GET', 'POST'])
def cheque():
    form = ChequeForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'cheque.html',
        'description': 'Ручная отправка чеков на возврат',
        'title': 'Отправка чека',
        'form': form,
    }
    if request.method == 'POST':
        utm = get_instance(request.form['fsrar'])

        # creating document with cheque header attributes
        document = ET.Element('Cheque')
        document.set('name', utm.title)
        document.set('address', utm.title)
        document.set('inn', request.form['inn'].strip())
        document.set('kpp', request.form['kpp'].strip())
        document.set('kassa', request.form['kassa'].strip())
        document.set('number', request.form['number'].strip())
        document.set('shift', request.form['shift'].strip())
        document.set('datetime', datetime.today().strftime("%d%m%y%H%M"))

        # inserting bottle subelement with attributes
        node = ET.SubElement(document, 'Bottle')
        node.set('barcode', request.form['bottle'].strip())
        node.set('price', request.form['price'].strip())

        # done, writing to xml file
        file = 'cheque.xml'
        tree = ET.ElementTree(document)
        tree.write(get_xml_template(file), encoding='utf-8', xml_declaration=True)

        # send xml and write log
        files = {'xml_file': (file, open(get_xml_template(file), 'rb'), 'application/xml')}
        result = send_xml_cheque(utm.xml_url(), files)

        log = f"Cheque: ТТ {utm.title} [{utm.fsrar}]: {request.form['bottle']}  цена: {request.form['price']}: {result}"
        flash(log)
        logging.info(log)

        return redirect('/cheque')

    return render_template(**params)


@app.route('/mark', methods=['GET', 'POST'])
def check_mark():
    form = MarkForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'mark.html',
        'title': 'Проверка марок УТМ',
        'description': 'Запрос наличия марки в УТМ (для помарочного учета)',
        'form': form,
    }
    if request.method == 'POST':
        res = None
        file = 'queryfilter.xml'
        xml = get_xml_template(file)
        url_suffix = '/opt/in/QueryFilter'
        mark = request.form['mark'].strip()
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar

        query = create_unique_mark_xml(utm.fsrar, mark, xml)
        url = utm.url() + url_suffix
        files = {'xml_file': (file, open(query, 'rb'), 'application/xml')}
        try:
            r = requests.post(url, files=files)
            for sign in ET.fromstring(r.text).iter('{http://fsrar.ru/WEGAIS/QueryFilter}result'):
                res = sign.text
        except requests.ConnectionError:
            res = 'УТМ недоступен'

        log = f'Проверка марки: {utm.title} [{utm.fsrar}] {mark[:16]}...{mark[-16:]} {res}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/utm_logs', methods=['GET', 'POST'])
def get_utm_errors():
    form = FsrarForm()
    form.fsrar.choices = cfg.utm_choices()
    form.fsrar.data = request.args.get('fsrar')
    params = {
        'template_name_or_list': 'utm_log.html',
        'title': 'УТМ поиск ошибок чеков',
        'description': 'Поиск ошибок в журнале чеков УТМ',
        'form': form,
    }
    if request.method == 'POST':
        from get_logs import parse_log_for_errors, parse_errors
        results = dict()

        log_name = 'transport_transaction.log'
        params['date'] = datetime.now().strftime(app.config['HUMAN_DATE_FORMAT'])
        form.fsrar.data = request.form['fsrar']

        all_utm = request.form.get('all', False)
        utm = cfg.utms if all_utm else [get_instance(request.form['fsrar']), ]

        for u in utm:
            transport_log = u.log_dir() + log_name
            utm_header = f'{u.title} [<a target="_blank" href="/utm_logs?fsrar={u.fsrar}">{u.fsrar}</a>] '

            errors_found, checks, err = parse_log_for_errors(transport_log)
            errors_objects = parse_errors(errors_found, u)
            error_results, marks = process_errors(errors_objects, not all_utm, u.ukm_host())
            summary = err if err is not None else f'Всего чеков: {checks}, ошибок {len(errors_objects)}, уникальных {marks}'
            results[utm_header + summary] = error_results

        params['results'] = results
        params['total'] = len(results)
        params['error_count'] = len([x for x in results.values() if len(x) > 0])
        params['total_errors'] = sum(len(v) for v in results.values())

    return render_template(**params)


@app.route('/rests', methods=['GET', 'POST'])
def get_rests():
    form = RestsForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'rests.html',
        'title': 'Поиск остатков в обмене',
        'description': 'Показывает остатки по алкокодам из последниx запросов',
        'form': form,
    }

    if request.method == 'POST':
        results = dict()
        exclude = ['Error']

        search_alc_code = request.form['alc_code'].strip()
        limit = get_limit(request.form['limit'], 50, 10)
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar

        for root, dirs, files in os.walk(utm.path):
            dirs[:] = [d for d in dirs if d not in exclude]
            files = [fi for fi in files if fi.find("ReplyRestsShop_v2") > 0]
            files.sort(reverse=True)

            for reply_rests in files[:limit]:
                with open(os.path.join(utm.path, reply_rests), encoding="utf8") as f:
                    rests_dict = xmltodict.parse(f.read())
                    rests_shop = rests_dict.get('ns:Documents').get('ns:Document').get('ns:ReplyRestsShop_v2')
                    rest_date = humanize_date(rests_shop.get('rst:RestsDate'))

                    for position in rests_shop.get('rst:Products').get('rst:ShopPosition'):
                        alc_code = position.get('rst:Product').get('pref:AlcCode')
                        quantity = position.get('rst:Quantity')
                        if search_alc_code in ('', alc_code):
                            if results.get(alc_code, False):
                                results[alc_code][rest_date] = quantity
                            else:
                                results[alc_code] = {rest_date: quantity}

        params['results'] = results

    return render_template(**params)


@app.route('/ticket', methods=['GET', 'POST'])
def get_tickets():
    form = TicketForm()
    form.fsrar.choices = cfg.utm_choices()
    params = {
        'template_name_or_list': 'ticket.html',
        'title': 'Поиск квитанций обмена',
        'description': 'Показывает квитанции из обмена по названию документа',
        'form': form,
    }

    if request.method == 'POST':

        results = list()
        doc = request.form['search'].strip()
        limit = request.form['limit'].strip()
        limit = int(limit) if limit.isdigit() and int(limit) < 5000 else 1000
        utm = get_instance(request.form['fsrar'])
        form.fsrar.data = utm.fsrar

        for root, dirs, files in os.walk(utm.path):
            files = [fi for fi in files if fi.find("Ticket") > 0]
            files.sort(reverse=True)

            for reply_rests in files[:limit]:
                with open(os.path.join(root, reply_rests), encoding="utf8") as f:
                    raw_data = f.read()
                    if doc in raw_data:
                        ticket_dict = xmltodict.parse(raw_data)
                        ticket_data = ticket_dict.get('ns:Documents').get('ns:Document').get('ns:Ticket')
                        if ticket_data is not None:
                            results.append(ticket_data)

        params['results'] = results

    return render_template(**params)


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    form = UploadForm()
    params = {
        'template_name_or_list': 'upload.html',
        'title': 'Конвертер',
        'form': form,
    }
    if request.method == 'POST':
        if not request.files.get('file'):
            flash('Файл не выбран')
            return redirect(request.url)

        file = request.files['file']

        if not validate_filename(file.filename):
            flash('Выберите XLSX документ')
            return redirect(request.url)

        if file:
            filename = f'{uuid.uuid4()}_{secure_filename(file.filename)}'
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            errors = list()

            export_date = datetime.strftime(datetime.now() - timedelta(1), app.config['CONVERTER_DATE_FORMAT'])

            def collect_import_data(filename: str) -> dict:
                results = dict()

                try:
                    df = pd.read_excel(
                        io=filename,
                        na_filter=False,                                # не проверяет на попадание в список NaN
                        sheet_name='Sheet1',
                        usecols=['SKU', 'Код склада'],                  # только 2 поля
                        converters={'SKU': str, 'Код склада': int},     # типы для полей
                    )
                    results = df.groupby('Код склада')['SKU'].apply(list).to_dict()
                except Exception as e:
                    errors.append(f'Не удалось прочитать файл импорта {e}')

                return results

            def collect_export_results(imp: dict) -> dict:
                results = dict()
                for warehouse in imp.keys():
                    exp_filename = f'skubody_{warehouse}_{export_date}.csv'
                    exp_filepath = os.path.join(app.config['CONVERTER_EXPORT_PATH'], exp_filename)

                    if os.path.isfile(exp_filepath):
                        warehouse_articles = pd.read_csv(
                            filepath_or_buffer=exp_filepath,
                            sep='¦',
                            header=None,            # без шапки, включая 1 строку
                            usecols=[0, ],          # первое поле с данным
                            squeeze=True,           # т.к. 1 поле, то ужимаем в лист
                            engine='python',        # для корректной обработки разделителя
                            encoding='utf-8',       # обязательно, т.к разделитель
                            converters={0: str}     # поле как строка
                        ).to_list()
                        results[warehouse] = warehouse_articles

                    else:
                        errors.append(f'Место хранения {warehouse}: Файл {exp_filepath} недоступен')

                return results

            def make_difference(imp: dict, exp: dict):
                results = dict()
                if imp and exp:
                    for warehouse, articles in imp.items():
                        list_imp = articles
                        list_exp = exp.get(warehouse)
                        if list_exp is not None:
                            difference = set(list_imp).difference(set(list_exp))
                            results[warehouse] = difference
                        else:
                            errors.append(f'Место хранения {warehouse} пропущено')

                return results

            def write_down(fin: dict) -> str:
                result = ''
                if fin:
                    wb = openpyxl.load_workbook(get_xml_template(app.config['CONVERTER_TEMPLATE_FILE']))
                    sh = wb.active
                    sh._current_row = 6  # header row, to append after

                    today = datetime.now().strftime(app.config['CONVERTER_DATE_FORMAT'])
                    result = f'autosupply_results_{today}_{uuid.uuid4()}.xlsx'
                    result_path = os.path.join(app.config['RESULT_FOLDER'], result)

                    for wh, articles in fin.items():
                        for article in articles:
                            sh.append((article, wh))

                    wb.save(result_path)
                return result

            import_results = collect_import_data(filepath)
            export_results = collect_export_results(import_results)
            finale_results = make_difference(import_results, export_results)
            result_filename = write_down(finale_results)

            if errors:
                flash('\n'.join(errors))

            if result_filename:
                return redirect(url_for('uploaded_file', filename=result_filename))

    return render_template(**params)


@app.route('/results/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['RESULT_FOLDER'], filename)


@app.route('/add_utm', methods=['GET', 'POST'])
def add_utm():
    def create_utm_from_request_form(request_form) -> Utm:
        """ Создание Utm инстанса из формы запроса"""
        signature = inspect.signature(Utm.__init__)
        args = signature.parameters.keys()
        return Utm(**{parameter: request_form.get(parameter) for parameter in request_form if parameter in args})

    form = CreateUpdateUtm()
    params = {
        'template_name_or_list': 'add_utm.html',
        'title': 'Добавление УТМ',
        'form': form
    }

    if request.method == 'POST':
        new_utm = create_utm_from_request_form(request.form)
        cfg.create_or_update_utm(new_utm)

    return render_template(**params)


@app.route('/status', methods=['GET', 'POST'])
def status():
    default = request.args.get('ordering') or 'title'
    form = StatusSelectOrder()
    form.ordering.data = default

    utm_filter = get_instance(request.form.get('filter'))
    if utm_filter is not None:
        try:
            flash(f"{utm_filter.title}[{utm_filter.fsrar}]: {requests.get(utm_filter.reset_filter_url()).text}")
        except (requests.ConnectionError, requests.ReadTimeout) as e:
            flash(f'Не удалось выполнить запрос обновления {e}, УТМ недоступен')

    params = {
        'template_name_or_list': 'status.html',
        'title': 'Статус (новый)',
        'description': 'Результат последней проверки УТМ',
        'ord': default,
        'form': form,
    }
    try:
        with MongoClient(app.config['MONGO_CONN']) as client:
            col = client[app.config['MONGO_DB']][app.config['MONGO_COL_RES']]

            results = list(col.find({'last': True}).sort(request.args.get('ordering', default), 1))
        params['results'] = results

    except Exception as e:
        flash(f'{e} Выполните полную проверку')
        logging.error(e)

    return render_template(**params)


@app.route('/status_check', methods=['GET'])
def status_check():
    params = {
        'title': 'Проверка УТМ',
        'template_name_or_list': 'status.html',
        'description': 'Проверка по требованию, выполняектся около минуты',
        'form': StatusSelectOrder(),
    }

    with MongoClient(app.config['MONGO_CONN']) as client:
        col = client[app.config['MONGO_DB']][app.config['MONGO_COL_RES']]
        results = grab_utm_check_results_to_db(cfg.utms, col)

    ordering = request.form.get('ordering', 'title')
    results.sort(key=lambda result: result[ordering])
    params['results'] = results

    return render_template(**params)


@app.route('/postman', methods=['GET'])
def postman_check():
    """ Проверка невыгруженных в обмен XML
    """
    params = {
        'title': 'Проверка обмена Супермаг STORGCO',
        'template_name_or_list': 'postman.html',
        'description': 'Файлы XML необработанные почтовым модулем',
    }

    with MongoClient(app.config['MONGO_CONN']) as client:
        col = client[app.config['MONGO_DB']][app.config['MONGO_COL_QUE']]

        try:
            results = col.find_one({}, sort=[('_id', DESCENDING)])
            if results:
                params['results'] = results['files']
                params['total'] = results['total']
                params['date'] = results['date']

        except Exception as e:
            logging.error(f'PostMan: не удалось получить результаты: {e}')

    return render_template(**params)


@app.route('/xml/', methods=['GET', 'POST'])
def test_utm():
    """ Тестовый УТМ для "подписи" чеков """
    return '<?xml version="1.0" encoding="UTF-8" standalone="no"?><A>' \
           '<url>http://check.egais.ru?id=b9ae79cf-b019-474d-a2da-eda7faa834b1&amp;dt=0101200000&amp;cn=010000123456</url>' \
           '<sign>1F4F407419A4CFDDD8B8A359B9AE2CE9E793F4E5057BB924321923E5A2C2184BE6F61A77932A9EE365FFD40A181102C7474072E8D9058565B49D417220A2A2EA</sign><ver>2</ver></A>'


@app.route('/view_errors', methods=['GET'])
def view_errors():
    def add_default_choice(choices: list) -> list:
        choices.insert(0, (0, 'Выберите...'))
        return choices

    def pipeline_group_by(field: str, start_date: datetime, limit: Optional[int] = None, ):
        """ Итоги сгруппированны по полям, с указанной даты, опционально лимит """
        result = [
            {"$match": {
                "date": {
                    "$gte": start_date
                },
            }
            },
            {"$group": {
                "_id": {
                    f"{field}": f"${field}",
                },
                "count": {"$sum": 1}
            }
            },
            {"$sort": SON([("count", -1), ("_id", -1)])},

        ]
        if limit:
            result.append({"$limit": limit})
        return result

    def validate_arg(arg: str) -> bool:
        """ Исключаем невалидные аргументы """
        return False if arg in ['0', '', ' ', '', None] else True

    def short_choices_hash(title: str) -> int:
        """ ИД для динамических полей выбора, у которых нет естественных идентификаторов """
        return hash(title) % 256

    last_days = app.config['MARK_ERRORS_LAST_DAYS']
    last_utms = app.config['MARK_ERRORS_LAST_UTMS']

    form = MarkFormError()
    form.fsrar.choices = cfg.utm_choices()
    add_default_choice(form.fsrar.choices)
    form.fsrar.data = int(request.args.get('fsrar', 0))
    week_ago = datetime.now() - timedelta(days=last_days)

    params = {
        'form': form,
        'title': 'История ошибок УТМ',
        'template_name_or_list': 'error_stats.html',
        'description': f'Статистика ошибок за {last_days}',
    }

    try:
        with MongoClient(app.config['MONGO_CONN']) as client:
            col = client[app.config['MONGO_DB']][app.config['MONGO_COL_ERR']]

            # Т.к поле с типом ошибок динамическое, мы сначала получаем этот список из MongoDB
            errors_types = list(col.aggregate(pipeline_group_by('error', week_ago)))
            # Собираем выпадайку с вариантами, добавляем туда пустой элемент
            choices_list = list((short_choices_hash(x['_id']['error']), x['_id']['error']) for x in errors_types)
            form.error.choices = add_default_choice(choices_list)

            params['error_type_total'] = errors_types
            params['fsrar_total'] = col.aggregate(pipeline_group_by('title', week_ago, last_utms))

            if request.args:
                # Если были переданы параметры, то собираем пайплайн фильтра ошибок из них
                pipeline_mark = {k: v for k, v in dict(request.args).items() if validate_arg(v)}
                error_arg = request.args.get('error')
                # Т.к. ошибки у нас динамические, берем из словаря по ИД
                if validate_arg(error_arg):
                    form.error.data = int(request.args.get('error', 0))
                    choices_dict = {short_choices_hash(x['_id']['error']): x['_id']['error'] for x in errors_types}
                    pipeline_mark['error'] = choices_dict.get(int(error_arg))

                params['results'] = col.find(pipeline_mark).sort([('title', 1), ('date', -1)])

    except Exception as e:
        err = f'Недоступна БД {e}'
        logging.error(err)
        flash(err)

    return render_template(**params)


logging.basicConfig(
    filename='app.log',
    level=logging.getLevelName(os.environ.get('LEVEL'), 'INFO'),
    format='%(asctime)s %(levelname)s: %(message)s'
)

with MongoClient(app.config['MONGO_CONN']) as cl:
    db = cl[app.config['MONGO_DB']]
    cfg = Configs(db)

for file in app.config.get('WORKING_DIRS', ()):
    create_folder(file)
