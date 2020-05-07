import logging
import os
import uuid
import xml.etree.ElementTree as ET
from abc import ABC
from datetime import date, datetime, timedelta
from typing import Optional, Iterable

import MySQLdb
import requests
import xmltodict
from bson import ObjectId
from bson.son import SON
from flask import Flask, Markup, flash, request, redirect, url_for, render_template
from flask_pymongo import PyMongo

from forms import FsrarForm, RestsForm, TicketForm, CreateUpdateUtm, StatusSelectOrder, MarkFormError, \
    MarkForm, ChequeForm, WBRepealConfirmForm, RequestRepealForm, TTNForm

app = Flask(__name__)
app.config.from_object('config.AppConfig')
app.secret_key = os.environ.get('FLASK_SECRET_KEY')
app.config['MONGO_URI'] = os.environ.get('MONGO_URI')

mongo = PyMongo(app)


class MongoStorage(ABC):
    def __init__(self, **kwargs):
        _id = kwargs.get('_id')
        if _id is not None:
            self._id = _id

    @classmethod
    def _get_all(cls, **kwargs):
        flt = {} if kwargs is None else kwargs
        return list(mongo.db[cls.__name__.lower()].find(flt))

    @classmethod
    def _get_one(cls, **kwargs):
        flt = {} if kwargs is None else kwargs
        return mongo.db[cls.__name__.lower()].find_one(flt)

    def _cleaned(self):
        data = {k: v for k, v in vars(self).items() if v is not None}
        data.pop('_id', None)
        return data

    def _update(self):
        # todo: update creates new instance
        mongo.db[self.__class__.__name__.lower()].replace_one({'_id': ObjectId(self._id)}, self._cleaned())

    def _create(self):
        mongo.db[self.__class__.__name__.lower()].insert_one(self._cleaned())

    @classmethod
    def _archive(cls):
        return mongo.db[cls.__name__.lower()].update_many({}, {'$set': {'active': False}})

    @classmethod
    def _save_many(cls, results):
        return mongo.db[cls.__name__.lower()].insert_many(results)


class Utm(MongoStorage):
    """ УТМ
    Включает в себя название, адрес сервера, заголовок-адрес, путь к XML обмену Супермага
    """

    @classmethod
    def get_one(cls, **kwargs):
        return Utm(**cls._get_one(**kwargs))

    @classmethod
    def get_all(cls):
        return [Utm(**u) for u in cls._get_all()]

    @classmethod
    def get_active(cls):
        return [Utm(**u) for u in cls._get_all(active=True)]

    @classmethod
    def utm_choices(cls):
        return [(u.fsrar, f'{u.title} [{u.fsrar}] [{u.host}]') for u in cls.get_active()]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ukm: str = kwargs.get('ukm')
        self.host: str = kwargs.get('host')
        self.fsrar: str = kwargs.get('fsrar')
        self.title: str = kwargs.get('title')
        self.path: str = kwargs.get('path', self._get_path())
        self.active: bool = kwargs.get('active', False)

    def __str__(self):
        return f'{self.fsrar} {self.title}'

    def __repr__(self):
        return f'{self.fsrar} {self.title}'

    @property
    def clean(self):
        return self._cleaned()

    def _get_path(self):
        return f'{app.config["DEFAULT_XML_PATH"]}{self.host.split("-")[0]}/in/'

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

    def create_or_update(self):
        return self._update() if self._id is None else self._create()


class Result(MongoStorage):
    """ Результаты опроса УТМ
    С главной страницы получаем:
    * Состояние УТМ и лицензии
    * Сроки ключей ГОСТ, PKI
    * Состояние чеков
    * Организация из сертификата ГОСТ

    Фиксируются все ошибки при парсинге
    Данные УТМ переносятся в результат для вывода в шаблон Jinja2

    """

    @classmethod
    def save_many(cls, results: Iterable['Result']):
        cls._archive()
        return cls._save_many([vars(r) for r in results])

    @classmethod
    def add_many(cls, results: Iterable[dict]):
        cls._archive()
        return cls._save_many(results)

    def __init__(self, utm=None, **kwargs):
        super().__init__(**kwargs)

        if utm is not None:
            self.fsrar: str = utm.fsrar
            self.host: str = utm.host
            self.url: str = utm.url()
            self.title: str = utm.title

        self.legal: str = kwargs.get('legal', '')
        self.surname: str = kwargs.get('surname', '')
        self.given_name: str = kwargs.get('given_name', '')
        self.gost: str = kwargs.get('gost', '')
        self.pki: str = kwargs.get('pki', '')
        self.cheques: str = kwargs.get('cheques', '')
        self.status: bool = kwargs.get('status', False)
        self.licence: bool = kwargs.get('licence', False)
        self.error: list = kwargs.get('errors', [])
        self.filter: bool = kwargs.get('filter', False)
        self.docs_in: int = kwargs.get('docs_in', 0)
        self.docs_out: int = kwargs.get('docs_out', 0)
        self.version: str = kwargs.get('cheques', '')
        self.change_set: str = kwargs.get('cheques', '')
        self.build: str = kwargs.get('build', '')
        self.date = kwargs.get('date', datetime.utcnow())
        self.active = True


def get_xml_template(filename: str) -> str:
    return os.path.join('xml/', filename)


def get_limit(field: str, max_limit: int, default_limit: int) -> int:
    """ Лимитер, валидирует поле или устанавливает значение по умолчанию """
    return int(field) if field.isdigit() and int(field) < max_limit else default_limit


def humanize_date(iso_date: str) -> str:
    try:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        iso_date = datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S')

    return (iso_date + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M')


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
            for i, _ in enumerate(ttn_list):
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
        mark, date, err = e['mark'], e['date'], e['error']
        # Вывести марки без дублей
        if mark not in current_marks:
            # Опционально вывести чеки по маркам
            cheques = get_cheques_from_ukm(ukm, mark) if full and mark is not None else []

            mark_text_result = compose_error_result(date, mark, err, cheques)
            current_results.append(mark_text_result)
            current_marks.append(e['mark'])

    return current_results, len(current_marks)


@app.route('/')
def index():
    return redirect(url_for('status'))


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = TTNForm()
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
    params = {
        'template_name_or_list': 'check_nattn.html',
        'title': 'Необработанные TTN',
        'description': 'Запрос в ЕГАИС списка необработанных документ, проверить через 5 минут',
        'form': form,
    }

    if request.method == 'POST':
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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

            utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
    params = {
        'template_name_or_list': 'service.html',
        'title': 'Удаление Форм 2 из УТМ',
        'description': 'Удаление Форм 2, ReplyNATTN со всех УТМ',
        'form': form
    }

    if request.method == 'POST':
        results = []
        if 'select' in request.form:
            utm = Utm.get_one(fsrar=request.form['fsrar'])
            results.append(clean(utm))
            form.fsrar.data = utm.fsrar

        elif 'all' in request.form:
            for utm in Utm.get_active():
                results.append(clean(utm))

        params['results'] = results

    return render_template(**params)


@app.route('/cheque', methods=['GET', 'POST'])
def cheque():
    form = ChequeForm()
    form.fsrar.choices = Utm.utm_choices()
    params = {
        'template_name_or_list': 'cheque.html',
        'description': 'Ручная отправка чеков на возврат',
        'title': 'Отправка чека',
        'form': form,
    }
    if request.method == 'POST':
        utm = Utm.get_one(fsrar=request.form['fsrar'])

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
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
        except UnicodeError:
            res = 'Ошибка в URL проверьте переменные окружения'
        log = f'Проверка марки: {utm.title} [{utm.fsrar}] {mark[:16]}...{mark[-16:]} {res}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/utm/logs', methods=['GET', 'POST'])
def get_utm_errors():
    form = FsrarForm()
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_active() if all_utm else [Utm.get_one(fsrar=request.form['fsrar']), ]

        for u in utm:
            transport_log = u.log_dir() + log_name
            transport_log = 'transport_transaction.log'
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
    form.fsrar.choices = Utm.utm_choices()
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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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
    form.fsrar.choices = Utm.utm_choices()
    form.search.data = request.args.get('res')
    form.limit.data = int(request.args.get('limit', 1000))
    form.fsrar.data = int(request.args.get('fsrar', 0))

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
        utm = Utm.get_one(fsrar=request.form['fsrar'])
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


@app.route('/utm/list', methods=['GET', ])
def list_utm():
    params = {
        'title': 'Список УТМ',
        'results': Utm.get_all(),
        'template_name_or_list': 'utm_list.html',
    }

    return render_template(**params)


@app.route('/utm/add', methods=['GET', 'POST'])
def add_utm():
    params = {
        'template_name_or_list': 'utm.html',
        'form': CreateUpdateUtm(),
        'title': 'Новый УТМ',
    }

    if request.method == 'POST':
        data = dict(request.form)
        result = mongo.db.utm.insert_one(data).inserted_id
        return redirect(url_for('edit_utm', utm_id=result))

    return render_template(**params)


@app.route('/utm/edit/<ObjectId:utm_id>', methods=['GET', 'POST', 'DELETE'])
def edit_utm(utm_id):
    form = CreateUpdateUtm()
    params = {
        'template_name_or_list': 'utm.html',
        'title': 'Редактировать УТМ',
        'form': form
    }

    utm = mongo.db.utm.find_one_or_404(utm_id)

    if request.method == 'POST':
        data = {k: v for k, v in request.form.items()}
        utm.update(data)
        utm['active'] = True if data.get('active') else False

        mongo.db.utm.replace_one({'_id': utm.pop('_id')}, utm)

    if request.method == 'DELETE':
        mongo.db.utm.delete_many({'_id': utm.pop('_id')})
        return redirect(url_for('list_utm'))

    form.process(**utm)

    return render_template(**params)


@app.route('/status', methods=['GET', 'POST'])
def status():
    ordering = request.args.get('ordering', 'title')
    form = StatusSelectOrder()
    form.ordering.data = ordering

    params = {
        'template_name_or_list': 'status.html',
        'title': 'Статус (новый)',
        'description': 'Результат последней проверки УТМ',
        'ord': ordering,
        'form': form,
        'results': mongo.db.result.find({'last': True}).sort(ordering, 1)
    }

    update_filter = request.form.get('filter')
    if update_filter is not None:
        utm_filter = Utm.get_one(fsrar=update_filter)
        if utm_filter is not None:
            try:
                flash(f"{utm_filter.title}[{utm_filter.fsrar}]: {requests.get(utm_filter.reset_filter_url()).text}")
            except (requests.ConnectionError, requests.ReadTimeout) as e:
                flash(f'Не удалось выполнить запрос обновления {e}, УТМ недоступен')

    return render_template(**params)


@app.route('/xml/', methods=['GET', 'POST'])
def test_utm():
    """ Тестовый УТМ для "подписи" чеков """
    return '<?xml version="1.0" encoding="UTF-8" standalone="no"?><A>' \
           '<url>http://check.egais.ru?id=b9ae79cf-b019-474d-a2da-eda7faa834b1&amp;dt=0101200000&amp</url>' \
           '<sign>1F4F407419A4CFDDD8B8A359B9AE2CE9E793F4E5057BB924321923E5A2C2184BE6F61A77932</sign><ver>2</ver></A>'


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
    form.fsrar.choices = Utm.utm_choices()
    add_default_choice(form.fsrar.choices)
    form.fsrar.data = int(request.args.get('fsrar', 0))
    week_ago = datetime.now() - timedelta(days=last_days)

    params = {
        'form': form,
        'title': 'История ошибок УТМ',
        'template_name_or_list': 'error_stats.html',
        'description': f'Статистика ошибок за {last_days}',
    }
    col = mongo.db.marks
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

    return render_template(**params)


logging.basicConfig(
    filename='app.log',
    level=logging.getLevelName(os.environ.get('LEVEL', 'INFO')),
    format='%(asctime)s %(levelname)s: %(message)s'
)
