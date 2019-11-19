import csv
import logging
import os
import re
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from typing import List

import openpyxl
import requests
import xmltodict
from flask import Flask, Markup, flash, request, redirect, url_for, send_from_directory, render_template
from flask_wtf import FlaskForm
from grab import Grab
from grab.error import GrabCouldNotResolveHostError, GrabConnectionError, GrabTimeoutError
from weblib.error import DataNotFound
from werkzeug.utils import secure_filename
from wtforms import StringField, IntegerField, SelectField, FileField
from wtforms.validators import DataRequired, Length, Regexp

UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
DOMAIN = 'severotorg.local'
UTM_PORT = '8080'
UTM_LOG = 'c$/utm/transporter/l/transport_transaction.log'
CONVERTER_TEMPLATE_FILE = os.environ.get('CONVERTER_SKU_TEMPLATE') or 'sku-body-template.xlsx'
CONVERTER_EXPORT_PATH = os.environ.get('CONVERTER_EXPORT_PATH') or './'
CONVERTER_DATE_FORMAT = '%Y%m%d'
ALLOWED_EXTENSIONS = {'xlsx', }

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULT_FOLDER'] = RESULT_FOLDER

app.secret_key = 'dev'

logging.basicConfig(filename='log', level=logging.WARNING, format='%(asctime)s %(levelname)s: %(message)s')


class Utm:
    """ УТМ
    Включает в себя название, адрес сервера, заголовок-адрес, путь к XML обмену Супермага
    """

    def __init__(self, fsrar, host, title, path):
        self.fsrar = fsrar
        self.host = host
        self.title = title
        self.path = path

    def __str__(self):
        return f'{self.fsrar} {self.title}'

    def __repr__(self):
        return f'{self.fsrar} {self.title}'

    def url(self):
        return f'http://{self.host}.{DOMAIN}:{UTM_PORT}'

    def build_url(self):
        return self.url() + '/?b'

    def version_url(self):
        return self.url() + '/info/version'

    def gost_url(self):
        return self.url() + '/info/certificate/GOST'

    def docs_in_url(self):
        return self.url() + '/opt/out/waybill_v3'

    def docs_out_url(self):
        return self.url() + '/opt/in'

    def xml_url(self):
        return self.url() + '/xml'


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

    def __init__(self, utm: Utm):
        self.utm: Utm = utm  # fsrar, server, title
        self.legal: str = ''
        self.gost: str = ''
        self.pki: str = ''
        self.cheques: str = ''
        self.status: bool = False
        self.licence: bool = False
        self.error: list = []
        self.fsrar: str = self.utm.fsrar
        self.host: str = self.utm.host
        self.url: str = self.utm.url()
        self.title: str = self.utm.title
        self.filter: bool = False
        self.docs_in: int = 0
        self.docs_out: int = 0
        self.version = ''
        self.changeset = ''
        self.build = ''


with open('config', 'r', encoding='utf-8') as f:
    utmlist = [Utm(*u.split(';')) for u in f.read().splitlines()]
    utmlist.sort(key=lambda utm: utm.title)

UTM_CHOICES = [(u.fsrar, f'{u.title} [{u.fsrar}] [{u.host}]') for u in utmlist]


class FsrarForm(FlaskForm):
    fsrar = SelectField('fsrar', choices=UTM_CHOICES)


class UploadForm(FlaskForm):
    file = FileField()


class RestsForm(FsrarForm):
    alccode = StringField('alccode')
    limit = IntegerField('limit')


class TicketForm(FsrarForm):
    search = StringField('search', validators=[DataRequired()])
    limit = IntegerField('limit')


class MarkForm(FsrarForm):
    mark = StringField('mark', validators=[DataRequired()])


class TTNForm(FsrarForm):
    wbregid = StringField('wbregid', validators=[DataRequired()])


class RequestRepealForm(TTNForm):
    r_type = SelectField('r_type',
                         choices=(('WB', 'TTN'), ('AWO', 'Акт списания (WOF-)'), ('ACO', 'Акт постановки (INV-)')))


class WBRepealConfirmForm(TTNForm):
    is_confirm = SelectField('is_confirm', choices=(('Accepted', 'Подтвердить'), ('Rejected', 'Отклонить')))


class ChequeForm(FsrarForm):
    kassa = StringField('kassa', validators=[DataRequired(), Length(min=1, max=20, message='от 1 до 20 символов')])
    inn = StringField('inn', validators=[DataRequired(), Length(min=10, max=10, message='10 цифр')])
    kpp = StringField('kpp', validators=[DataRequired(), Length(min=9, max=9, message='9 цифр')])
    number = StringField('number', validators=[DataRequired(), Length(min=1, max=4, message='от 1 до 4 цифр')])
    shift = StringField('shift', validators=[DataRequired(), Length(min=1, max=4, message='от 1 до 4 цифр')])
    bottle = StringField('bottle',
                         validators=[DataRequired(), Length(min=68, max=150, message='68 или 150 символов')])
    price = StringField('price', validators=[
        DataRequired(),
        Regexp('[-]?\d+[.]\d+', message='Цена с минусом, разделитель точка, два десятичных знака'),
        Length(min=1, max=8, message='Слишком большое число')
    ])


def validate_filename(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_xml_template(filename: str) -> str:
    return os.path.join('xml/', filename)


def get_instance(fsrar: str, utms: List[Utm]) -> Utm:
    """ Получаем УТМ из формы по ФСРАР ИД"""
    return next((x for x in utms if x.fsrar == fsrar), None)


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
    result = Result(utm)
    homepage, gostpage = Grab(), Grab()

    try:
        homepage.go(utm.build_url())
        gostpage.go(utm.gost_url())
        # версия
        try:
            result.version = homepage.doc.select('//*[@id="home"]/div[1]/div[2]').text()
            result.changeset = homepage.doc.select('//*[@id="home"]/div[2]/div[2]').text()
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
        except:
            result.error.append('Проблема с отправкой чеков\n')

        try:
            pre = gostpage.doc.select('//pre').text()
            string_start = pre.find('CN') + 3
            end_comma = pre.find(',', string_start)
            end_sig = pre.find(' Signature', string_start)
            end = end_comma if end_comma < end_sig else end_sig
            result.legal = pre[string_start:end].replace('"', '').replace("\\", '')[0:20]
        except:
            result.error.append('Не найден сертификат организации\n')

    # не удалось соединиться
    except GrabTimeoutError:
        result.error.append('Таймаут')

    except GrabCouldNotResolveHostError:
        result.error.append('Нет DNS имени')

    except GrabConnectionError:
        result.error.append('Не удалось подключиться')

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


def parse_nattn(url: str):
    ttn_list, date_list, doc_list, nattn_list = [], [], [], []
    if url is not None:
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            flash('Ошибка получения списка ReplyNoAnswerTTN', url)
        try:
            tree = ET.fromstring(response.text)
            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}WbRegID'):
                ttn_list.append(elem.text)
            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnDate'):
                date_list.append(elem.text)
            for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnNumber'):
                doc_list.append(elem.text)
            for i, ttn in enumerate(ttn_list):
                nattn_list.append([ttn_list[i], date_list[i], doc_list[i]])
        except:
            flash('Ошибка обработки XML', url)
        return nattn_list
    return None


@app.route('/')
def index():
    return redirect(url_for('status'))


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = TTNForm()
    params = {
        'template_name_or_list': 'ttn.html',
        'title': 'Повторный запрос TTN',
        'form': form,
    }

    if request.method == 'POST' and form.validate_on_submit():
        file = 'ttn.xml'
        xml = get_xml_template(file)

        wbregid = request.form['wbregid'].strip()
        utm = get_instance(request.form['fsrar'], utmlist)
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
    params = {
        'template_name_or_list': 'reject.html',
        'title': 'Отозвать или отклонить TTN',
        'form': form,
    }

    if request.method == 'POST' and form.validate_on_submit():
        file = 'reject.xml'
        filepath = get_xml_template(file)

        wbregid = request.form['wbregid'].strip()
        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        url = utm.url() + '/opt/in/WayBillAct_v3'

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0][2].text = str(date.today())
        root[1][0][0][3].text = wbregid
        tree.write(filepath)
        files = {'xml_file': (file, open(filepath, 'rb'), 'application/xml')}
        err = send_xml(url, files)
        log = f'WayBillAct_v3: {wbregid} отправлен отзыв/отказ от {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        logging.info(log)
        flash(log)

    return render_template(**params)


@app.route('/wbrepeal', methods=['GET', 'POST'])
def wbrepeal():
    form = TTNForm()
    params = {
        'template_name_or_list': 'wbrepeal.html',
        'title': 'Распроведение TTN',
        'form': form,
    }
    if request.method == 'POST' and form.validate_on_submit():
        file = 'wbrepeal.xml'
        filepath = get_xml_template(file)
        wbregid = request.form['wbregid'].strip()
        request_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        url = utm.url() + '/opt/in/RequestRepealWB'

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0].text = utm.fsrar
        root[1][0][2].text = request_date
        root[1][0][3].text = wbregid
        tree.write(filepath)
        files = {'xml_file': (file, open(filepath, 'rb'), 'application/xml')}

        err = send_xml(url, files)
        log = f'RequestRepealWB: {wbregid} отправлен запрос на распроведение {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/requestrepeal', methods=['GET', 'POST'])
def requestrepeal():
    form = RequestRepealForm()
    params = {
        'template_name_or_list': 'requestrepeal.html',
        'title': 'Запрос распроведения',
        'form': form,
    }
    if request.method == 'POST' and form.validate_on_submit():
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
        utm = get_instance(request.form['fsrar'], utmlist)
        url = utm.url() + repeal_data['url']
        form.fsrar.data = utm.fsrar
        form.r_type.data = repeal_type

        tree = ET.parse(filepath)
        root = tree.getroot()
        root[0][0].text = utm.fsrar
        root[1][0][0].text = utm.fsrar
        root[1][0][2].text = request_date
        root[1][0][3].text = wbregid
        tree.write(filepath)
        files = {'xml_file': (repeal_data['file'], open(filepath, 'rb'), 'application/xml')}
        err = send_xml(url, files)
        log = f'RequestRepeal{repeal_type}: {wbregid} отправлен запрос на распроведение {repeal_type} {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/wbrepealconfirm', methods=['GET', 'POST'])
def wbrepealconfirm():
    form = WBRepealConfirmForm()
    params = {
        'template_name_or_list': 'wbrepealconfirm.html',
        'title': 'Подтверждение распроведения TTN',
        'form': form,
    }

    if request.method == 'POST' and form.validate_on_submit():
        file = 'wbrepealconfirm.xml'
        filepath = get_xml_template(file)
        wbregid = request.form['wbregid'].strip()
        is_confirm = request.form['is_confirm']
        utm = get_instance(request.form['fsrar'], utmlist)
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

        tree.write(filepath)

        files = {'xml_file': (file, open(filepath, 'rb'), 'application/xml')}

        err = send_xml(url, files)
        log = f'ConfirmRepealWB: {wbregid} подтверждения распроведения {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'
        flash(log)
        logging.info(log)

    return render_template(**params)


@app.route('/get_nattn', methods=['GET', 'POST'])
def get_nattn():
    form = FsrarForm()
    params = {
        'template_name_or_list': 'get_nattn.html',
        'title': 'Запросить необработанные TTN',
        'form': form,
    }

    if request.method == 'POST':
        file = 'nattn.xml'
        xml = get_xml_template(file)

        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        url = utm.url() + '/opt/in/QueryNATTN'

        query = create_unique_xml(utm.fsrar, utm.fsrar, xml)
        files = {'xml_file': (file, open(query, 'rb'), 'application/xml')}
        err = send_xml(url, files)

        log = f'QueryNATTN: Отправлен запрос {utm.title} [{utm.fsrar}]: {err if err is not None else "OK"}'

        logging.info(log)
        flash(Markup(log))

    return render_template(**params)


@app.route('/check_nattn', methods=['GET', 'POST'])
def check_nattn():
    form = FsrarForm()
    params = {
        'template_name_or_list': 'check_nattn.html',
        'title': 'Проверить необработанные TTN',
        'form': form,
    }

    if request.method == 'POST':
        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        ttnlist = parse_nattn(find_last_nattn(utm.url()))
        if ttnlist is None:
            flash('Нет запроса необработанных документов')
        elif not ttnlist:
            flash('Все документы обработаны')
        else:
            flash('Необработанные документы в списке результатов')

        params['tt'] = utm.title
        params['doc_list'] = ttnlist

    return render_template(**params)


@app.route('/service_clean', methods=['GET', 'POST'])
def service_clean():
    def clean(utm: Utm):
        # todo: внести clean_documents
        try:
            return utm.title, clean_documents(utm.url())
        except:
            return utm.title, 'недоступен'

    form = FsrarForm()
    params = {
        'template_name_or_list': 'service.html',
        'title': 'Удаление Форм 2 из УТМ',
        'form': form
    }

    if request.method == 'POST':
        results = []
        if 'select' in request.form:
            utm = get_instance(request.form['fsrar'], utmlist)
            results.append(clean(utm))
            form.fsrar.data = utm.fsrar

        elif 'all' in request.form:
            for utm in utmlist:
                results.append(clean(utm))

        params['results'] = results

    return render_template(**params)


@app.route('/status', methods=['GET', 'POST'])
def status():
    params = {
        'template_name_or_list': 'status.html',
        'title': 'Статус',
    }
    err = False

    if request.method == 'POST':
        results = [parse_utm(utm) for utm in utmlist]

        if 'gost' in request.form:
            results.sort(key=lambda result: result.gost)
        elif 'fsrar' in request.form:
            results.sort(key=lambda result: result.fsrar)
        elif 'filter' in request.form:
            results.sort(key=lambda result: result.filter)

        params['err'] = any([res.error != '' for res in results])
        params['results'] = results

    return render_template(**params)


@app.route('/cheque', methods=['GET', 'POST'])
def cheque():
    form = ChequeForm()
    params = {
        'template_name_or_list': 'cheque.html',
        'title': 'Отправка чека',
        'form': form,
    }
    if request.method == 'POST' and form.validate_on_submit():
        utm = get_instance(request.form['fsrar'], utmlist)

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

    params = {
        'template_name_or_list': 'mark.html',
        'title': 'Проверка марок УТМ',
        'form': form,
    }
    if request.method == 'POST' and form.validate_on_submit():
        res = None
        file = 'queryfilter.xml'
        xml = get_xml_template(file)
        url_suffix = '/opt/in/QueryFilter'
        mark = request.form['mark'].strip()
        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        query = create_unique_mark_xml(utm.fsrar, mark, file)
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
    params = {
        'template_name_or_list': 'utm_log.html',
        'title': 'УТМ поиск ошибок чеков',
        'form': form,
    }

    if request.method == 'POST':
        utm = get_instance(request.form['fsrar'], utmlist)
        form.fsrar.data = utm.fsrar

        transport_log = f'//{utm.host}.{DOMAIN}/{UTM_LOG}'

        summary = f'{utm.title} [{utm.fsrar}]'
        data = []
        results = []
        total = 0
        try:
            with open(transport_log, encoding="utf8") as file:
                data = file.readlines()
        except FileNotFoundError:
            summary = f'{summary}: недоступен или журнал не найден'

        if data:
            re_error = re.compile('<error>(.*)</error>')
            for line in data:
                if 'Получен чек.' in line:
                    total += 1
                else:
                    result = re_error.search(line)
                    if result:
                        results.append(result.groups()[0])

            summary = f'{summary}: Всего чеков сегодня: {total}, из них с ошибками {len(results)}'

        params['summary'] = summary
        params['results'] = results

    return render_template(**params)


@app.route('/rests', methods=['GET', 'POST'])
def get_rests():
    form = RestsForm()
    params = {
        'template_name_or_list': 'rests.html',
        'title': 'Поиск остатков в обмене',
        'form': form,
    }

    if request.method == 'POST':
        results = dict()
        exclude = ['Error']

        search_alccode = request.form['alccode'].strip()
        limit = get_limit(request.form['limit'], 50, 10)
        utm = get_instance(request.form['fsrar'], utmlist)
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
                        alccode = position.get('rst:Product').get('pref:AlcCode')
                        quantity = position.get('rst:Quantity')
                        if search_alccode in ('', alccode):
                            if results.get(alccode, False):
                                results[alccode][rest_date] = quantity
                            else:
                                results[alccode] = {rest_date: quantity}

        params['results'] = results

    return render_template(**params)


@app.route('/ticket', methods=['GET', 'POST'])
def get_tickets():
    form = TicketForm()
    params = {
        'template_name_or_list': 'ticket.html',
        'title': 'Поиск квитанций обмена',
        'form': form,
    }

    if request.method == 'POST':

        results = list()
        doc = request.form['search'].strip()
        limit = request.form['limit'].strip()
        limit = int(limit) if limit.isdigit() and int(limit) < 5000 else 1000
        utm = get_instance(request.form['fsrar'], utmlist)
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

            export_date = datetime.strftime(datetime.now() - timedelta(1), CONVERTER_DATE_FORMAT)

            def insert_or_append(d: dict, k: str, v: str):
                if d.get(k):
                    d[k].append(v)
                else:
                    d[k] = [v, ]

            def collect_import_data(filename: str) -> dict:
                results = dict()
                try:
                    wb = openpyxl.load_workbook(filename)
                    ws = wb.get_active_sheet()

                    # validate worksheet header
                    if not (ws.cell(1, 2).value == 'SKU' and ws.cell(1, 5).value == 'Код склада'):
                        errors.append('Не найдены заголовки таблицы')
                    else:
                        ws.delete_rows(1)
                        for r in ws.rows:
                            article = r[1].value
                            warehouse = r[4].value
                            insert_or_append(results, warehouse, article)

                    wb.close()
                except openpyxl.utils.exceptions.InvalidFileException:
                    errors.append('Не удалось прочитать файл импорта')

                return results

            def collect_export_results(imp: dict) -> dict:
                results = dict()
                if imp:
                    for warehouse in imp.keys():
                        exp_filename = f'skubody_{warehouse}_{export_date}.csv'
                        exp_filepath = os.path.join(CONVERTER_EXPORT_PATH, exp_filename)
                        if os.path.isfile(exp_filepath):
                            with open(exp_filepath, 'r', encoding='utf-8') as f:
                                csv_data = csv.reader(f, delimiter='¦')
                                for row in csv_data:
                                    article = row[0]
                                    insert_or_append(results, warehouse, article)
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
                            results[warehouse] = set(list_imp).difference(set(list_exp))
                        else:
                            errors.append(f'Место хранения {warehouse} пропущено')

                return results

            def write_down(fin: dict) -> str:
                result = ''
                if fin:
                    current_row = 7  # first row after header
                    wb = openpyxl.load_workbook(CONVERTER_TEMPLATE_FILE)
                    sh = wb.get_active_sheet()
                    today = datetime.now().strftime(CONVERTER_DATE_FORMAT)

                    result = f'autosupply_results_{today}_{uuid.uuid4()}.xlsx'
                    result_path = os.path.join(os.path.join(app.config['RESULT_FOLDER'], result))

                    for wh, articles in fin.items():
                        idx = 0
                        for idx, article in enumerate(articles):
                            sh.cell(current_row + idx, 1).value = article
                            sh.cell(current_row + idx, 2).value = wh
                        current_row = current_row + idx

                    wb.save(result_path)
                return result

            import_results = collect_import_data(filepath)
            export_results = collect_export_results(import_results)
            finale_results = make_difference(import_results, export_results)
            result_filename = write_down(finale_results)
            flash(
                f'Места хранения:\n'
                f'Импорта: {", ".join(import_results.keys())},\n'
                f'Экспорта: {", ".join(export_results.keys())},\n'
                f'Результат: {", ".join(finale_results.keys())}'
            )
            if errors:
                flash('\n'.join(errors))

            if result_filename:
                return redirect(url_for('uploaded_file', filename=result_filename))

    return render_template(**params)


@app.route('/results/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['RESULT_FOLDER'], filename)
