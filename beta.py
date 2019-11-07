import platform
import subprocess

from requests_html import HTMLSession

from utms import utmlist

import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime

import requests
from flask import Flask, request, redirect, render_template, flash, Markup, url_for
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired, Length, Regexp

app = Flask(__name__)

app.config.update(dict(
    SECRET_KEY='65c6b192-0cef-49b2-aebf-42d6303869dc',
    FLASK_DEBUG=True,
    port=80))


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()


class Utm:
    """ УТМ
    Включает в себя название, адрес сервера, заголовок-адрес, путь к XML обмену Супермага
    """

    def __init__(self, fsrar, host, title, path):
        self.fsrar = fsrar
        self.title = title
        self.path = path
        self.host = f'{host}.severotorg.local'
        self.url = f'http://{self.host}:8080'

    def __str__(self):
        return f'{self.fsrar} {self.title}'

    def url(self):
        return f'http://{self.host}:8080'

    def version_url(self):
        return f'{self.url}/info/version'

    def gost_url(self):
        return f'{self.url}/info/certificate/GOST'


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
        self.url: str = self.utm.url
        self.title: str = self.utm.title


class TTNForm(FlaskForm):
    ttn = StringField('ttn', validators=[DataRequired()])


class SearchForm(FlaskForm):
    search = StringField('search', validators=[DataRequired()])


class ChequeForm(FlaskForm):
    inn = StringField(label='inn', validators=[DataRequired(), Length(min=10, max=10, message='10 цифр')])
    kpp = StringField(label='kpp', validators=[DataRequired(), Length(min=9, max=9, message='9 цифр')])

    kassa = StringField(
        label='kassa',
        validators=[DataRequired(), Length(min=1, max=20, message='от 1 до 20 символов')]
    )

    number = StringField(label='number', validators=[DataRequired(), Length(min=1, max=4, message='от 1 до 4 цифр')])
    shift = StringField(label='shift', validators=[DataRequired(), Length(min=1, max=4, message='от 1 до 4 цифр')])

    bottle = StringField(
        label='bottle',
        validators=[DataRequired(), Length(min=68, max=150, message='Акцизная марка должна быть от 68 до 150 символов')]
    )
    price = StringField(
        label='price',
        validators=[
            DataRequired(),
            Length(min=1, max=8, message='Слишком большое число'),
            Regexp('[-]?\d+[.]\d+', message='Цена с минусом, разделитель точка, два десятичных знака'),
        ]
    )


class TTNDocument:

    def __init__(self, utm: Utm, ttn):
        self.fsrar = utm.fsrar
        self.ttn = ttn
        self.xml = 'xml/'
        self.url = utm.url
        self.date = datetime.now().strftime("%Y-%m-%d")


class TTNResend(TTNDocument):

    def __init__(self, utm, ttn):
        super().__init__(utm, ttn)
        self.file = 'ttn.xml'
        self.url = f'{self.url}/opt/in/QueryResendDoc'
        self.path = os.path.join(self.xml, self.file)

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}

    def make_file(self):
        tree = ET.parse(self.path)
        root = tree.getroot()
        root[0][0].text = self.fsrar
        root[1][0][0][0][1].text = self.ttn
        tree.write(self.path)


class GetNattn(TTNDocument):

    def __init__(self, utm):
        super().__init__(utm)
        self.file = 'nattn.xml'
        self.url = f'{self.url}/opt/in/QueryNATTN'
        self.path = os.path.join(self.xml, self.file)

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}

    def make_file(self):
        tree = ET.parse(self.path)
        root = tree.getroot()
        root[0][0].text = self.fsrar
        root[1][0][0][0][1].text = self.fsrar
        tree.write(self.path)


class WBRepealConfirm(TTNDocument):

    def __init__(self, utm, ttn):
        super().__init__(utm, ttn)
        self.file = 'wbrepealconfirm.xml'
        self.url = f'{self.url}/opt/in/ConfirmRepealWB'
        self.path = os.path.join(self.xml, self.file)

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}

    def make_file(self):
        isconfirm = 'Accepted'

        tree = ET.parse(self.path)
        root = tree.getroot()
        root[0][0].text = self.fsrar
        root[1][0][0][0].text = isconfirm
        root[1][0][0][2].text = self.date
        root[1][0][0][3].text = self.ttn
        root[1][0][0][4].text = isconfirm
        tree.write(self.path)


class WBRepeal(TTNDocument):

    def __init__(self, utm, ttn):
        super().__init__(utm, ttn)
        self.file = 'wbrepeal.xml'
        self.url = f'{self.url}/opt/in/RequestRepealWB'
        self.path = os.path.join(self.xml, self.file)

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}

    def make_file(self):
        tree = ET.parse(self.path)
        root = tree.getroot()
        root[0][0].text = self.fsrar
        root[1][0][0].text = self.fsrar
        root[1][0][2].text = self.date
        root[1][0][3].text = self.ttn
        tree.write(self.path)


class Cheque:

    def __init__(self, utm: Utm, request):
        self.file = 'cheque.xml'
        self.xml = 'xml/'
        self.url = f'{utm.url}/xml'
        self.path = os.path.join(self.xml, self.file)

        self.number = request.form['number']
        self.bottle = request.form['bottle']
        self.shift = request.form['shift']
        self.price = request.form['price']
        self.kassa = request.form['kassa']
        self.inn = request.form['inn']
        self.kpp = request.form['kpp']
        self.name = utm.title
        self.fsrar = utm.fsrar

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}

    def make_file(self):
        today = datetime.today().strftime("%d%m%y%H%M")

        document = ET.Element('Cheque')
        document.set('datetime', today)
        document.set('number', self.number)
        document.set('address', self.name)
        document.set('kassa', self.kassa)
        document.set('shift', self.shift)
        document.set('name', self.name)
        document.set('kpp', self.kpp)
        document.set('inn', self.inn)
        document.set('kpp', self.kpp)

        # inserting bottle subelement with attributes
        node = ET.SubElement(document, 'Bottle')
        node.set('barcode', self.bottle)
        node.set('price', self.price)

        # done, writing to xml file
        tree = ET.ElementTree(document)
        tree.write(self.path, encoding='utf-8', xml_declaration=True)


class TTNReject(TTNDocument):

    def __init__(self, utm, ttn):
        super().__init__(utm, ttn)
        self.file = 'reject.xml'
        self.url = f'{self.url}/opt/in/WayBillAct_v3'
        self.path = os.path.join(self.xml, self.file)

    def make_file(self):
        today = str(date.today())

        tree = ET.parse(self.path)

        root = tree.getroot()
        root[0][0].text = self.fsrar
        root[1][0][0][2].text = today
        root[1][0][0][3].text = self.ttn

        tree.write(self.path)

    def gen_files_attachment(self):
        return {'xml_file': (self.file, open(self.path, 'rb'), 'application/xml')}


xml_path = 'xml/'
test_utms = [
    ('8', '020000271396', '192.168.10.9', 'Владивосток, ул Деревенская 14', '//vl20-srv15/d$/egais-exch/VL44/in/'),
]
utms_ = []

for i in test_utms:
    utms_.append(Utm(i[1], i[2], i[3], i[4]))


def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """
    # Option for the number of packets as a function of
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    # Building the command. Ex: "ping -c 1 google.com"
    command = ['ping', param, '1', host]
    return subprocess.call(command) == 0


def last_date(date_string: str):
    return re.findall('\d{4}-\d{2}-\d{2}', date_string)[-1]


def get_utm_from_form(utm_id: str):
    return utms_[int(request.form[utm_id]) - 1]


def parse_utm(utm: Utm):
    result = Result(utm)
    session = HTMLSession()

    try:
        index = session.get(utm.url)

        rsa_data = index.html.find('#RSA', first=True)
        home_data = index.html.find('#home', first=True)

        if utm.fsrar not in rsa_data.text:
            result.error.append('ФСРАР не соответствует')

        home = home_data.text.split('\n')
        pki_string = home[12]
        gost_string = home[14]
        cheque_string = home[10]

        today = datetime.strftime(datetime.now(), "%Y-%m-%d")

        # Даты окончания сертификатов
        result.pki = last_date(pki_string)
        result.gost = last_date(gost_string)

        # Проверка статус и лицензии
        if 'RSA сертификат pki.fsrar.ru соответствует контуру' in home:
            result.status = True

        if 'Лицензия на вид деятельности действует' in home:
            result.license = True

        # Проверка чеков, допускается сегодняшние чеки
        if 'Отсутствуют неотправленные чеки' in home:
            result.cheques = 'OK'
        elif last_date(cheque_string) == today:
            result.cheques = 'OK'
        else:
            result.cheques = last_date(cheque_string)

        # Название организации из сертификата
        gost = session.get(utm.gost_url())
        gost_data = gost.html.find('pre', first=True).text
        start = gost_data.find('CN') + 3

        end_comma = gost_data.find(',', start)
        end_signature = gost_data.find(' Signature', start)
        end = end_comma if end_comma < end_signature else end_signature

        result.gost = gost_data[start:end]

        # 0 Информация об установленном УТМ
        # 1 Версия ПО
        # 2 3.0.8
        # 3 Продуктивный контур
        # 4 RSA сертификат pki.fsrar.ru соответствует контуру
        # 5 Статус лицензии
        # 6 Лицензия на вид деятельности действует
        # 7 Дата создания БД
        # 8 2017-05-04 11:37:01.116
        # 9 Неотправленные чеки
        # 10 Отсутствуют неотправленные чеки
        # 11 Сертификат RSA
        # 12 Действителен с 2019-03-13 17:23:18 +1000 по 2020-03-13 17:33:18 +1000
        # 13 Сертификат ГОСТ
        # 14 Действителен с 2019-03-13 17:25:05 +1000 по 2020-03-13 17:27:19 +1000
        # 15 Настройки
        # 16 Необходимо обновить настройки

    except requests.exceptions.ConnectionError:
        err = 'Связь есть, проблема с УТМ' if ping(utm.server()) else 'Нет связи'
        result.error.append(err)

    result.error = ' '.join(result.error)
    return result


def send_xml(url: str, files: dict):
    try:
        session = HTMLSession()
        post = session.post(url, files=files)
        key = 'url' if post.status_code == 200 else 'error'
        res = flash(post.html.find(key, first=True).text)

    except requests.ConnectionError as err:
        res = err

    return res


@app.route('/status', methods=['GET', 'POST'])
def status():
    results = []
    if request.method == 'POST':
        for utm in utms_:
            results.append(parse_utm(utm))

        results.sort(key=lambda result: result.gost)

        return render_template('status.html', results=results, title='Статус УТМ')

    return render_template('status.html', title='Статус УТМ')


@app.route('/reject', methods=['GET', 'POST'])
def reject():
    form = TTNForm()

    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        utm = get_utm_from_form(request.form['utmlist'])

        doc = TTNReject(utm, ttn)
        doc.make_file()

        files = doc.gen_files_attachment()
        result = send_xml(doc.url, files)
        log = f'WayBillAct_v3 Reject: {ttn} {utm.title} [{utm.fsrar}] [{result}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('wbrepeal.html', title='Отзыв', server_list=utms_, form=form)


@app.route('/wbrepeal', methods=['GET', 'POST'])
def wbrepeal():
    form = TTNForm()

    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        utm = get_utm_from_form(request.form['utmlist'])

        doc = WBRepeal(utm, ttn)
        doc.make_file()

        files = doc.gen_files_attachment()
        result = send_xml(doc.url, files)
        log = f'ConfirmRepealWB: {ttn} {utm.title} [{utm.fsrar}] [{result}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('wbrepeal.html', title='Запрос распроведения', form=form, server_list=utms_)


@app.route('/wbrepealconfirm', methods=['GET', 'POST'])
def wbrepealconfirm():
    form = TTNForm()

    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        utm = get_utm_from_form(request.form['utmlist'])

        doc = WBRepealConfirm(utm, ttn)
        doc.make_file()

        url = utm.url + doc.url
        files = doc.gen_files_attachment()

        log = f'ConfirmRepealWB: {ttn} {utm.title} [{utm.fsrar}] [{send_xml(url, files)}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('wbrepealconfirm.html', title='Подтверждение распроведения', form=form, server_list=utms_)


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = TTNForm()
    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        utm = get_utm_from_form(request.form['utmlist'])

        doc = TTNResend(utm, ttn)
        doc.make_file()

        files = doc.gen_files_attachment()
        result = send_xml(doc.url, files)
        log = f'QueryResendDoc: {ttn} {utm.title} [{utm.fsrar}] [{result}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('ttn.html', title='Повторный запрос TTN', form=form, server_list=utms_)


@app.route('/cheque', methods=['GET', 'POST'])
def cheque():
    form = ChequeForm()
    if request.method == 'POST' and form.validate_on_submit():
        utm = get_utm_from_form(request.form['utmlist'])

        chk = Cheque(utm, request)
        chk.make_file()

        log = f'Cheque: {chk.shift} {chk.number} марка: {chk.bottle} цена: {chk.price} ' \
            f'ТТ {chk.name} [{chk.fsrar}] [{send_xml(chk.url, chk.gen_files_attachment())}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('cheque.html', title='Отправка чека', form=form, server_list=utms_)


@app.route('/nattn', methods=['GET', 'POST'])
def get_nattn():
    if request.method == 'POST':
        utm = get_utm_from_form(request.form['utmlist'])

        doc = GetNattn(utm)
        doc.make_file()

        files = doc.gen_files_attachment()
        result = send_xml(doc.url, files)

        log = f'GetNATTN: {ttn} {utm.title} [{utm.fsrar}] [{result}]'

        flash(Markup(log))
        logging.info(log)

    return render_template('get_nattn.html', title='Повторный запрос TTN', server_list=utms_)


def find_last_nattn(utm: Utm) -> str:
    session = HTMLSession
    url = f'{utm.url}/opt/out'
    documents = session.get(url).html.find('url')
    for doc in reversed(documents):
        if 'ReplyNATTN' in doc.text:
            return doc.text


def parse_nattn(url: str):
    session = HTMLSession
    ttn_list, date_list, doc_list, nattn_list = [], [], [], []

    tree = ET.fromstring(session.get(url).html.html)
    for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}WbRegID'):
        ttn_list.append(elem.text)
    for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnDate'):
        date_list.append(elem.text)
    for elem in tree.iter('{http://fsrar.ru/WEGAIS/ReplyNoAnswerTTN}ttnNumber'):
        doc_list.append(elem.text)
    for i, ttn in enumerate(ttn_list):
        nattn_list.append([ttn_list[i], date_list[i], doc_list[i]])

    return nattn_list


@app.route('/check_nattn', methods=['GET', 'POST'])
def check_nattn():
    form = TTNForm()
    if request.method == 'POST':
        utm = get_utm_from_form(request.form['utmlist'])

        ttnlist = parse_nattn(find_last_nattn(utm.url))
        if ttnlist == ' ':
            flash('Нет запроса необработанных документов')
        elif ttnlist == []:
            flash('Все документы обработаны')
        else:
            flash('Необработанные документы в списке результатов')
        return render_template('check_nattn.html',
                               title='Проверить необработанные TTN',
                               form=form,
                               server_list=utmlist,
                               doc_list=ttnlist,
                               tt=name)
    return render_template('check_nattn.html',
                           title='Проверить необработанные TTN',
                           form=form,
                           server_list=utmlist, )
