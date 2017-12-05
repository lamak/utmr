# -*- coding: utf-8 -*-
import io
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime

import requests
from flask import Flask, request, redirect, render_template, flash, Markup
from flask_wtf import FlaskForm
from grab import Grab, GrabError
from wtforms import StringField, validators
from wtforms.validators import DataRequired

from utms import utmlist

app = Flask(__name__)

app.config.update(dict(
    SECRET_KEY='key',
    FLASK_DEBUG=True,
    port=80))

# UTM list moved to external file utms.py,
# utmlist = (
#     ('id', 'fsrar', 'utm_link', 'name', 'inn', 'kpp', 'kass', 'address', 'exch-full-path'),
# )

r_type = ('ReplyNATTN', 'TTNHISTORYF2REG')
xml_path = 'utmr/xml/'
logging.basicConfig(filename='log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


class TTNForm(FlaskForm):
    ttn = StringField('ttn', validators=[DataRequired()])


class SearchForm(FlaskForm):
    search = StringField('search', validators=[DataRequired()])


class ChequeForm(FlaskForm):
    # not using since data in utmlist, so it would be legacy
    # name = StringField('name')
    # inn = StringField('inn')
    # kpp = StringField('kpp')
    # kassa = StringField('kassa')
    # address = StringField('address')
    # #next field is using
    number = StringField('number',
                         validators=[DataRequired(), validators.Length(min=1, max=4, message='от 1 до 4 символов')])
    shift = StringField('shift',
                        validators=[DataRequired(), validators.Length(min=1, max=4, message='от 1 до 4 символов')])
    bottle = StringField('bottle', [validators.DataRequired(), validators.Length(min=68, max=68,
                                                                                 message='Акцизная марка должна быть 68 символов')])
    price = StringField('price', validators=[DataRequired(), validators.Regexp('[-]\d+[.]\d+',
                                                                               message='Цена с минусом, разделитель точка, два десятичных знака'),
                                             validators.Length(min=1, max=8, message='Слишком большое число')])


def last_date(date_string: str):
    return re.findall('\d{4}-\d{2}-\d{2}', date_string)[-1]


def parse_utm(utm_url: str):
    from datetime import datetime
    fsrar, pki_date, gost_date, status_string, license_string, cheque_date = '', '', '', '', '', ''
    try:
        g_utm = Grab(connect_timeout=100)
        g_utm.go(utm_url)
        try:
            gost_string = g_utm.doc.select('//*[@id="home"]/pre[7]').text()
            pki_string = g_utm.doc.select('//*[@id="home"]/pre[6]').text()
            status_string = g_utm.doc.select('//*[@id="home"]/pre[2]/img/@alt').text()
            license_string = g_utm.doc.select('//*[@id="home"]/pre[3]/img/@alt').text()
            cheque_string = g_utm.doc.select('//*[@id="home"]/pre[5]').text()
        except:
            pass
        try:
            fsrar = pki_string.split(' ')[1].split('-')[2]
            pki_date = last_date(pki_string)
            gost_date = last_date(gost_string)
        except:
            pass
        try:
            cheque_date = last_date(cheque_string)
        except:
            cheque_date = 'OK'
        if cheque_date == datetime.strftime(datetime.now(), "%Y-%m-%d"):
            cheque_date = 'OK'
    except GrabError:
        flash('Ошибка подключения к УТМ ', utm_url)
    return fsrar, pki_date, gost_date, status_string, license_string, cheque_date


def match_id(select_list: tuple) -> tuple:
    search = request.form[select_list]
    for element in utmlist:
        if element[0] == search:
            return element[1], element[2], element[3]


def match_srv(select_list: tuple) -> str:
    search = request.form[select_list]
    for element in utmlist:
        if element[0] == search:
            return element[8]


def match_cheque(select_list: tuple) -> tuple:
    search = request.form[select_list]
    for element in utmlist:
        if element[0] == search:
            return element[1], element[2], element[3], element[4], element[5], element[6], element[7]


def make_xml(fsrar: str, content: str, filename: str):
    path = os.path.join(xml_path, filename)
    tree = ET.parse(path)
    root = tree.getroot()
    root[0][0].text = fsrar
    root[1][0][0][0][1].text = content
    tree.write(path)


def send_xml(url: str, files, log: str):
    try:
        r = requests.post(url, files=files)
        for sign in ET.fromstring(r.text).iter('sign'):
            flash(Markup(log))
            # e = r.text
            e = 'Отправлено'
        for error in ET.fromstring(r.text).iter('error'):
            flash(error.text)
            e = error.text
    except requests.ConnectionError:
        flash('УТМ недоступен')
        e = 'УТМ недоступен'
    return e


def send_cheque(url: str, files, log: str):
    try:
        r = requests.post(url, files=files)
        for url in ET.fromstring(r.text).iter('url'):
            log += url.text
            flash(Markup(log))
        for error in ET.fromstring(r.text).iter('error'):
            flash(error.text)
            log = error.text
    except requests.ConnectionError:
        flash('УТМ недоступен')
        log += 'УТМ недоступен'
    return log


def request_nattn(fsrar: str, url: str):
    file = 'nattn.xml'
    if request.method == 'POST':
        counter = 0
        make_xml(fsrar, fsrar, file)
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        url = str(url) + '/opt/in/QueryNATTN'
        try:
            e = send_xml(url, files, '')
        except:
            e = 'Не отправлен'
    return e


def del_out(url: str):
    counter = 0
    url_out = url + '/opt/out'
    response = requests.get(url_out)
    tree = ET.fromstring(response.text)
    for u in tree.findall('url'):
        if any(ext in u.text for ext in r_type):
            requests.delete(u.text)
            counter += 1
    return counter


def find_last_nattn(url: str) -> str:
    url_out = url + '/opt/out'
    try:
        response = requests.get(url_out)
        tree = ET.fromstring(response.text)
        for nattn_url in reversed(tree.findall('url')):
            if 'ReplyNATTN' in nattn_url.text:
                return nattn_url.text
    except requests.exceptions.ConnectionError:
        flash('Ошибка подключения к УТМ')


def search_ticket(query: str, log_dir: str):
    counter_hit = 0
    counter_str = 0
    megalist = []
    try:
        start_time = time.time()
        for d, dirs, files in os.walk(log_dir):
            for f in files:
                if f.find('Ticket') > 0:
                    path = os.path.join(d, f)
                    with io.open(path, encoding="utf8") as file:
                        counter_str += 1
                        for line in file:
                            if query in line:
                                counter_hit += 1
                                with open(path, encoding="utf8") as myfile:
                                    data = myfile.read().replace('\n', '<br> ')
                                megalist.append([f, data])

                                # tree = ET.parse(path)
                                # root = tree.getroot()
                                # for elem in tree.iter('{http://fsrar.ru/WEGAIS/Ticket}OperationComment'):
                                #     print(elem.text)

        flash('Найдено {count}, из {count_str} файлов, за {timer} секунд'
              .format(count=counter_hit, count_str=counter_str, timer=int(time.time() - start_time)))
    except OSError:
        flash('can\'t find such path or file')
    return (megalist)


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
    return ' '


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = TTNForm()
    file = 'ttn.xml'
    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        fsrar, link, name = match_id('utmlist')
        make_xml(fsrar, ttn, file)
        url = str(link) + '/opt/in/QueryResendDoc'
        log = 'QueryResendDoc: ' + str(ttn) + ' отправлена ' + str(name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/ttn')
    return render_template('ttn.html',
                           title='Повторный запрос TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/reject', methods=['GET', 'POST'])
def reject():
    form = TTNForm()
    file = 'reject.xml'
    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        today = str(date.today())
        fsrar, link, name = match_id('utmlist')
        tree = ET.parse(os.path.join(xml_path, file))
        root = tree.getroot()
        root[0][0].text = fsrar
        root[1][0][0][2].text = today
        root[1][0][0][3].text = ttn
        tree.write(os.path.join(xml_path, file))
        url = str(link) + '/opt/in/WayBillAct_v2'
        log = 'WayBillAct_v2: ' + str(ttn) + ' отправлен отзыв / отказ от ' + today + ' ' + str(
            name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/reject')
    return render_template('reject.html',
                           title='Отозвать или отклонить TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/wbrepeal', methods=['GET', 'POST'])
def wbrepeal():
    form = TTNForm()
    file = 'wbrepeal.xml'
    if request.method == 'POST' and form.validate_on_submit():
        WBRegId = request.form['ttn']
        requestdate = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        fsrar, link, name = match_id('utmlist')
        tree = ET.parse(os.path.join(xml_path, file))
        root = tree.getroot()
        root[0][0].text = fsrar
        root[1][0][0].text = fsrar
        root[1][0][2].text = requestdate
        root[1][0][3].text = WBRegId
        tree.write(os.path.join(xml_path, file))
        url = str(link) + '/opt/in/RequestRepealWB'
        log = 'RequestRepealWB: ' + str(WBRegId) + ' отправлен запрос на распроведение ' + requestdate + ' ' + str(
            name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/wbrepeal')
    return render_template('wbrepeal.html',
                           title='Распроведение TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/get_nattn', methods=['GET', 'POST'])
def get_nattn():
    form = TTNForm()
    file = 'nattn.xml'
    if request.method == 'POST':
        fsrar, link, name = match_id('utmlist')
        make_xml(fsrar, fsrar, file)
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        url = str(link) + '/opt/in/QueryNATTN'
        log = 'QueryNATTN: Отправлен запрос ' + str(name) + ' [' + fsrar + ']    <a href="' + str(
            link) + '#menu5">Перейти на УТМ для проверки</a>'
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/get_nattn')
    return render_template('get_nattn.html',
                           title='Запросить необработанные TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/check_nattn', methods=['GET', 'POST'])
def check_nattn():
    form = TTNForm()
    if request.method == 'POST':
        fsrar, link, name = match_id('utmlist')
        ttnlist = parse_nattn(find_last_nattn(link))
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


@app.route('/service', methods=['GET', 'POST'])
def service():
    if request.method == 'POST':
        megalist = []
        for site in utmlist:
            try:
                megalist.append((site[3], del_out(site[2]), request_nattn(site[1], site[2])))
            except:
                megalist.append((site[3], 'not available'))
        return render_template('service.html',
                               title='Очистка УТМ',
                               megalist=megalist,
                               )
    return render_template('service.html',
                           title='Очистка УТМ',
                           )


@app.route('/status', methods=['GET', 'POST'])
def status():
    megalist = []
    if request.method == 'POST':
        for i, site in enumerate(utmlist):
            megalist.append((site[0], site[1], site[2], site[3]) + parse_utm(site[2]))
        return render_template('status.html',
                               megalist=megalist,
                               title='Статус УТМ',
                               )
    return render_template('status.html',
                           title='Статус УТМ',
                           )


@app.route('/cheque', methods=['GET', 'POST'])
def cheque():
    form = ChequeForm()
    file = 'cheque.xml'
    if request.method == 'POST' and form.validate_on_submit():
        doctime = datetime.today()

        # get some info from utmlist
        fsrar, link, name, inn, kpp, kassa, address = match_cheque('utmlist')

        # parse form
        # inn = request.form['inn']
        # kpp = request.form['kpp']
        # kassa = request.form['kassa']
        # address = request.form['address']
        # name = request.form['name']
        number = request.form['number']
        shift = request.form['shift']
        bottle = request.form['bottle']
        price = request.form['price']

        # creating document with cheque header attributes
        document = ET.Element('Cheque')
        document.set('inn', inn)
        document.set('kpp', kpp)
        document.set('kassa', kassa)
        document.set('address', address)
        document.set('number', number)
        document.set('shift', shift)
        document.set('name', name)
        document.set('kpp', kpp)
        document.set('datetime', doctime.strftime("%d%m%y%H%M"))

        # inserting bottle subelement with attributes
        node = ET.SubElement(document, 'Bottle')
        node.set('barcode', bottle)
        node.set('price', price)

        # done, writing to xml file
        tree = ET.ElementTree(document)
        tree.write(os.path.join(xml_path, file), encoding='utf-8', xml_declaration=True)

        # send xml and write log
        url = str(link) + '/xml'
        log = 'Cheque: ' + shift + ', ' + number + ' чек, марка: ' + bottle + ', цена: ' + price + ', ТТ ' + str(
            name) + ' [' + fsrar + '] '
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        log = send_cheque(url, files, log)
        logging.info(log)
        return redirect('/cheque')
    return render_template('cheque.html',
                           title='Отправка чека',
                           form=form,
                           server_list=utmlist)


@app.route('/ticket_finder', methods=['GET', 'POST'])
def ticket_finder():
    form = SearchForm()
    if request.method == 'POST':
        word = request.form['ttn']
        dir = match_srv('utmlist')
        megalist = search_ticket(word, dir)
        return render_template('ticket.html',
                               megalist=megalist,
                               title='Поиск тикета',
                               form=form,
                               server_list=utmlist,
                               )
    return render_template('ticket.html',
                           title='Поиск тикета',
                           form=form,
                           server_list=utmlist,
                           )
