# -*- coding: utf-8 -*-
import requests
import os
import re
import logging
import xml.etree.ElementTree as ET
from grab import Grab, GrabTimeoutError
from flask import Flask, request, redirect, render_template, flash, Markup
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired
from datetime import date

app = Flask(__name__)

app.config.update(dict(
    SECRET_KEY='key',
    FLASK_DEBUG=True,
    port=80))

utmlist = (
    ('1', '020000314999', 'http://cash-bk.severotorg.local:8080', 'Большой камень - Аллея Труда'),
    ('2', '030000326777', 'http://bk63-srv01.severotorg.local:8080', 'Большой камень - Крылова'),
    ('3', '030000327460', 'http://vl62-srv01.severotorg.local:8080', 'Владивосток - Баляева'),
    ('4', '020000623568', 'http://vl23-srv03.severotorg.local:8080', 'Владивосток - Борисенко'),
    ('5', '020000754943', 'http://dbase-vlk.severotorg.local:8080', 'Владивосток - Вилкова'),
    ('6', '020000623562', 'http://cash-vl.severotorg.local:8080', 'Владивосток - Волгоградская'),
    ('7', '020000623569', 'http://vl26-srv03.severotorg.local:8080', 'Владивосток - Героев хасана'),
    ('8', '020000271396', 'http://vl44-srv03.severotorg.local:8080', 'Владивосток - Деревенская'),
    ('9', '020000623565', 'http://vl27-srv03.severotorg.local:8080', 'Владивосток - Ильичева'),
    ('10', '030000323196', 'http://vl48-srv03.severotorg.local:8080', 'Владивосток - Калинина'),
    ('11', '020000674029', 'http://vl21-srv03.severotorg.local:8080', 'Владивосток - Кр. знамени'),
    ('12', '020000623570', 'http://vl29-srv03.severotorg.local:8080', 'Владивосток - Русская'),
    ('13', '030000315698', 'http://vl49-srv03.severotorg.local:8080', 'Владивосток - Ульяновская'),
    ('14', '020000623563', 'http://dbase-fs.severotorg.local:8080', 'Владивосток - Фирсова'),
    ('15', '020000623571', 'http://vl31-srv03.severotorg.local:8080', 'Владивосток - Хабаровская'),
    ('16', '030000299907', 'http://vl47-srv03.severotorg.local:8080', 'Владивосток - Шилкинская'),
    ('17', '020000623564', 'http://vl28-srv03.severotorg.local:8080', 'Владивосток - Шуйская'),
    ('18', '020000623566', 'http://vl33-srv03.severotorg.local:8080', 'Владивосток - Юмашева'),
    ('19', '030000157438', 'http://dbase-vr.severotorg.local:8080', 'Врангель - Восточный'),
    ('20', '030000288947', 'http://ls46-srv02.severotorg.local:8080', 'Лесозаводск - Пушкинская'),
    ('21', '030000157441', 'http://dbase-np.severotorg.local:8080', 'Находка - Нах проспект'),
    ('22', '030000157440', 'http://cash-nhm.severotorg.local:8080', 'Находка - Нахимовская'),
    ('23', '030000157439', 'http://dbase-nh.severotorg.local:8080', 'Находка - Рыбацкая'),
    ('24', '030000255411', 'http://pr42-srv02.severotorg.local:8080', 'Партизанск - Ленинская'),
    ('25', '030000326776', 'http://sp57-srv02.severotorg.local:8080', 'Спасск - Cпасск Э'),
    ('26', '030000353814', 'http://sp65-srv01.severotorg.local:8080', 'Спасск - Новый спасск'),
    ('27', '030000326774', 'http://us58-srv01.severotorg.local:8080', 'Уссурийск - Ленинградская'),
    ('28', '020000745413', 'http://cash-uss.severotorg.local:8080', 'Уссурийск - Советская'),
    ('29', '020000745414', 'http://dbase-usch.severotorg.local:8080', 'Уссурийск - Чичерина'),
    ('30', '030000330565', 'http://hb53-srv01.severotorg.local:8080', 'Хабаровск - Восточное шоссе'),
    ('31', '030000330606', 'http://hb51-srv01.severotorg.local:8080', 'Хабаровск - Карла Маркса'),
    ('32', '030000326786', 'http://hb52-srv01.severotorg.local:8080', 'Хабаровск - Краснореченская'),
    ('33', '030000337340', 'http://hb54-srv01.severotorg.local:8080', 'Хабаровск - Суворова'),
    ('34', '030000326785', 'http://hb60-srv01.severotorg.local:8080', 'Хабаровск - Шелеста'),
    ('35', '030000326784', 'http://hb61-srv01.severotorg.local:8080', 'Хабаровск - Шкотова'),
    ('36', '030000340126', 'http://ch64-srv01.severotorg.local:8080', 'Черниговка - Октябрьская'),
    ('37', '030000295973', 'http://ks59-srv01.severotorg.local:8080', 'Комсомольск'),
)

r_type = ('ReplyNATTN', 'TTNHISTORYF2REG')
xml_path = 'utmr/xml/'
logging.basicConfig(filename='log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


class FSForm(FlaskForm):
    ttn = StringField('ttn', validators=[DataRequired()])


def last_date(date_string: str):
    return re.findall('\d{4}-\d{2}-\d{2}', date_string)[-1]


def parse_utm(utm_url: str):
    from datetime import datetime
    fsrar, pki_date, gost_date, status_string, license_string, cheque_date = '', '', '', '', '', ''
    try:
        g_utm = Grab(connect_timeout=100)
        g_utm.go(utm_url)
        gost_string = g_utm.doc.select('//*[@id="home"]/pre[7]').text()
        pki_string = g_utm.doc.select('//*[@id="home"]/pre[6]').text()
        status_string = g_utm.doc.select('//*[@id="home"]/pre[2]/img/@alt').text()
        license_string = g_utm.doc.select('//*[@id="home"]/pre[3]/img/@alt').text()
        cheque_string = g_utm.doc.select('//*[@id="home"]/pre[5]').text()
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
    except GrabTimeoutError:
        pass
    return fsrar, pki_date, gost_date, status_string, license_string, cheque_date


def match_id(select_list: tuple) -> tuple:
    search = request.form[select_list]
    for element in utmlist:
        if element[0] == search:
            return element[1], element[2], element[3]


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
            # flash(Markup(log))
            # e = r.text
            e = 'Отправлено'
        for error in ET.fromstring(r.text).iter('error'):
            # flash(error.text)
            e = error.text
    except requests.ConnectionError:
        # flash('УТМ недоступен')
        e = 'УТМ недоступен'
    return e


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
    response = requests.get(url_out)
    tree = ET.fromstring(response.text)
    for nattn_url in reversed(tree.findall('url')):
        if 'ReplyNATTN' in nattn_url.text:
            return nattn_url.text


def parse_nattn(url: str):
    ttn_list, date_list, doc_list, nattn_list = [], [], [], []
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        flash(url, e)
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
    except ET.ParseError as e:
        flash(url, e)
    return nattn_list


@app.route('/ttn', methods=['GET', 'POST'])
def ttn():
    form = FSForm()
    file = 'ttn.xml'
    if request.method == 'POST' and form.validate_on_submit():
        ttn = request.form['ttn']
        fsrar, link, name = match_id('utmlist')
        make_xml(fsrar, ttn, file)
        url = str(link) + '/opt/in/QueryResendDoc'
        log = str(ttn) + ' отправлена ' + str(name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/ttn')
    return render_template('ttn.html',
                           title='Повторный запрос TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/get_nattn', methods=['GET', 'POST'])
def get_nattn():
    form = FSForm()
    file = 'nattn.xml'
    if request.method == 'POST':
        fsrar, link, name = match_id('utmlist')
        make_xml(fsrar, fsrar, file)
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        url = str(link) + '/opt/in/QueryNATTN'
        log = 'Отправлен запрос ' + str(name) + ' [' + fsrar + ']    <a href="' + str(
            link) + '#menu5">Перейти на УТМ для проверки</a>'
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/get_nattn')
    return render_template('get_nattn.html',
                           title='Запросить необработанные TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/reject', methods=['GET', 'POST'])
def reject():
    form = FSForm()
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
        log = str(ttn) + ' отправлен отзыв / отказ от ' + today + ' ' + str(name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
        send_xml(url, files, log)
        logging.info(log)
        return redirect('/reject')
    return render_template('reject.html',
                           title='Отозвать или отклонить TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/check_nattn', methods=['GET', 'POST'])
def check_nattn():
    form = FSForm()
    if request.method == 'POST':
        fsrar, link, name = match_id(utmlist)
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
            megalist.append(utmlist[i] + parse_utm(site[2]))
        return render_template('status.html',
                               megalist=megalist,
                               title='Статус УТМ',
                               )
    return render_template('status.html',
                           title='Статус УТМ',
                           )

    # @app.route('/cheque', methods=['GET', 'POST'])
    # def cheque():
    #     form = FSForm()
    #     file = 'cheque.xml'
    #     if request.method == 'POST' and form.validate_on_submit():
    #         ttn = request.form['ttn']
    #         fsrar, link, name = match_id('utmlist')
    #         make_xml(fsrar, ttn, file)
    #         url = str(link) + '/opt/in/QueryResendDoc'
    #         log = str(ttn) + ' отправлена ' + str(name) + ' [' + fsrar + ']'
    #         files = {'xml_file': (file, open(os.path.join(xml_path, file), 'rb'), 'application/xml')}
    #         send_xml(url, files, log)
    #         logging.info(log)
    #         return redirect('/ttn')
    #     return render_template('ttn.html',
    #                            title='Отправка чека',
    #                            form=form,
    #                            server_list=utmlist)
