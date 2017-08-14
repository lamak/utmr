# -*- coding: utf-8 -*-

import requests
import xml.etree.ElementTree as ET
from flask import Flask, request, redirect, render_template, flash
from flask_wtf import Form
from wtforms import StringField
from wtforms.validators import DataRequired
from datetime import date

app = Flask(__name__)

app.config.update(dict(
    SECRET_KEY='key',
    FLASK_DEBUG=True
))


utmlist = (
    ('101', '000000000000', 'http://cash-bk.severotorg.local:8080', 'FSRAR error'),
    ('102', '000000000000', 'http://000.severotorg.local', 'Host error'),
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
    ('12', '020000623570', 'http://dbase-rs.severotorg.local:8080', 'Владивосток - Русская'),
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
    ('24', '030000255411', 'http://pr42-srv01.severotorg.local:8080', 'Партизанск - Ленинская'),
    ('25', '030000326776', 'http://sp57-srv01.severotorg.local:8080', 'Спасск - Cпасск Э'),
    ('26', '020000745415', 'http://dbase-sp.severotorg.local:8080', 'Спасск - Спасск'),
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
    ('37', '030000295973', 'http://ks59-srv01.severotorg.local:8080', 'Комсомольск1'),
)


class FSForm(Form):
    ttn = StringField('ttn', validators=[DataRequired()])


def match_id(select_list: tuple) -> tuple:
    search = request.form[select_list]
    for element in utmlist:
        if element[0] == search:
            return element[1], element[2], element[3]


def make_xml(fsrar: str, content: str, filename: str):
    tree = ET.parse(filename)
    root = tree.getroot()
    root[0][0].text = fsrar
    root[1][0][0][0][1].text = content
    tree.write(filename)


def send_xml(url: str, files: str, log: str):
    try:
        r = requests.post(url, files=files)
        for sign in ET.fromstring(r.text).iter('sign'):
            flash(log)
        for error in ET.fromstring(r.text).iter('error'):
            flash(error.text)
    except requests.ConnectionError:
        flash('УТМ недоступен')


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
        files = {'xml_file': (file, open(file, 'rb'), 'application/xml')}
        send_xml(url, files, log)
        return redirect('/ttn')
    return render_template('ttn.html',
                           title='Send TTN',
                           form=form,
                           server_list=utmlist)


@app.route('/nattn', methods=['GET', 'POST'])
def nattn():
    form = FSForm()
    file = 'nattn.xml'
    if request.method == 'POST':
        fsrar, link, name = match_id('utmlist')
        make_xml(fsrar, fsrar, file)
        files = {'xml_file': (file, open(file, 'rb'), 'application/xml')}
        url = str(link) + '/opt/in/QueryNATTN'
        log = 'Отправлен запрос ' + str(name) + ' [' + fsrar + ']'
        send_xml(url, files, log)
        return redirect('/nattn')
    return render_template('nattn.html',
                           title='Request NATTN',
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
        # make_xml(fsrar, ttn, file)
        tree = ET.parse(file)
        root = tree.getroot()
        root[0][0].text = fsrar
        root[1][0][0][2].text = today
        root[1][0][0][3].text = ttn
        tree.write(file)
        flash(str(today))
        url = str(link) + '/opt/in/WayBillAct_v2'
        log = str(ttn) + ' отправлен отзыв / отказ от ' + today + ' ' + str(name) + ' [' + fsrar + ']'
        files = {'xml_file': (file, open(file, 'rb'), 'application/xml')}
        send_xml(url, files, log)
        return redirect('/reject')
    return render_template('reject.html',
                           title='Reject TTN',
                           form=form,
                           server_list=utmlist)
