import re
from datetime import datetime, timedelta

from grab import Grab
from grab.error import GrabCouldNotResolveHostError, GrabConnectionError, GrabTimeoutError
from weblib.error import DataNotFound

from app import Result, Utm


def parse_utm(utm: Utm) -> Result:
    """ Парсер УТМ получает всю необходимую информацию с главной страницы и сертификата"""

    def last_date(date_string: str):
        return re.findall('\d{4}-\d{2}-\d{2}', date_string)[-1]

    result = Result(utm)
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
