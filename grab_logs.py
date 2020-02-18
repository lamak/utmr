import logging
import os
import re
import smtplib
import zipfile
from datetime import datetime, timedelta
from email.header import Header
from email.mime.text import MIMEText
from typing import List, Optional

from pymongo import MongoClient

# UTM LOG
UTM_LOG_PATH = os.environ.get('UTM_LOG_PATH', 'c$/utm/transporter/l/')
UTM_CONFIG = os.environ.get('UTM_LOG_PATH', 'config')
DOMAIN = os.environ.get('USERDNSDOMAIN')

# EMAIL
MAIL_USER = os.environ.get('MAIL_USER', '')
MAIL_PASS = os.environ.get('MAIL_PASS', '')
MAIL_HOST = os.environ.get('MAIL_HOST', '')
MAIL_FROM = os.environ.get('MAIL_FROM', '')
MAIL_TO = os.environ.get('MAIL_TO', '')

# Mongo
mongo_conn = os.environ.get('MONGODB_CONN', 'localhost:27017')
client = MongoClient(mongo_conn)
db = client.tempdb

logging.basicConfig(filename='mark.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

re_error = re.compile('<error>(.*)</error>')


class Utm:
    """ УТМ
    Включает в себя название, адрес сервера, заголовок-адрес, путь к XML обмену Супермага
    """

    def __init__(self, fsrar, host, title, path, ukm):
        self.fsrar = fsrar
        self.host = host
        self.title = title
        self.path = path
        self.ukm = ukm

    def log_dir(self):
        return f'//{self.host}.{DOMAIN}/{UTM_LOG_PATH}'


class MarkErrors:

    def __init__(self, logdate, fsrar, error, mark=None):
        self.fsrar = fsrar
        self.date = logdate
        self.error = error
        self.mark = mark


def get_utm_list(filename: str = UTM_CONFIG):
    with open(filename, 'r', encoding='utf-8') as f:
        utms = [Utm(*u.split(';')) for u in f.read().splitlines()]
        utms.sort(key=lambda utm: utm.fsrar)
    return utms


def catch_error_line(line: str, re_err) -> Optional[str]:
    """ Поиск ошибок в строке и возврат текста ошибки"""
    error_result = re_err.search(line)
    return error_result.groups()[0] if error_result else None


def get_marks_from_errors(mark_res: str) -> (str, str):
    """ Выделение марки и описания ошибки из ошибки лога УТМ"""
    description, mark = mark_res.split('(')
    description = description.strip(' ')
    mark = mark.strip(')')
    return mark, description


def parse_log_for_errors(filename: str) -> (list, int, str):
    """ Возвращаем список событий с ошибками, кол-во чеков в логе"""
    error_mark_events = []

    try:
        with open(filename, encoding="utf8") as file:
            for line in file.readlines():
                error_text = catch_error_line(line, re_error)
                if error_text is not None:
                    error_time = datetime.strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                    error_mark_events.append([error_time, error_text])
        # logging.info(f'Итого строк с ошибками: {filename} : {len(error_mark_events)}')

    except (FileNotFoundError, TypeError):
        logging.error(f'Недоступен или журнал не найден {filename}')

    return error_mark_events


def parse_errors(errors: list, fsrar: str):
    """ собираем список объектов ошибок для дальнешей обработки"""
    processed_errors = []

    nonvalid = 'Невалидные марки'
    bad_time = 'продажа в запрещенное время'
    no_filter = 'Настройки еще не обновлены'
    no_key = 'Ошибка поиска модели'

    for e in errors:
        error_time = e[0]
        error_text = e[1]

        try:
            if nonvalid in error_text:
                a, b = error_text.find('['), error_text.find(']')
                marks = error_text[a + 1:b].split(', ')
                for m in marks:
                    processed_errors.append(MarkErrors(error_time, fsrar, nonvalid, m))

            elif bad_time in error_text:
                processed_errors.append(MarkErrors(error_time, fsrar, bad_time))

            elif no_filter in error_text:
                processed_errors.append(MarkErrors(error_time, fsrar, no_filter))

            elif no_key in error_text:
                processed_errors.append(MarkErrors(error_time, fsrar, no_key))

            else:
                _, title, error_line = error_text.split(':')
                split_results = error_line.split(',')
                for mark_res in split_results:
                    mark, description = get_marks_from_errors(mark_res)
                    processed_errors.append(MarkErrors(error_time, fsrar, description, mark))

        except ValueError:
            processed_errors.append(MarkErrors(error_time, fsrar, 'Не удалось обработать ошибку: ' + error_text))

    return processed_errors


def get_transport_transaction_filenames(basename: str = 'transport_transaction.log') -> List[str]:
    """ Список файлов журналов за последние 4 дня, включая """
    today = datetime.now()
    log_date_format = '%Y_%m_%d'

    f1 = f'{basename}.{(today - timedelta(days=1)).strftime(log_date_format)}'
    f2 = f'{basename}.{(today - timedelta(days=2)).strftime(log_date_format)}.zip'
    f3 = f'{basename}.{(today - timedelta(days=3)).strftime(log_date_format)}.zip'
    return [basename, f1, f2, f3]


def extract_transactions(log_path: str, fsrar: str):
    """ Распаковка zip журналов локально для архива, без перезаписи """
    filename = log_path.split('/')[-1]
    local_unzip_path = f'utm_logs/{fsrar}/'
    local_unzipped_file = f'{local_unzip_path}{filename[:-4]}'
    if not os.path.isfile(local_unzipped_file):
        with zipfile.ZipFile(log_path, 'r') as zip_ref:
            zip_ref.extractall(local_unzip_path)
            return local_unzipped_file


def get_existing_transactions_files(utm: Utm, filenames: List[str]):
    """ Получение списка существующих журналов транзакций для УТМ """
    result = []
    log_dir = utm.log_dir()
    if os.path.isdir(log_dir):
        for file in filenames:
            log_path = log_dir + file
            if os.path.isfile(log_path):
                if 'zip' not in log_path:
                    result.append(log_path)
                else:
                    result.append(extract_transactions(log_path, utm.fsrar))

    else:
        logging.error(f'Недоступен путь {log_dir}')

    return result


def process_transport_transaction_log_old(fsrar, file: str):
    """ Сохранение ошибок из файла журанала транзакций УТМ в MongoDB  """
    errors_found = parse_log_for_errors(file)
    errors_objects = parse_errors(errors_found, fsrar)
    for e in errors_objects:
        if not db.mark_errors.find_one({'date': e.date, 'fsrar': fsrar}):
            db.mark_errors.insert_one(vars(e))
            logging.info(f'Добавлена {fsrar} {e.error} {e.mark}')
        else:
            logging.error(f'Ошибка уже в журнале {fsrar} {e.error} {e.mark}')


def send_email(subject: str, text: str, mail_from: str = 'balega_aa@remi.ru', mail_to: str = 'balega_aa@remi.ru'):
    """ Отправка сообщений об ошибках """

    msg = MIMEText(text, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = mail_from
    msg['To'] = mail_to

    try:
        with smtplib.SMTP(MAIL_HOST) as server:
            server.login(MAIL_USER, MAIL_PASS)
            server.sendmail(msg['From'], msg['To'], msg.as_string())
    except:
        logging.error(f'Ошибка отправки email {MAIL_USER}@{MAIL_HOST}:{MAIL_PASS}')


def process_transport_transaction_log(u: Utm, file: str):
    """ Сохранение ошибок из файла журанала транзакций УТМ в MongoDB  """
    errors_found = parse_log_for_errors(file)
    errors_objects = parse_errors(errors_found, u.fsrar)

    errors_to_mail = []
    for e in errors_objects:
        if not db.mark_errors.find_one({'date': e.date, 'fsrar': u.fsrar}):
            db.mark_errors.insert_one(vars(e))
            errors_to_mail.append(e)
            logging.info(f'Добавлена {u.fsrar} {e.error} {e.mark}')

        else:
            logging.error(f'Ошибка уже в журнале {u.fsrar} {e.error} {e.mark}')

    if errors_to_mail:
        human_date = '%Y.%m.%d %H:%M'
        message = '\n\n'.join([f'{e.date.strftime(human_date)} Ошибка "{e.error}"\n{e.mark}' for e in errors_to_mail])
        message = f'{u.title} {u.fsrar} {u.host}\n При проверке были найдены следующие ошибки:\n\n' + message
        subj = f'Ошибка УТМ {u.title} {datetime.today().strftime("%Y.%m.%d")}'
        send_email(subj, message, MAIL_FROM, MAIL_TO)
        logging.info(f'Отправлено сообщение об {len(errors_to_mail)} ошибках')


def process_utm(u: Utm, filenames: List[str]):
    """ Сбор и обработку журналов транзакций УТМ """
    logging.info(f'УТМ {u.host} {u.title} {u.fsrar}')
    files = get_existing_transactions_files(u, filenames)
    [process_transport_transaction_log(u, file) for file in files if file]


def main():
    transport_transactions_files = get_transport_transaction_filenames()
    [process_utm(u, transport_transactions_files) for u in get_utm_list()]


main()
