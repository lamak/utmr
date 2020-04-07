import logging
import os
import re
import smtplib
from datetime import datetime
from email.header import Header
from email.mime.text import MIMEText
from typing import Optional, Tuple, Union

from pymongo import MongoClient

from app import Utm, Configs
from config import AppConfig


class MarkErrors:

    def __init__(self, log_date, title, fsrar, error, mark=None):
        self.title = title
        self.fsrar = fsrar
        self.date = log_date
        self.error = error
        self.mark = mark


def get_utm_list(filename: str = AppConfig.UTM_CONFIG):
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
    re_error = re.compile('<error>(.*)</error>')
    error_mark_events = []
    cheques_counter = 0
    err = None

    try:
        with open(filename, encoding="utf8") as file:
            cheque_text = 'Получен чек.'

            for line in file.readlines():
                if cheque_text in line:
                    cheques_counter += 1

                else:
                    error_text = catch_error_line(line, re_error)
                    if error_text is not None:
                        error_time = datetime.strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                        error_mark_events.append([error_time, error_text])

    except (FileNotFoundError, TypeError):
        err = 'Недоступен или журнал не найден'
        logging.error(f'{err} {filename}')

    return error_mark_events, cheques_counter, err


def parse_errors(errors: list, utm: Utm):
    """ собираем список объектов ошибок для дальнешей обработки"""
    processed_errors = []

    nonvalid = 'Невалидные марки'
    bad_time = 'продажа в запрещенное время'
    no_filter = 'Настройки еще не обновлены'
    last_cheque = 'Подпись предыдущего чека не завершена.'
    no_key = 'Ошибка поиска модели'

    for e in errors:
        error_time = e[0]
        error_text = e[1]

        try:
            if nonvalid in error_text:
                a, b = error_text.find('['), error_text.find(']')
                marks = error_text[a + 1:b].split(', ')
                for m in marks:
                    processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, nonvalid, m))

            elif bad_time in error_text:
                processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, bad_time))

            elif no_filter in error_text:
                processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, no_filter))

            elif no_key in error_text:
                processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, no_key))

            elif last_cheque in error_text:
                processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, last_cheque))

            else:
                _, title, error_line = error_text.split(':')
                split_results = error_line.split(',')
                for mark_res in split_results:
                    mark, description = get_marks_from_errors(mark_res)
                    processed_errors.append(MarkErrors(error_time, utm.title, utm.fsrar, description, mark))

        except ValueError:
            processed_errors.append(
                MarkErrors(error_time, utm.title, utm.fsrar, 'Не удалось обработать ошибку: ' + error_text))

    return processed_errors


def get_transport_transaction_filenames(current: str = AppConfig.UTM_LOG_NAME) -> Tuple[str]:
    """ Список файлов журналов за последние 2 дня"""
    # todo: при штатном запуске не существует, может понадобится при пропущенном запуске
    # yesterday = f'{current}.{(datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")}'
    return current,


def check_file_exist(utm: Utm, filename: str):
    """ Проверяем существование файла журнала """

    filename = utm.log_dir() + filename

    if not os.path.isfile(filename):
        logging.error(f'Недоступен файл {filename}')

    return filename


def send_email(subject: str, text: str, mail_from: Union[str, list], mail_to: str):
    """ Отправка сообщений об ошибках """
    msg = MIMEText(text, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = mail_from
    msg['To'] = mail_to

    try:
        with smtplib.SMTP(AppConfig.MAIL_HOST) as server:
            server.login(AppConfig.MAIL_USER, AppConfig.MAIL_PASS)
            server.sendmail(msg['From'], msg['To'], msg.as_string())
    except:
        logging.error(f'Ошибка отправки email {AppConfig.MAIL_USER}@{AppConfig.MAIL_HOST}:{AppConfig.MAIL_PASS}')


def process_transport_transaction_log(u: Utm, file: str):
    """ Сохранение ошибок из файла журнала транзакций УТМ в MongoDB и отправка писем """

    file = check_file_exist(u, file)

    if file is not None:
        errors_found, _, _ = parse_log_for_errors(file)
        errors_objects = parse_errors(errors_found, u)

        err_to_mail = []
        with MongoClient(AppConfig.MONGO_CONN) as client:
            col = client[AppConfig.MONGO_DB][AppConfig.MONGO_COL_ERR]

            for e in errors_objects:

                if not col.find_one({'date': e.date, 'fsrar': u.fsrar}):
                    col.insert_one(vars(e))
                    err_to_mail.append(e)
                    logging.info(f'Добавлена {u.fsrar} {e.error} {e.mark}')

        if err_to_mail:
            human_date = '%Y.%m.%d %H:%M'
            message = '\n\n'.join([f'{e.date.strftime(human_date)} Ошибка "{e.error}"\n{e.mark}' for e in err_to_mail])
            message = f'{u.title} {u.fsrar} {u.host}\n При проверке были найдены следующие ошибки:\n\n' + message
            subj = f'Ошибка УТМ {u.title} {datetime.today().strftime("%Y.%m.%d")}'
            send_email(subj, message, AppConfig.MAIL_FROM, AppConfig.MAIL_TO)
            logging.info(f'Отправлено сообщение об {len(err_to_mail)} ошибках')


def process_utm(u: Utm, filenames: Tuple[str]):
    """ Сбор и обработку журналов транзакций УТМ """
    logging.info(f'УТМ {u.host} {u.title} {u.fsrar}')
    [process_transport_transaction_log(u, file) for file in filenames]


def main():
    with MongoClient(AppConfig.MONGO_CONN) as client:
        db = client[AppConfig.MONGO_DB]
        cfg = Configs(db)
        utms = cfg.utms

    start = datetime.now()
    transport_transactions_files = get_transport_transaction_filenames()
    [process_utm(u, transport_transactions_files) for u in utms]
    logging.info(f'Done: {datetime.now() - start}')


main()
