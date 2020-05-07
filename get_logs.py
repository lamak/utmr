import logging
import os
import re
import smtplib
from datetime import datetime
from email.header import Header
from email.mime.text import MIMEText
from typing import Optional, Union, List

from app import Utm
from config import AppConfig


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


def parse_errors(errors: list, utm: Utm) -> List[dict]:
    """ собираем список объектов ошибок для дальнешей обработки"""

    def add_mark(lst, date, ttl, fsrar_id, err, err_marks=None):
        template_result = {'date': date, 'title': ttl, 'fsrar': fsrar_id, 'error': err}

        if err_marks is None:
            lst.append(template_result)

        elif isinstance(err_marks, list):
            for m in err_marks:
                template_result['mark'] = m
                lst.append(template_result)

        else:
            template_result['mark'] = err_marks
            lst.append(template_result)

    no_key = 'Ошибка поиска модели'
    bad_time = 'продажа в запрещенное время'
    no_filter = 'Настройки еще не обновлены'
    last_cheque = 'Подпись предыдущего чека не завершена.'
    invalid_mark = 'Невалидные марки'
    errs = (invalid_mark, bad_time, no_filter, last_cheque, no_key)

    title = utm.title
    fsrar = utm.fsrar
    parsed_entries = []

    for e in errors:
        dt, message = e

        try:
            r = [err for err in errs if err in message]
            if r:
                marks = None
                if r[0] == invalid_mark:
                    a, b = message.find('['), message.find(']')
                    marks = message[a + 1:b].split(', ')

                add_mark(parsed_entries, dt, title, fsrar, r[0], marks)

            else:
                _, _, error_line = message.split(':')
                split_results = error_line.split(',')
                for mark_res in split_results:
                    mark, description = get_marks_from_errors(mark_res)
                    add_mark(parsed_entries, dt, title, fsrar, description, mark)

        except ValueError:
            add_mark(parsed_entries, dt, title, fsrar, 'Не удалось обработать ошибку: ' + message)

    return parsed_entries


def get_log_file(utm: Utm, filename: str):
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
    file = get_log_file(u, file)

    if file is not None:
        from app import mongo

        errors_found, _, _ = parse_log_for_errors(file)
        errors = parse_errors(errors_found, u)
        marks = []

        for e in errors:
            if not mongo.db.marks.find_one({'date': e['date'], 'fsrar': e['fsrar']}):
                marks.append(e)

        if marks:
            mongo.db.marks.insert_many(marks)

            human_date = '%Y.%m.%d %H:%M'
            message = '\n\n'.join([f"{e['date'].strftime(human_date)} Ошибка {e['error']}\n{e['mark']}" for e in marks])
            message = f'{u.title} {u.fsrar} {u.host}\n При проверке были найдены следующие ошибки:\n\n' + message
            subj = f'Ошибка УТМ {u.title} {datetime.today().strftime("%Y.%m.%d")}'
            send_email(subj, message, AppConfig.MAIL_FROM, AppConfig.MAIL_TO)
            logging.info(f'Отправлено сообщение об {len(marks)} ошибках')


def process_utm(u: Utm):
    """ Сбор и обработку журналов транзакций УТМ """
    logging.info(f'УТМ {u.host} {u.title} {u.fsrar}')
    process_transport_transaction_log(u, AppConfig.UTM_LOG_NAME)


def main():
    start = datetime.now()
    [process_utm(u) for u in Utm.get_active()]
    logging.info(f'Cheque errors processing done: {datetime.now() - start}')


main()
