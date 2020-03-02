import os

from MySQLdb import cursors


class AppConfig(object):
    LOCAL_DOMAIN = os.environ.get('USERDNSDOMAIN', '.local')
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'uploads')
    RESULT_FOLDER = os.environ.get('RESULT_FOLDER', 'results')

    UTM_PORT = os.environ.get('UTM_PORT', '8080')
    UTM_CONFIG = os.environ.get('UTM_CONFIG', 'config')
    UTM_LOG_PATH = os.environ.get('UTM_PORT', 'c$/utm/transporter/l/')
    DEFAULT_XML_PATH = os.environ.get('DEFAULT_XML_PATH')

    CONVERTER_EXPORT_PATH = os.environ.get('CONVERTER_EXPORT_PATH', './')
    CONVERTER_TEMPLATE_FILE = os.environ.get('CONVERTER_SKU_TEMPLATE', 'sku-body-template.xlsx')

    CONVERTER_DATE_FORMAT = '%Y%m%d'
    LOGFILE_DATE_FORMAT = '%Y_%m_%d'
    HUMAN_DATE_FORMAT = '%Y-%m-%d'
    ALLOWED_EXTENSIONS = {'xlsx', }
    WORKING_DIRS = [UPLOAD_FOLDER, RESULT_FOLDER]

    MARK_ERRORS_LAST_DAYS = int(os.environ.get('MARK_ERRORS_LAST_DAYS', 7))
    MARK_ERRORS_LAST_UTMS = int(os.environ.get('MARK_ERRORS_LAST_UTMS', 15))

    MYSQL_CONN = {
        'db': os.environ.get('UKM_DB'),
        'user': os.environ.get('UKM_USER'),
        'passwd': os.environ.get('UKM_PASSWD'),
        'cursorclass': cursors.DictCursor,
        'charset': 'utf8',
        'use_unicode': True,
    }

    MONGO_CONN = os.environ.get('MONGODB_CONN', 'localhost:27017')
    MONGO_DB = os.environ.get('MONGO_DB', 'tepmdb')