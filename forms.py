from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SelectField, BooleanField
from wtforms.validators import DataRequired, Length, Regexp


class FsrarForm(FlaskForm):
    fsrar = SelectField('fsrar', coerce=int)


class RestsForm(FsrarForm):
    alc_code = StringField('alc_code')
    limit = IntegerField('limit')


class TicketForm(FsrarForm):
    search = StringField('search', validators=[DataRequired()])
    limit = IntegerField('limit')


class MarkForm(FsrarForm):
    mark = StringField('mark', validators=[DataRequired()])


class MarkFormError(FsrarForm):
    error = SelectField('error_type', coerce=int)
    mark = StringField('mark')


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


class CreateUpdateUtm(FlaskForm):
    fsrar = StringField('fsrar', validators=[DataRequired()])
    title = StringField('title', validators=[DataRequired()])
    host = StringField('host', validators=[DataRequired()])
    ukm = StringField('ukm', validators=[DataRequired()])
    active = BooleanField('active')
    path = StringField('path')
    _id = StringField('_id')


class StatusSelectOrder(FlaskForm):
    choices = (
        ('fsrar', 'ФСРАР'),
        ('title', 'Адрес'),
        ('legal', 'Организация'),
        ('surname', 'Директор'),
        ('gost', 'ГОСТ'),
        ('pki', 'PKI'),
        ('host', 'Сервер'),
        ('filter', 'Фильтр'),
        ('license', 'Лицензия'),
        ('version', 'Версия'),
        ('error', 'Ошибки'),
    )

    ordering = SelectField('ordering', choices=choices)
