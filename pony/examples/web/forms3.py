# -*- coding: cp1251 -*-

from pony.main import *

@http('/')
def index():
    f = Form(secure=False)
    f.country = Select('Select your country', required=True,
                       options=[ '', 'USA', 'Australia', 'Russia' ])
    if f.country.value in ('USA', 'Australia'):
        f.secure = True # Make multi-step form secure only on last step
        f.state = Select('Select your state', True)
        if f.country.value == 'USA':
            f.state.options = [ '', 'Alabama', 'New York', 'Virginia' ]
        else:
            f.state.options = [ '', 'Tasmania', 'Victoria', 'Western Australia']
        f.zip = Text('Zip code', required=True)
        f.first_name = Text(required=True)
        f.last_name = Text()
        f.email = Text('E-mail', required=True)
        f.password = Password(required=True)
        f.password2 = Password('Re-type password', True)
        f.comment = TextArea('You can type comment here')
        f.file = File('Attachment file')
        f.subscribe = Checkbox('I want receive news', value=True)
        f.news_categories = MultiSelect(options=[ 'Daily reviews',
                                                  'Weekly digests',
                                                  'Security updates' ],
                                        value=[ 'Daily reviews',
                                                'Weekly digests' ])
    elif f.country.value == 'Russia':
        f.secure = True # Make multi-step form secure only on last step
        f.country.label = u'Выберите страну'
        f.city = Select(u'Выберите город', True,
                        options = ['', u'Москва',
                                       u'Санкт-Петербург',
                                       u'Владивосток'])
        f.last_name = Text(u'Фамилия', required=True)
        f.fist_name = Text(u'Имя', required=True)
        f.patronymic_name = Text(u'Отчество')
        f.sex = RadioGroup(u'Пол', options=[ u'Мужской', u'Женский' ])
        f.email = Text(u'Почтовый адрес', True)
        f.password = Password(u'Пароль', True)
        f.password2 = Password(u'Введите пароль еще раз', True)
        f.subscribe = Checkbox(u'Я хочу получать новости', value=True)
        f.news_categories = CheckboxGroup(u'Категории новостей',
                                          options=[ (1, u'Недельные выпуски'),
                                                    (2, u'Месячные выпуски'),
                                                    (3, u'Срочные сообщения') ],
                                          value=[ 1, 2 ])

    if (f.country.value and f.password.is_submitted
                        and f.password2.is_submitted
                        and f.password.value != f.password2.value):
        if f.country.value == 'Russia': msg = u'Пароль не совпадает!'
        else: msg = 'Password did not match!'
        f.password.error_text = f.password2.error_text = msg

    return html('''
        <style>
            .required, .error { color: red; }
        </style>
        $f.html
    ''')

start_http_server()
