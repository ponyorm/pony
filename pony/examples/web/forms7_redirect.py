# -*- coding: cp1251 -*-
from pony.main import *

use_autoreload()

@http('/')
def page1():
    f = Form()
    f.msg = StaticText(u'Перевод денег между счетами')
    f.from_account = Text(u'с')
    f.to_account = Text(u'на')
    f.amount = Text(u'Сумма')
    f.sbm = Submit()
    if f.is_valid:
        raise http.Redirect('/success')
    else:
        return f

@http('/success')
def page2():
    return html(u"<h4>Операция выполнена успешно</h4>")


http.start()    