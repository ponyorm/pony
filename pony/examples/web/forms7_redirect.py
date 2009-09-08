# -*- coding: cp1251 -*-
from pony.main import *

use_autoreload()

@webpage('/')
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
        print f

@webpage('/success')
def page2():
    print u"<h4>Операция выполнена успешно</h4>"


http.start()    