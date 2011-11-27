# -*- coding: cp1251 -*-
from pony.main import *

use_autoreload()

@http('/')
def page1():
    f1 = Form(name='form1')
    f1.msg = StaticText(u'Служебный роман')
    f1.rating = Text(u'Рейтинг', type=int)
    f1.sbm = Submit(u'Проголосовать')
    f2 = Form(name='form2')
    f2.msg = StaticText(u'Криминальное чтиво')
    f2.rating = Text(u'Рейтинг',type=int)
    f2.sbm = Submit(u'Проголосовать')

    forms = [ f1, f2 ]
    for f in forms:
        if f.is_valid:
            return html(u'<h4>Фильм: @f.msg.value  Рейтинг: @f.rating.value</h4>')

    return html('''
        @for(f in forms)
        {
            @f
        }
    ''')

http.start()    