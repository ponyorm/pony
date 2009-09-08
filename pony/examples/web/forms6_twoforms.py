# -*- coding: cp1251 -*-
from pony.main import *

use_autoreload()

@webpage('/')
def page1():
    f1 = Form(name='form1')
    f1.msg = StaticText(u'Служебный роман')
    f1.rating = Text(u'Рейтинг', type=int)
    f1.sbm = Submit(u'Проголосовать')
    f2 = Form(name='from2')
    f2.msg = StaticText(u'Криминальное чтиво')
    f2.rating = Text(u'Рейтинг',type=int)
    f2.sbm = Submit(u'Проголосовать')

    for f in [f1, f2]:
        if f.is_valid:
            raise http.Redirect(url(success, f.msg.value, f.rating.value))
        else:
            print f

@webpage
def success(movie, rating):
    print u"<h4>Фильм:%s  Рейтинг:%s</h4>" % (movie, rating)


http.start()    