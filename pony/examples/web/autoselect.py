# coding: cp1251

from pony.main import *

use_autoreload()

@webpage('/')
def index():
    f = Form()
    f.continent = AutoSelect(u'Континент', options=[ (None, u'<Выберите континент>'), u'Америка', u'Евразия' ])
    if f.continent.value:
        if f.continent.value == u'Америка': countries = [ u'США', u'Бразилия' ]
        elif f.continent.value == u'Евразия': countries = [ u'Россия', u'Франция' ]
        f.country = AutoSelect(u'Страна', options= [ (None, u'<Выберите страну>') ] + countries)
        if f.country.value:
            if f.country.value == u'США': cities = [ u'Нью-Йорк', u'Лос-Анджелес', u'Вашингтон' ]
            elif f.country.value == u'Бразилия': cities = [ u'Рио-де-Жанейро', u'Белу-Оризонти', u'Бразилиа' ]
            elif f.country.value == u'Франция': cities = [ u'Париж', u'Лион', u'Марсель' ]
            elif f.country.value == u'Россия': cities = [ u'Москва', u'Санкт-Петербург', u'Норильск' ]
            f.city = AutoSelect(u'Город', options=[ (None, u'<Выберите город>') ] + cities)
            if f.city.value:
                print u'<h1>Результат:</h1>'
                print u'<h2>Континент: %s</h2>' % f.continent.value
                print u'<h2>Страна: %s</h2>' % f.country.value
                print u'<h2>Город: %s</h2>' % f.city.value
    print f

http.start()    
