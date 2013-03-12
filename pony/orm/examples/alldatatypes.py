#coding: cp1251

from datetime import date, datetime
from decimal import Decimal
from pony.orm.core import *
import time

#db = Database('oracle', 'presentation/pony@localhost')
#db = Database('postgres', user='pony', password='magic', host='localhost', database='presentation')
#db = Database('mysql', user='pony', passwd='magic', host='localhost', db='test')
db = Database('sqlite', 'alldatatypes.sqlite', create_db=True)

sql_debug(False)  # sql_debug(True) can result in long delay due to enormous print

class AllDataTypes(db.Entity):
    a_bool1 = Required(bool)
    a_bool2 = Required(bool)
    a_unicode = Required(unicode)
    a_str = Required(str, encoding='cp1251')
    a_long_unicode = Required(LongUnicode)
    a_long_str = Required(LongStr, encoding='cp1251')
    a_int = Required(int)
    a_long = Required(long)
    a_float = Required(float)
    a_decimal = Required(Decimal)
    a_buffer = Required(buffer)
    a_datetime = Required(datetime)
    a_date = Required(date)

db.generate_mapping(create_tables=True)

s = "".join(map(chr, range(256))) * 1000

fields = dict(a_bool1=True, a_bool2=False,
              a_unicode=u"Юникод",
              a_str="Строка",
              a_long_unicode = u"Юникод" * 100000,
              a_long_str = "Строка" * 100000,
              a_int=-2000000,
              a_long=123456789123456789,
              a_float=3.1415927, a_decimal=Decimal("0.1"),
              a_buffer=buffer(s),
              a_datetime=datetime.now(), a_date=date.today())

db.execute('delete from %s' % db.provider.quote_name('AllDataTypes'))
commit()

t1 = time.time()
e1 = AllDataTypes(**fields)

commit()

rollback()
t2 = time.time()

e2 = AllDataTypes.select().first()
t3 = time.time()

for name, value in fields.items():
    value2 = getattr(e2, name)
    print value==value2, name,
    if value!=value2: print 'py=', repr(value), 'db=', repr(value2)
    else: print

for i, (ch1, ch2) in enumerate(zip(s, str(e2.a_buffer))):
    if ch1 <> ch2: print i, repr(ch1), repr(ch2), ch1, ch2

commit()

print t2-t1, t3-t2
