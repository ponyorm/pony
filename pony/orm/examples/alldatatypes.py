#coding: cp1251

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4
from pony.orm.core import *
import time

#db = Database('oracle', 'presentation/pony@localhost')
db = Database('pygresql', user='presentation', password='pony', host='localhost', database='cyrillic')
#db = Database('mysql', user='presentation', passwd='pony', host='localhost', db='test')
#db = Database('sqlite', 'alldatatypes.sqlite', create_db=True)

class AllDataTypes(db.Entity):
    bool1_attr = Required(bool)
    bool2_attr = Required(bool)
    unicode_attr = Required(unicode)
    str_attr = Required(str, encoding='cp1251')
    long_unicode_attr = Required(LongUnicode)
    long_str_attr = Required(LongStr, encoding='cp1251')
    int_attr = Required(int)
    long_attr = Required(long)
    float_attr = Required(float)
    decimal_attr = Required(Decimal)
    buffer_attr = Required(buffer)
    datetime_attr = Required(datetime)
    date_attr = Required(date)
    uuid_attr = Required(UUID)

sql_debug(True)
db.generate_mapping(create_tables=True)
sql_debug(False)  # sql_debug(True) can result in long delay due to enormous print

s = "".join(map(chr, range(256))) * 1000

fields = dict(bool1_attr=True,
              bool2_attr=False,
              unicode_attr=u"Юникод",
              str_attr="Строка",
              long_unicode_attr = u"Юникод" * 100000,
              long_str_attr = "Строка" * 100000,
              int_attr=-2000000,
              long_attr=123456789123456789,
              float_attr=3.1415927,
              decimal_attr=Decimal("0.1"),
              buffer_attr=buffer(s),
              datetime_attr=datetime.now(),
              date_attr=date.today(),
              uuid_attr=uuid4())

for obj in AllDataTypes.select():
    obj.delete()
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

for i, (ch1, ch2) in enumerate(zip(s, str(e2.buffer_attr))):
    if ch1 <> ch2: print i, repr(ch1), repr(ch2), ch1, ch2

commit()

print t2-t1, t3-t2
