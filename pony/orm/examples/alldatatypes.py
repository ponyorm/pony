#coding: cp1251

from __future__ import absolute_import, print_function
from pony.py23compat import PY2

from datetime import date, time, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4
from time import time as get_time

from pony.orm.core import *

#db = Database('oracle', 'presentation/pony@localhost')
#db = Database('postgres', user='presentation', password='pony', host='localhost', database='cyrillic')
#db = Database('mysql', user='presentation', passwd='pony', host='localhost', db='test')
#db = Database('sqlite', 'alldatatypes.sqlite', create_db=True)

class AllDataTypes(db.Entity):
    bool1_attr = Required(bool)
    bool2_attr = Required(bool)
    str_attr = Required(str)
    long_str_attr = Required(LongStr)
    int_attr = Required(int)
    long_attr = Required(int, size=64)
    float_attr = Required(float)
    decimal_attr = Required(Decimal)
    buffer_attr = Required(buffer)
    datetime_attr = Required(datetime, 6)
    date_attr = Required(date)
    time_attr = Required(time, 6)
    timedelta_attr = Required(timedelta, 6)
    uuid_attr = Required(UUID)

sql_debug(True)
db.generate_mapping(check_tables=False)
AllDataTypes.drop_table(with_all_data=True)
db.create_tables()
sql_debug(False)  # sql_debug(True) can result in long delay due to enormous print

if PY2:
    b = "".join(map(chr, range(256))) * 1000
else:
    b = bytes(i % 256 for i in range(256 * 1000))

fields = dict(bool1_attr=True,
              bool2_attr=False,
              str_attr=u"Юникод",
              long_str_attr = u"Юникод" * 100000,
              int_attr=-2000000,
              long_attr = 123456789123456789,
              float_attr=3.1415927,
              decimal_attr=Decimal("0.1"),
              buffer_attr=buffer(b),
              datetime_attr=datetime.now(),
              date_attr=date.today(),
              time_attr=datetime.now().time(),
              timedelta_attr=timedelta(hours=1, minutes=1, seconds=1, microseconds=3333),
              uuid_attr=uuid4())

with db_session:
    for obj in AllDataTypes.select():
        obj.delete()
    commit()

    t1 = get_time()
    e1 = AllDataTypes(**fields)

    commit()

    rollback()
    t2 = get_time()

    e2 = AllDataTypes.select().first()
    t3 = get_time()

    for name, value in fields.items():
        value2 = getattr(e2, name)
        if value == value2: print(True, name)
        else: print(False, name, 'py=', repr(value), 'db=', repr(value2))

    b2 = e2.buffer_attr
    for i, (b1, b2) in enumerate(zip(b, b2)):
        if b1 != b2: print(i, repr(b1), repr(b2), b1, b2)

    commit()

    print("Created in %ss; extracted in %ss" % (t2-t1, t3-t2))
