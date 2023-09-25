from __future__ import absolute_import, print_function

from pony.orm.core import *

db = Database()

class Numbers(db.Entity):
    _table_ = "Numbers"
    id = PrimaryKey(int, auto=True)
    int8 = Required(int, size=8)    # TINYINT
    int16 = Required(int, size=16)  # SMALLINT
    int24 = Required(int, size=24)  # MEDIUMINT
    int32 = Required(int, size=32)  # INTEGER
    int64 = Required(int, size=64)  # BIGINT
    uint8 = Required(int, size=8, unsigned=True)    # TINYINT UNSIGNED
    uint16 = Required(int, size=16, unsigned=True)  # SMALLINT UNSIGNED
    uint24 = Required(int, size=24, unsigned=True)  # MEDIUMINT UNSIGNED
    uint32 = Required(int, size=32, unsigned=True)  # INTEGER UNSIGNED
    # uint64 = Required(int, size=64, unsigned=True)  # BIGINT UNSIGNED, supported by MySQL and Oracle

sql_debug(True)  # Output all SQL queries to stdout

db.bind('sqlite', 'test_numbers.sqlite', create_db=True)
#db.bind('mysql', host="localhost", user="pony", passwd="pony", db="test_numbers")
#db.bind('postgres', user='pony', password='pony', host='localhost', database='test_numbers')
#db.bind('oracle', 'test_numbers/pony@localhost')

db.drop_table("Numbers", if_exists=True, with_all_data=True)
db.generate_mapping(create_tables=True)

@db_session
def populate_database():
    lo = Numbers(int8=-128,
                 int16=-32768,
                 int24=-8388608,
                 int32=-2147483648,
                 int64=-9223372036854775808,
                 uint8=0, uint16=0, uint24=0, uint32=0) #, uint64=0)
    hi = Numbers(int8=127,
                 int16=32767,
                 int24=8388607,
                 int32=2147483647,
                 int64=9223372036854775807,
                 uint8=255,
                 uint16=65535,
                 uint24=16777215,
                 uint32=4294967295)
                 # uint64=18446744073709551615)
    commit()

@db_session
def test_data():
    for n in Numbers.select():
        print(n.id, n.int8, n.int16, n.int24, n.int32, n.int64,
              n.uint8, n.uint16, n.uint24, n.uint32) #, n.uint64)

if __name__ == '__main__':
    populate_database()
    test_data()
