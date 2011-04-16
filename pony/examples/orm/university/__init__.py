from pony.sqltranslator import select
from pony.orm import sql_debug

sql_debug(False)
from pony.examples.orm.university.university import *
sql_debug(True)
