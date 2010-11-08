import pony.db
pony.db.debug = False

from pony.examples.orm.university.university import *
from pony.sqltranslator import select

pony.db.debug = True