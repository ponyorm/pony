import pony.orm.core
import pony.options

pony.options.CUT_TRACEBACK = False
pony.orm.core.sql_debug(False)
