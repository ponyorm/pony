import pony.orm.core, pony.options

pony.options.CUT_TRACEBACK = False
pony.orm.core.sql_debug(False)