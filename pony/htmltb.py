import sys, inspect #, cgitb, cStringIO
from itertools import izip, count

import pony
from pony import options
from pony.utils import detect_source_encoding
from pony.templating import html, cycle

class Record(object):
    def __init__(self, **keyargs):
        self.__dict__.update(keyargs)

def format_exc(info=None, context=5):
    if info: exc_type, exc_value, tb = info
    else:
        exc_type, exc_value, tb = sys.exc_info()
        # if tb.tb_next is not None: tb = tb.tb_next  # application() frame
        # if tb.tb_next is not None: tb = tb.tb_next  # http_invoke() frame
        # if tb.tb_next is not None: tb = tb.tb_next  # with_transaction() frame
    try:
        records = []
        for frame, file, lnum, func, lines, index in inspect.getinnerframes(tb, context):
            source_encoding = detect_source_encoding(file)
            lines = [ line.decode(source_encoding, 'replace') for line in lines ]
            record = Record(frame=frame, file=file, lnum=lnum, func=func, lines=lines, index=index)
            module = record.module = frame.f_globals['__name__'] or '?'
            if module == 'pony' or module.startswith('pony.'): record.moduletype = 'system'
            else: record.moduletype = 'user'
            records.append(record)
        return html()
    finally: del tb

##def format_exc():
##    exc_type, exc_value, traceback = sys.exc_info()
##    try:
##        io = cStringIO.StringIO()
##        hook = cgitb.Hook(file=io)
##        hook.handle((exc_type, exc_value, traceback))
##        return io.getvalue()
##    finally: del traceback
