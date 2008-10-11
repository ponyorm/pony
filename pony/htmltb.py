import sys, inspect #, cgitb, cStringIO
from itertools import izip, count

import pony
from pony.templating import html, cycle

class Record(object):
    def __init__(self, **keyargs):
        self.__dict__.update(keyargs)

def format_exc(context=5):
    exc_type, exc_value, tb = sys.exc_info()
    if tb.tb_next is not None: tb = tb.tb_next
    if tb.tb_next is not None: tb = tb.tb_next
    if tb.tb_next is not None: tb = tb.tb_next
    try:
        records = []
        for frame, file, lnum, func, lines, index in inspect.getinnerframes(tb, context):
            record = Record(frame=frame, file=file, lnum=lnum, func=func, lines=lines, index=index)
            if frame.f_globals['__name__'].startswith('pony.'): record.moduletype = 'system'
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
