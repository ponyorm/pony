import re, sys, inspect, keyword #, cgitb, cStringIO
from itertools import izip, count

import pony
from pony import options
from pony.utils import detect_source_encoding, is_ident
from pony.templating import html, cycle, quote, htmljoin, Html, StrHtml

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
            lines = [ format_line(line.decode(source_encoding, 'replace')) for line in lines ]
            record = Record(frame=frame, file=file, lnum=lnum, func=func, lines=lines, index=index)
            module = record.module = frame.f_globals['__name__'] or '?'
            if module == 'pony' or module.startswith('pony.'): record.moduletype = 'system'
            else: record.moduletype = 'user'
            records.append(record)
        return html()
    finally: del tb

python_re = re.compile(r"""
        (                                        # string (group 1)
        (?:[Uu][Rr]?|[Rr][Uu]?)?                 #     string prefix 
        (?:                                      
            '''(?:[^\\]|\\.)*?(?:'''|\Z)         #     '''triple-quoted string'''
        |   \"""(?:[^\\]|\\.)*?(?:\"""|\Z)       #     \"""triple-quoted string\"""
        |   '(?:[^'\\]|\\.)*?'                   #     'string'
        |   "(?:[^"\\]|\\.)*?"                   #     "string"
        ))
    |   ([A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*)  # identifier chain (group 2)
    |   (\#.*$)                                  # comment (group 3)
    """, re.VERBOSE)
           

ident_re = re.compile(r'[A-Za-z_]\w*')
end1_re = re.compile(r"(?:[^\\]|\\.)*?'''")
end2_re = re.compile(r'(?:[^\\]|\\.)*?"""')

ident_html = StrHtml('<span class="ident" title="%s">%s</span>')
keyword_html = StrHtml('<strong>%s</strong>')
comment_html = StrHtml('<span class="comment">%s</span>')
str_html = StrHtml('<span class="string">%s</span>')

def format_line(line):
    result = []
    pos = 0
    end = len(line)
    while pos < end:
        match = python_re.search(line, pos)
        if match is None: break
        result.append(quote(line[pos:match.start()]))
        i = match.lastindex
        if i == 1: result.append(str_html % match.group())
        elif i == 2:
            chain = []
            for x in re.split('(\W+)', match.group()):
                if x in keyword.kwlist: result.append(keyword_html % x)
                elif is_ident(x):
                    chain.append(x)
                    title = '.'.join(chain)
                    result.append(ident_html % (title, x))
                else: result.append(quote(x))
        elif i == 3: result.append(comment_html % match.group())
        else: assert False
        pos = match.end()
    result.append(quote(line[pos:]))
    return htmljoin(result)

##def format_exc():
##    exc_type, exc_value, traceback = sys.exc_info()
##    try:
##        io = cStringIO.StringIO()
##        hook = cgitb.Hook(file=io)
##        hook.handle((exc_type, exc_value, traceback))
##        return io.getvalue()
##    finally: del traceback
