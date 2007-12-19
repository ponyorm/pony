# -*- coding: cp1251 -*-

import re, os, os.path, sys, time, datetime

from itertools import imap, ifilter
from operator import itemgetter
from inspect import isfunction
from time import strptime
from os import urandom
from codecs import BOM_UTF8, BOM_LE, BOM_BE
from locale import getpreferredencoding

class ValidationError(Exception):
    def __init__(self, err_msg=None):
        self.err_msg = err_msg

def copy_func_attrs(new_func, old_func):
    if new_func is old_func: return
    new_func.__name__ = old_func.__name__
    new_func.__doc__ = old_func.__doc__
    new_func.__module__ = old_func.__module__
    new_func.__dict__.update(old_func.__dict__)
    if not hasattr(old_func, 'original_func'):
        new_func.original_func = old_func

def simple_decorator(old_dec):
    def new_dec(old_func):
        def new_func(*args, **keyargs):
            return old_dec(old_func, *args, **keyargs)
        copy_func_attrs(new_func, old_func)
        return new_func
    copy_func_attrs(new_dec, old_dec)
    return new_dec

def decorator(old_dec):
    def new_dec(old_func):
        new_func = old_dec(old_func)
        copy_func_attrs(new_func, old_func)
        return new_func
    copy_func_attrs(new_dec, old_dec)
    return new_dec

def decorator_with_params(old_dec):
    def new_dec(*args, **keyargs):
        if len(args) == 1 and isfunction(args[0]) and not keyargs:
            old_func = args[0]
            new_func = old_dec()(old_func)
            copy_func_attrs(new_func, old_func)
            return new_func
        else:
            def even_more_new_dec(old_func):
                new_func = old_dec(*args, **keyargs)(old_func)
                copy_func_attrs(new_func, old_func)
                return new_func
            copy_func_attrs(even_more_new_dec, old_dec)
            return even_more_new_dec
    copy_func_attrs(new_dec, old_dec)
    return new_dec

def error_method(*args, **kwargs):
    raise TypeError

ident_re = re.compile(r'^[A-Za-z_]\w*\Z')

# is_ident = ident_re.match
def is_ident(string):
    'is_ident(string) -> bool'
    return bool(ident_re.match(string))

def import_module(name):
    "import_module('a.b.c') -> <module a.b.c>"
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]: mod = getattr(mod, comp)
    return mod

def absolutize_path(filename, frame_depth=2):
    code_filename = sys._getframe(frame_depth).f_code.co_filename
    code_path = os.path.dirname(code_filename)
    return os.path.join(code_path, filename)

def get_mtime(filename):
    stat = os.stat(filename)
    mtime = stat.st_mtime
    if sys.platform == "win32": mtime -= stat.st_ctime
    return mtime

def current_timestamp():
    result = datetime.datetime.now().isoformat(' ')
    if len(result) == 19: return result + '.000000'
    return result

def datetime2timestamp(d):
    result = d.isoformat(' ')
    if len(result) == 19: return result + '.000000'
    return result

def timestamp2datetime(t):
    time_tuple = strptime(t[:19], '%Y-%m-%d %H:%M:%S')
    microseconds = int((t[20:26] + '000000')[:6])
    return datetime.datetime(*(time_tuple[:6] + (microseconds,)))

def read_text_file(fname, encoding=None):
    f = file(fname)
    text = f.read()
    f.close;
    for bom, enc in [ (BOM_UTF8, 'utf8'),
                      (BOM_LE, 'utf-16le'),
                      (BOM_BE, 'utf-16be') ]:
        if text[:len(bom)] == bom:
            return text[len(bom):].decode(enc)
    try: return text.decode('utf8')
    except UnicodeDecodeError:
        return text.decode(encoding or getpreferredencoding())

def new_guid():
    'new_guid() -> new_binary_guid'
    return buffer(urandom(16))

def guid2str(guid):
    """guid_binary2str(binary_guid) -> string_guid

    >>> guid2str(unxehlify('ff19966f868b11d0b42d00c04fc964ff'))
    '6F9619FF-8B86-D011-B42D-00C04FC964FF'
    """
    assert isinstance(guid, buffer) and len(guid) == 16
    guid = str(guid)
    return '%s-%s-%s-%s-%s' % tuple(map(hexlify, (
        guid[3::-1], guid[5:3:-1], guid[7:5:-1], guid[8:10], guid[10:])))

def str2guid(s):
    """guid_str2binary(str_guid) -> binary_guid

    >>> unhexlify(str2guid('6F9619FF-8B86-D011-B42D-00C04FC964FF'))
    'ff19966f868b11d0b42d00c04fc964ff'
    """
    assert isinstance(s, basestring) and len(s) == 36
    a, b, c, d, e = map(unhexlify, (s[:8],s[9:13],s[14:18],s[19:23],s[24:]))
    reverse = slice(-1, None, -1)
    return buffer(''.join((a[reverse], b[reverse], c[reverse], d, e)))

def check_ip(s):
    s = s.strip()
    list = map(int, s.split('.'))
    if len(list) != 4: raise ValueError
    for number in list:
        if not 0 <= number <= 255: raise ValueError
    return s

def check_positive(s):
    i = int(s)
    if i > 0: return i
    raise ValueError

def check_identifier(s):
    if ident_re.match(s): return s
    raise ValueError

date_str_list = [
    r'(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{4})',
    r'(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})',
    r'(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,4})',
    r'(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,4})',
    r'(?P<year>\d{4})\.(?P<month>\d{1,2})\.(?P<day>\d{1,4})',
    r'\D*(?P<year>\d{4})\D+(?P<day>\d{1,2})\D*',
    r'\D*(?P<day>\d{1,2})\D+(?P<year>\d{4})\D*'
    ]
date_re_list = [ re.compile('^%s$'%s, re.UNICODE) for s in date_str_list ]

time_str = r'(?P<hh>\d{1,2})(?:[:. ](?P<mm>\d{1,2})(?:[:. ](?P<ss>\d{1,2}))?)?\s*(?P<ampm>[ap][m])?'
time_re = re.compile('^%s$'%time_str)

datetime_re_list = [ re.compile('^%s(?: %s)?$' % (date_str, time_str), re.UNICODE) for date_str in date_str_list ]

month_lists = [
    "jan feb mar apr may jun jul aug sep oct nov dec".split(),
    u"янв фев мар апр май июн июл авг сен окт ноя дек".split(),
    ]
month_dict = {}

for month_list in month_lists:
    for i, month in enumerate(month_list):
        month_dict[month] = i + 1

month_dict[u'мая'] = 5
    
def str2date(s):
    s = s.strip().lower()
    for date_re in date_re_list:
        match = date_re.match(s)
        if match is not None: break
    else: raise ValueError('Unrecognizable date format')
    dict = match.groupdict()
    year = dict['year']
    day = dict['day']
    month = dict.get('month')
    if month is None:
        for key, value in month_dict.iteritems():
            if key in s: month = value; break
        else: raise ValueError('Unrecognizable date format')
    return datetime.date(int(year), int(month), int(day))

def str2time(s):
    s = s.strip().lower()
    match = time_re.match(s)
    if match is None: raise ValueError('Unrecognizable time format')
    hh, mm, ss, ampm = match.groups()
    if ampm == 'pm': hh = int(hh) + 12
    return datetime.time(int(hh), int(mm or 0), int(ss or 0))

def str2datetime(s):
    s = s.strip().lower()
    for datetime_re in datetime_re_list:
        match = datetime_re.match(s)
        if match is not None: break
    else: raise ValueError('Unrecognizable datetime format')
    dict = match.groupdict()
    year = dict['year']
    day = dict['day']
    month = dict.get('month')
    if month is None:
        for key, value in month_dict.iteritems():
            if key in s: month = value; break
        else: raise ValueError('Unrecognizable datetime format')
    hh, mm, ss = dict.get('hh'), dict.get('mm'), dict.get('ss')
    if hh is None: hh, mm, ss = 12, 00, 00
    elif dict.get('ampm') == 'pm': hh = int(hh) + 12
    return datetime.datetime(int(year), int(month), int(day), int(hh), int(mm or 0), int(ss or 0))

converters = {
    int:  (int, unicode, 'Must be number'),
    long: (long, unicode, 'Must be number'),
    float: (float, unicode, 'Must be real number'),
    'ip': (check_ip, unicode, 'Must be correct IP address'),
    'positive': (check_positive, unicode, 'Must be positive'),
    'identifier': (check_identifier, unicode, 'Must be correct identifier'),
    datetime.date: (str2date, unicode, 'Must be correct date (mm/dd/yyyy or dd.mm.yyyy)'),
    datetime.time: (str2time, unicode, 'Must be correct time (hh:mm or hh:mm:ss)'),
    datetime.datetime: (str2datetime, unicode, 'Must be correct date & time'),
    }
