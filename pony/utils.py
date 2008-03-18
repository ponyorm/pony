# -*- coding: cp1251 -*-

import re, os, os.path, sys, time, datetime

from itertools import imap, ifilter
from operator import itemgetter
from inspect import isfunction
from time import strptime
from os import urandom
from codecs import BOM_UTF8, BOM_LE, BOM_BE
from locale import getpreferredencoding

class ValidationError(ValueError):
    def __init__(self, err_msg=None):
        ValueError.__init__(self, err_msg)
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

_cache = {}
MAX_CACHE_SIZE = 1000

@simple_decorator
def cached(f, *args, **keyargs):
    key = (f, args, tuple(sorted(keyargs.items())))
    value = _cache.get(key)
    if value is not None: return value
    if len(_cache) == MAX_CACHE_SIZE: _cache.clear()
    return _cache.setdefault(key, f(*args, **keyargs))

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

def compress(s):
    zipped = s.encode('zip')
    if len(zipped) < len(s): return 'Z' + zipped
    return 'N' + s

def decompress(s):
    first = s[0]
    if first == 'N': return s[1:]
    elif first == 'Z': return s[1:].decode('zip')
    raise ValueError('Incorrect data')

def markdown(s, escape_html=True):
    from pony.templating import Html, StrHtml, quote
    from pony.thirdparty.markdown import markdown
    if escape_html: s = quote(s)
    # if isinstance(s, str): s = str.__str__(s)
    # elif isinstance(s, unicode): s = unicode(s)
    return Html(markdown(s))

class JsonString(unicode): pass

def json(obj, **keyargs):
    from pony.thirdparty import simplejson
    result = JsonString(simplejson.dumps(obj, **keyargs))
    result.media_type = 'application/json'
    if 'encoding' in keyargs: result.charset = keyargs['encoding']
    return result

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

isbn_re = re.compile(r'(?:\d[ -]?)+x?')

def isbn10_checksum(digits):
    if len(digits) != 9: raise ValueError
    reminder = sum(digit*coef for digit, coef in zip(map(int, digits), xrange(10, 1, -1))) % 11
    if reminder == 1: return 'X'
    return reminder and str(11 - reminder) or '0'
    
def isbn13_checksum(digits):
    if len(digits) != 12: raise ValueError
    reminder = sum(digit*coef for digit, coef in zip(map(int, digits), (1, 3)*6)) % 10
    return reminder and str(10 - reminder) or '0'

def check_isbn(s, convert_to=None):
    s = s.strip().upper()
    if s[:4] == 'ISBN': s = s[4:].lstrip()
    digits = s.replace('-', '').replace(' ', '')
    size = len(digits)
    if size == 10: checksum_func = isbn10_checksum
    elif size == 13: checksum_func = isbn13_checksum
    else: raise ValueError
    digits, last = digits[:-1], digits[-1]
    if checksum_func(digits) != last:
        if last.isdigit() or size == 10 and last == 'X':
            raise ValidationError('Invalid ISBN checksum')
        raise ValueError
    if convert_to is not None:
        if size == 10 and convert_to == 13:
            digits = '978' + digits
            s = digits + isbn13_checksum(digits)
        elif size == 13 and convert_to == 10 and digits[:3] == '978':
            digits = digits[3:]
            s = digits + isbn10_checksum(digits)
    return s

def isbn10_to_isbn13(s):
    return check_isbn(s, convert_to=13)

def isbn13_to_isbn10(s):
    return check_isbn(s, convert_to=10)

# The next two regular expressions taken from
# http://www.regular-expressions.info/email.html

email_re = re.compile(
    r'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.(?:[A-Z]{2}|com|org|net|gov|mil|biz|info|name|aero|biz|info|jobs|museum)$',
    re.IGNORECASE)

rfc2822_email_re = re.compile(r'''
    ^(?: [a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*
     |   "(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*"
     )
     @
     (?: (?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?
     |   \[ (?: (?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}
            (?: 25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]
                :(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+
            )
         \]
     )$''', re.IGNORECASE | re.VERBOSE)

def check_email(s):
    s = s.strip()
    if email_re.match(s) is None: raise ValueError
    return s

def check_rfc2822_email(s):
    s = s.strip()
    if rfc2822_email_re.match(s) is None: raise ValueError
    return s

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
    'IP': (check_ip, unicode, 'Must be correct IP address'),
    'positive': (check_positive, unicode, 'Must be positive number'),
    'identifier': (check_identifier, unicode, 'Must be correct identifier'),
    'ISBN': (check_isbn, unicode, 'Must be correct ISBN'),
    'email': (check_email, unicode, 'Must be correct e-mail address'),
    'rfc2822_email': (check_rfc2822_email, unicode, 'Must be correct e-mail address'),
    datetime.date: (str2date, unicode, 'Must be correct date (mm/dd/yyyy or dd.mm.yyyy)'),
    datetime.time: (str2time, unicode, 'Must be correct time (hh:mm or hh:mm:ss)'),
    datetime.datetime: (str2datetime, unicode, 'Must be correct date & time'),
    }
