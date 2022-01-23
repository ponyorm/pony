# coding: cp1251

from __future__ import absolute_import, print_function

import re
from datetime import datetime, date, time, timedelta

from pony.utils import is_ident

class ValidationError(ValueError):
    pass

def check_ip(s):
    s = s.strip()
    items = s.split('.')
    if len(items) != 4: raise ValueError()
    for item in items:
        if not 0 <= int(item) <= 255: raise ValueError()
    return s

def check_positive(s):
    i = int(s)
    if i > 0: return i
    raise ValueError()

def check_identifier(s):
    if is_ident(s): return s
    raise ValueError()

isbn_re = re.compile(r'(?:\d[ -]?)+x?')

def isbn10_checksum(digits):
    if len(digits) != 9: raise ValueError()
    reminder = sum(digit*coef for digit, coef in zip(map(int, digits), range(10, 1, -1))) % 11
    if reminder == 1: return 'X'
    return reminder and str(11 - reminder) or '0'

def isbn13_checksum(digits):
    if len(digits) != 12: raise ValueError()
    reminder = sum(digit*coef for digit, coef in zip(map(int, digits), (1, 3)*6)) % 10
    return reminder and str(10 - reminder) or '0'

def check_isbn(s, convert_to=None):
    s = s.strip().upper()
    if s[:4] == 'ISBN': s = s[4:].lstrip()
    digits = s.replace('-', '').replace(' ', '')
    size = len(digits)
    if size == 10: checksum_func = isbn10_checksum
    elif size == 13: checksum_func = isbn13_checksum
    else: raise ValueError()
    digits, last = digits[:-1], digits[-1]
    if checksum_func(digits) != last:
        if last.isdigit() or size == 10 and last == 'X':
            raise ValidationError('Invalid ISBN checksum')
        raise ValueError()
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
    r'^[a-z0-9._%+-]+@[a-z0-9][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)+$',
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
    if email_re.match(s) is None: raise ValueError()
    return s

def check_rfc2822_email(s):
    s = s.strip()
    if rfc2822_email_re.match(s) is None: raise ValueError()
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

time_str = r'''
    (?P<hh>\d{1,2})  # hours
    (?: \s* [hu] \s* )?  # optional hours suffix
    (?:
        (?: (?<=\d)[:. ] | (?<!\d) )  # separator between hours and minutes
        (?P<mm>\d{1,2})  # minutes
        (?: (?: \s* m(?:in)? | ' ) \s* )?  # optional minutes suffix
        (?:
            (?: (?<=\d)[:. ] | (?<!\d) )  # separator between minutes and seconds
            (?P<ss>\d{1,2}(?:\.\d{1,6})?)  # seconds with optional microseconds
            \s*
            (?: (?: s(?:ec)? | " ) \s* )?  # optional seconds suffix
        )?
    )?
    (?:  # optional A.M./P.M. part
        \s* (?: (?P<am> a\.?m\.? ) | (?P<pm> p\.?m\.? ) )
    )?
'''
time_re = re.compile('^%s$'%time_str, re.VERBOSE)

datetime_re_list = [ re.compile('^%s(?:[t ]%s)?$' % (date_str, time_str), re.UNICODE | re.VERBOSE)
                     for date_str in date_str_list ]

month_lists = [
    "jan feb mar apr may jun jul aug sep oct nov dec".split(),
    u"янв фев мар апр май июн июл авг сен окт ноя дек".split(),  # Russian
    ]
month_dict = {}

for month_list in month_lists:
    for i, month in enumerate(month_list):
        month_dict[month] = i + 1

month_dict[u'мая'] = 5  # Russian

def str2date(s):
    s = s.strip().lower()
    for date_re in date_re_list:
        match = date_re.match(s)
        if match is not None: break
    else: raise ValueError('Unrecognized date format')
    dict = match.groupdict()
    year = dict['year']
    day = dict['day']
    month = dict.get('month')
    if month is None:
        for key, value in month_dict.items():
            if key in s: month = value; break
        else: raise ValueError('Unrecognized date format')
    return date(int(year), int(month), int(day))

def str2time(s):
    s = s.strip().lower()
    match = time_re.match(s)
    if match is None: raise ValueError('Unrecognized time format')
    hh, mm, ss, mcs = _extract_time_parts(match.groupdict())
    return time(hh, mm, ss, mcs)

def str2datetime(s):
    s = s.strip().lower()
    for datetime_re in datetime_re_list:
        match = datetime_re.match(s)
        if match is not None: break
    else: raise ValueError('Unrecognized datetime format')

    d = match.groupdict()
    year, day, month = d['year'], d['day'], d.get('month')

    if month is None:
        for key, value in month_dict.items():
            if key in s: month = value; break
        else: raise ValueError('Unrecognized datetime format')

    hh, mm, ss, mcs = _extract_time_parts(d)
    return datetime(int(year), int(month), int(day), hh, mm, ss, mcs)

def _extract_time_parts(groupdict):
    hh, mm, ss, am, pm = map(groupdict.get, ('hh', 'mm', 'ss', 'am', 'pm'))

    if hh is None: hh, mm, ss = 12, 00, 00
    elif am and hh == '12': hh = 0
    elif pm and hh != '12': hh = int(hh) + 12

    if isinstance(ss, str) and '.' in ss:
        ss, mcs = ss.split('.', 1)
        if len('mcs') < 6: mcs = (mcs + '000000')[:6]
    else: mcs = 0

    return int(hh), int(mm or 0), int(ss or 0), int(mcs)

def str2timedelta(s):
    negative = s.startswith('-')
    if '.' in s:
        s, fractional = s.split('.')
        microseconds = int((fractional + '000000')[:6])
    else: microseconds = 0
    h, m, s = map(int, s.split(':'))
    td = timedelta(hours=abs(h), minutes=m, seconds=s, microseconds=microseconds)
    return -td if negative else td

def timedelta2str(td):
    total_seconds = td.days * (24 * 60 * 60) + td.seconds
    microseconds = td.microseconds
    if td.days < 0:
        total_seconds = abs(total_seconds)
        if microseconds:
            total_seconds -= 1
            microseconds = 1000000 - microseconds
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if microseconds: result = '%d:%d:%d.%06d' % (hours, minutes, seconds, microseconds)
    else: result = '%d:%d:%d' % (hours, minutes, seconds)
    if td.days >= 0: return result
    return '-' + result

converters = {
    int:  (int, str, 'Incorrect number'),
    float: (float, str, 'Must be a real number'),
    'IP': (check_ip, str, 'Incorrect IP address'),
    'positive': (check_positive, str, 'Must be a positive number'),
    'identifier': (check_identifier, str, 'Incorrect identifier'),
    'ISBN': (check_isbn, str, 'Incorrect ISBN'),
    'email': (check_email, str, 'Incorrect e-mail address'),
    'rfc2822_email': (check_rfc2822_email, str, 'Must be correct e-mail address'),
    date: (str2date, str, 'Must be correct date (mm/dd/yyyy or dd.mm.yyyy)'),
    time: (str2time, str, 'Must be correct time (hh:mm or hh:mm:ss)'),
    datetime: (str2datetime, str, 'Must be correct date & time'),
    }

def str2py(value, type):
    if type is None or not isinstance(value, str): return value
    if isinstance(type, tuple): str2py, py2str, err_msg = type
    else: str2py, py2str, err_msg = converters.get(type, (type, str, None))
    try: return str2py(value)
    except ValidationError: raise
    except:
        if value == '': return None
        raise ValidationError(err_msg or 'Incorrect data')
