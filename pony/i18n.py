import re, os.path
from itertools import izip, count

import pony
from pony.utils import read_text_file

class I18nParseError(Exception): pass

space_re = re.compile(r'\s+')
lang_re = re.compile(r'([A-Za-z-]+)\s*')
param_re = re.compile(r"\$(?:\w+|\$)")

translations = {}

def translate(key, params, lang_list):
    for lang in lang_list:
        while True:
            try: params_order, lstr = translations[key][lang]
            except KeyError:
                try:
                    lang = lang[:lang.rindex('-')]
                    continue
                except ValueError: break
            result = []
            for flag, value in lstr:
                if flag: result.append(params[params_order.pop(0)])
                else: result.append(value)
            return ''.join(result)
    else: return None

def load(filename):
    text = read_text_file(filename)
    translations.update(parse(text.split('\n')))

def parse(lines):
    d = {}
    for kstr, lstr_list in read_phrases(lines):
        key_lineno, key = kstr
        t = transform_string(key)
        norm_key = []
        for flag, value in t:
            if flag: norm_key.append('$#')
            else: norm_key.append(value)
        norm_key = ''.join(norm_key)
        norm_key = space_re.sub(' ', norm_key).strip()
        ld = {}        
        key_params_list = []
        for match in param_re.finditer(key):
            if match.group() != '$$': key_params_list.append(match.group())
        for lstr_lineno, line in lstr_list:
            line = line.strip()
            match = lang_re.match(line)
            if match is None: raise I18nParseError(
                "No language selector found in line %d (line=%s)" % (lstr_lineno, line))
            lang_code = match.group(1)
            lstr = line[match.end():]
            check_params(key_params_list, key_lineno, lstr, lstr_lineno, lang_code)
            lstr = transform_string(lstr)
            ld[lang_code] = (get_params_order(t, lstr), lstr)
        d[norm_key] = ld
    return d

def read_phrases(lines):
   kstr, lstr_list = None, []
   for lineno, line in izip(count(1), lines):
       if not line or line.isspace(): continue
       elif line[0].isspace():
           if kstr is None: raise I18nParseError(
               "Translation string found but key string was expected in line %d" % lineno)
           lstr_list.append((lineno, line))
       elif kstr is None: kstr = lineno, line  # assert lineno == 1
       else:
           yield kstr, lstr_list
           kstr, lstr_list = (lineno, line), []
   if kstr is not None:
       yield kstr, lstr_list

def transform_string(s):
    result = []
    pos = 0
    for match in param_re.finditer(s):
        result.append((False, s[pos:match.start()]))
        if match.group() == '$$': result.append((False, '$'))
        else: result.append((True, match.group()[1:]))
        pos = match.end()
    result.append((False, s[pos:]))
    prevf = None
    result2 = []
    for flag, value in result:
        if flag == prevf == False: result2[-1] = (flag, result2[-1][1] + value)
        elif value: result2.append((flag, value))
        prevf = flag
    return result2

def check_params(key_params_list, key_lineno, lstr, lstr_lineno, lang_code):
    lstr_params_list = []
    for match in param_re.finditer(lstr):
        if match.group() != '$$': lstr_params_list.append(match.group())
    if len(key_params_list) != len(lstr_params_list): raise I18nParseError(
        "Parameters count in line %d doesn't match with line %d" % (key_lineno, lstr_lineno))
    key_params_list.sort()
    lstr_params_list.sort()
    for a, b in zip(key_params_list, lstr_params_list):
        if a != b: raise I18nParseError(
            "Unknown parameter in line %d: %s (translation for %s)" % (lstr_lineno, b, lang_code))

def get_params_order(key, lstr):
    pkey, plstr = [], []
    for flag, value in key:
        if flag: pkey.append(value)
    for flag, value in lstr:
        if flag: plstr.append(value)
    result = []
    for v in plstr:
        result.append(pkey.index(v))
    return result

load(os.path.join(pony.PONY_DIR, 'translations.txt'))
