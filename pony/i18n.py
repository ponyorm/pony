import re, os.path
from itertools import izip, count, chain

import pony
from pony.utils import read_text_file

class I18nParseError(Exception): pass

space_re = re.compile(r'\s+')
lang_re = re.compile(r'([A-Za-z-]+)\s*')
param_re = re.compile(r"\$(?:\w+|\$)")

translations = {}

def translate(key, params, lang_list):
    d2 = translations.get(key)
    if d2 is None: return None
    for lang in chain(lang_list, (None,)):
        try: params_order, lstr = d2[lang]
        except KeyError: continue
        ordered_params = map(params.__getitem__, params_order)
        ordered_params.reverse()
        return u"".join(not flag and value or ordered_params.pop()
                        for flag, value in lstr)
    assert False

def load(filename):
    text = read_text_file(filename)
    translations.update(parse(text.split('\n')))

def parse(lines):
    d = {}
    for kstr, lstr_list in read_phrases(lines):
        key_lineno, key_line = kstr
        key_pieces = transform_string(key_line)
        key = ''.join(flag and '$#' or value for flag, value in key_pieces)
        key = ' '.join(key.split())
        key_params_list = [ match.group() for match in param_re.finditer(key_line)
                                          if match.group() != '$$' ]
        d2 = {}
        for lstr_lineno, line in lstr_list:
            line = line.strip()
            match = lang_re.match(line)
            if match is None: raise I18nParseError(
                "No language selector found in line %d (line=%s)" % (lstr_lineno, line))
            lang_code = match.group(1)
            lstr = line[match.end():]
            check_params(key_params_list, key_lineno, lstr, lstr_lineno, lang_code)
            lstr_pieces = transform_string(lstr)
            d2[lang_code] = (get_params_order(key_pieces, lstr_pieces), lstr_pieces)
        d2[None] = (get_params_order(key_pieces, key_pieces), key_pieces)
        d[key] = d2
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
    lstr_params_list = [ match.group() for match in param_re.finditer(lstr)
                                       if match.group() != '$$' ]
    if len(key_params_list) != len(lstr_params_list): raise I18nParseError(
        "Parameters count in line %d doesn't match with line %d" % (key_lineno, lstr_lineno))
    for a, b in zip(sorted(key_params_list), sorted(lstr_params_list)):
        if a != b: raise I18nParseError("Unknown parameter in line %d: %s (translation for %s)"
                                        % (lstr_lineno, b, lang_code))

def get_params_order(key_pieces, lstr_pieces):
    key_params = [ value for flag, value in key_pieces if flag ]
    lstr_params = [ value for flag, value in lstr_pieces if flag ]
    return map(key_params.index, lstr_params)

load(os.path.join(pony.PONY_DIR, 'translations.txt'))
