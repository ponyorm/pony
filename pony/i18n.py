import re
from itertools import izip, count

class ParseError(Exception): pass

space_re = re.compile(r'\s+')
lang_re = re.compile(r'\s+([A-Za-z-]+)\s*')
param_re = re.compile(r"\$(?:\w+|\$)")

translations = {}

def parse(lines):
    d = {}
    for kstr, lstr_list in read_phrases(lines):
        lineno, key = kstr
        t = transform_string(key)
        norm_key = []
        for (flag, value) in t:
            if flag: norm_key.append('$#')
            else: norm_key.append(value)
        norm_key = ''.join(norm_key)
        norm_key = space_re.sub(' ', norm_key).strip()
        ld = {}        
        params_list = []
        for match in param_re.finditer(key):
            if match.group() != '$$': params_list.append(match.group())
        for lineno2, s in lstr_list:
            s = s[:-1]
            m = lang_re.match(s)
            if m is None: raise ParseError(
                "No language selector found in line %d (line=%s)" % (lineno2, s))
            langkey = m.groups(0)[0]
            lstr = s[m.end():]
            check_params(params_list, lineno, lstr, lineno2, langkey)
            lstr = transform_string(lstr)
            ld[langkey] = (get_params_order(t, lstr), lstr)
        d[norm_key] = ld
    return d

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

def read_phrases(lines):
   kstr, lstr_list = None, []
   for lineno, line in izip(count(1), lines):
       if line.isspace(): continue
       elif line[0].isspace():
           if kstr is None: raise ParseError(
               "Translation string found but key string was expected in line %d" % lineno)
           lstr_list.append((lineno, line))
       elif kstr is None: kstr = lineno, line
       else:
           yield kstr, lstr_list
           kstr, lstr_list = (lineno, line), []
   if kstr is not None:
       yield kstr, lstr_list

def check_params(params_list, lineno, lstr, lineno2, langkey):
    params_list2 = []
    for match in param_re.finditer(lstr):
        if match.group() != '$$': params_list2.append(match.group())
    if len(params_list) != len(params_list2): raise ParseError(
        "Parameters count in line %d doesn't match with line %d" % (lineno, lineno2))
    params_list.sort()
    params_list2.sort()
    for a, b in zip(params_list, params_list2):
        if a != b: raise ParseError(
            "Unknown parameter in line %d: %s (translation for %s)" % (lineno2, b, langkey))

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

if __name__ == "__main__":
    #lines = open("c:\\temp\\trans.txt").readlines()
    import textwrap
    lines = textwrap.dedent('''Hello,  $name
      ru Privet, $name
      de Guten Tag,      $name
    Date dd/mm/yy, $dd/$mm/$yy
      ru Data dd/mm/yy,  $dd/$mm/$yy
      de Datai yy/mm/dd, $yy/$mm/$dd
      en Date mm/dd/yy,  $mm/$dd/$yy
    Hey
      ru
    ''').split('\n')
    translations = parse(lines)
    print translate('Hello, $#', ['Peter'], ['en-ca', 'ru'])
    print translate('Hello, $#', ['Peter'], ['de'])
    print translate('Hello, $#', ['Peter'], ['cz'])
    print translate('Date dd/mm/yy, $#/$#/$#', ['25', '04', '77'], ['ru'])
    print translate('Date dd/mm/yy, $#/$#/$#', ['25', '04', '77'], ['de'])
    print translate('Date dd/mm/yy, $#/$#/$#', ['25', '04', '77'], ['en-us'])
    print translate('Hey', [], ['ru'])
    raw_input()