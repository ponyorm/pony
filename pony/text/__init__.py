# coding: cp1251

import re

token_re = re.compile(r'\w+', re.UNICODE)

def tokenize(message, prefix=''):
    message = unicode(message)
    from pony.text import en, ru
    if prefix: prefix += '$'
    for match in token_re.finditer(message):
        word = match.group().lower()
        first = word[0]
        if first in en.ALPHABET:
            if word in en.stopwords: continue
            word = en.stem(word)
        elif first in ru.ALPHABET:
            word = word.replace(u"ё", u"е")
            if word in ru.stopwords: continue
            word = ru.stem(word)
        else:
            try: int(word)
            except ValueError: pass
            else: continue
        yield prefix + word
