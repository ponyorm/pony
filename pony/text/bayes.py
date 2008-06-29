from __future__ import division

import operator, re

token_re = re.compile(r'\w+', re.UNICODE)

def tokenize(message, prefix=''):
    from pony.text import en, ru
    if prefix: prefix += '$'
    for match in token_re.finditer(message):
        word = match.group().lower()
        first = word[0]
        if first in en.ALPHABET:
            if word in en.stopwords: continue
            word = en.stem(word)
        elif first in ru.ALPHABET:
            word = word.replace(u"¸", u"å")
            if word in ru.stopwords: continue
            word = ru.stem(word)
        else:
            try: int(word)
            except ValueError: pass
            else: continue
        yield prefix + word

class Estimator(object):
    def __init__(self):
        self.all = {}
        self.spam = {}
        self.all_count = self.spam_count = 0
    def tokenize(self, message, prefix=''):
        return tokenize(message, prefix)
    def train(self, good, message, **keyargs):
        self.all_count += 1
        if good: self.spam_count += 1
        keyargs['']=message
        for prefix, s in keyargs.items():
            for token in set(self.tokenize(message, prefix)):
                self.all[token] = self.all.get(token, 0) + 1
                if good: self.spam[token] = self.spam.get(token, 0) + 1
    def good(self, message, **keyargs):
        self.train(True, message, **keyargs)
    def bad(self, message, **keyargs):
        self.train(False, message, **keyargs)
    def estimate(self, message):
        all_count = self.all_count + 1
        if not all_count: return 1
        spam_count = self.spam_count
        result = spam_count / all_count
        getall, getspam = self.all.get, self.spam.get
        for token in set(self.tokenize(message)):
            token_count = getall(token, 0)
            if token_count < 2: continue
            tokenspam_count = getspam(token, 0)
            numerator = (tokenspam_count or 0.1) * all_count
            denominator = token_count * spam_count
            result *= numerator / denominator
        return result
