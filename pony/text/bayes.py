from __future__ import division

import operator, re

class Filter(object):
    token_re = re.compile(r'(\w|\$)+', re.UNICODE)
    def __init__(self):
        self.all = {}
        self.spam = {}
        self.all_count = self.spam_count = 0
    def tokenize(self, message):
        for match in self.token_re.finditer(message):
            yield normalize(match.group().lower())         
    def train(self, is_spam, message):
        self.all_count += 1
        if is_spam: self.spam_count += 1
        for token in set(self.tokenize(message)):
            self.all[token] = self.all.get(token, 0) + 1
            if is_spam: self.spam[token] = self.spam.get(token, 0) + 1
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
