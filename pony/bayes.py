# -*- encoding:cp1251 -*-
from __future__ import division

import operator, re

endings = u"""
а ам ами ах ая е ев его ее ей ем ему и ие ии ий им ими их ия й о ов
ого ое ой ом ому у ую ы ые ый ым ыми ых ь ью ю юю я ям ями ях яя ём
""".split()

endings_1 = set(x for x in endings if len(x) == 1)
endings_2 = set(x for x in endings if len(x) == 2)
endings_3 = set(x for x in endings if len(x) == 3)

def normalize(word):
    size = len(word)
    if size > 5 and word[-3:] in endings_3: return word[:-3]
    if size > 4 and word[-2:] in endings_2: return word[:-2]
    if size > 3 and word[-1:] in endings_1: return word[:-1]
    return word

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
        result = self.spam_count / self.all_count
        for token in set(self.tokenize(message)):
            token_count = self.all.get(token, 0)
            if token_count < 2: continue
            tokenspam_count = self.spam.get(token, 0)
            numerator = (tokenspam_count + 1) * self.all_count
            denominator = (token_count + 1) * self.spam_count
            result *= numerator / denominator
        return result
