# -*- encoding:cp1251 -*-
from __future__ import division

import operator, re

def grouped(s):
    return u"(?:%s)" % s

PGERUND    = grouped(u"(?:(?:ьс)?иш)?в(?:[иы]|(?=[ая]))")
ADJECTIVE  = grouped(u"[емй][еиыо]|им[иы]|ог[ео]|ум[ео]|х[иы]|ю[оеую]|я[ая]")
PARTICIPLE = grouped(u"щюу|шв[иы]|(?:ме|нн|шв|щю?)(?=[ая])")
ADJECTIVAL = "%s%s?" % (ADJECTIVE, PARTICIPLE)
REFLEXIVE  = grouped(u"[ья]с")
VERB1      = u"(?:а[лн]|ет[ей]|ил|л|й|ме|н|о(?:л|нн?)|т[ею]|ын|ь(?:т|ше))(?=[ая])"
VERB2      = u"а(?:л[иы]|не)|ет(?:и|й[еу])|ил[иы]|й[еу]|л[иы]|м[иы]|не|о(?:не|л[иы])|т(?:[иыя]|[ею]у)|ыне|ь(?:ши|т[иы])|юу?"
VERB       = grouped(VERB1 + '|' + VERB2)
NOUN       = grouped(u"[аоуыь]|в[ео]|е[иь]?|им(?:а|яи?)|и[еи]?|й(?:[ои]|еи?)м(?:[ао]|[яе]и?)|х(?:а|яи?)|ю[иь]?|я[иь]?")
SUPERLATIVE  = grouped(u"е?шйе")
DERIVATIONAL = grouped(u"ь?тсо")

def regex(s):
    return re.compile(s, re.UNICODE)

VOVELS = u"аеиоуыэюя"
rv_re = regex(ur"([^%s]*[%s])(.*)" % (VOVELS, VOVELS))
r2_re = regex(ur"([%s]*[^%s]+[%s]+[^%s])(.*)")
word_re = regex(ur"^[а-я]+$")

STEP12 = u"(%s|%s?(?:%s|%s|%s)?)и?(.*)" % (PGERUND, REFLEXIVE, ADJECTIVAL, VERB, NOUN)
re_step12 = regex(STEP1)
STEP3 = "%s?(.*)"
re_step3 = regex(STEP3)

def stem(word):
    word = word.lower().replace(u'ё', u'е')
    if not word_re.match(word): return # word
    rv_match = rv_re.match(word)
    if not rv_match: return # word
    prefix, rv = rv_match.groups()
    r2_match = r2_re.match(rv)
    if not r2_match: prefix2, rv2 = rv, ''
    else: prefix2, rv2 = r2_match.groups()
    revrv = ''.join(reversed(rv))
    ending, rest = re_step12.match(revrv).groups()
    rest_rv2 = rest[:-len(prefix2)]
    

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
