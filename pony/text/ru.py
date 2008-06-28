# coding: cp1251

import re, os.path

from pony.utils import read_text_file

stopwords_filename = os.path.join(os.path.dirname(__file__), 'stopwords-ru.txt')
stopwords = set(read_text_file(stopwords_filename).split())

endings = u"""
а ам ами ах ая е ев его ее ей ем ему и ие ии ий им ими их ия й о ов
ого ое ой ом ому у ую ы ые ый ым ыми ых ь ью ю юю я ям ями ях яя ём
""".split()

endings_1 = set(x for x in endings if len(x) == 1)
endings_2 = set(x for x in endings if len(x) == 2)
endings_3 = set(x for x in endings if len(x) == 3)

def basicstem(word):
    "Basic stemming. Approximate 10x faster then stem(word)"
    size = len(word)
    if size > 5 and word[-3:] in endings_3: return word[:-3]
    if size > 4 and word[-2:] in endings_2: return word[:-2]
    if size > 3 and word[-1:] in endings_1: return word[:-1]
    return word

def regex(s):
    return re.compile(s, re.UNICODE)

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
NOUN       = grouped(u"[аоуыь]|в[ео]|е[иь]?|им(?:а|яи?)|и[еи]?|й(?:[ои]|еи?)?|м(?:[ао]|[яе]и?)|х(?:а|яи?)|ю[иь]?|я[иь]?")
SUPERLATIVE  = grouped(u"е?шйе")
DERIVATIONAL = u"ь?тсо"

VOVELS = u"аеиоуыэюя"
STEP1 = u"(?:%s|%s?(?:%s|%s|%s)?)" % (PGERUND, REFLEXIVE, ADJECTIVAL, VERB, NOUN)
STEP2 = u"и?"
STEP3 = u"(?:ь?тсо(?=[^@]+[@]+[^@]))?".replace('@', VOVELS)
STEP4 = u"(?:ь|%s?(?:н(?=н))?)?" % SUPERLATIVE
stem_re = regex(STEP1+STEP2+STEP3+STEP4)
word_re = regex(ur"^[а-я]+$")
rv_re = regex(ur"([^@]*[@])(.*)".replace('@', VOVELS))

def stem(word):
    # Based on http://snowball.tartarus.org/algorithms/russian/stemmer.html
    word = word.lower().replace(u'ё', u'е')
    if not word_re.match(word): return word
    rv_match = rv_re.match(word)
    if not rv_match: return word
    prefix, rv = rv_match.groups()
    revrv = rv[::-1]
    ending = stem_re.match(revrv).group()
    rest = revrv[len(ending):]
    return prefix + rest[::-1]

if __name__ == '__main__':
    text = read_text_file('test-ru.txt')
    for line in text.split('\n'):
        if not line or line.isspace(): continue
        word, expected = line.split()
        s = stem(word)
        if s != expected: print 'failed: %s (expected: %s, got: %s)' % (word, expected, s)
    print 'done'
    raw_input()
