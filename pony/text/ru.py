# coding: cp1251

import re, os.path

from pony.utils import read_text_file

ALPHABET = set(u"абвгдеёжзийклмнопрстуфхцчшщьыъэюя")
VOVELS = u"аеиоуыэюя"

if __name__ != '__main__':
    stopwords_filename = os.path.join(os.path.dirname(__file__), 'stopwords-ru.txt')
    stopwords = set(read_text_file(stopwords_filename).split())

basic_endings = set(u"""
а ам ами ах ая е ев его ее ей ем ему и ие ии ий им ими их ия й о ов
ого ое ой ом ому у ую ы ые ый ым ыми ых ь ью ю юю я ям ями ях яя ём
""".split())

def basic_stem(word):
    # Basic stemming. Approximate 5x faster then snowball_stem(word)
    
    # word = word.lower().replace(u'ё', u'е')
    size = len(word)
    if size > 5 and word[-3:] in basic_endings: return word[:-3]
    if size > 4 and word[-2:] in basic_endings: return word[:-2]
    if size > 3 and word[-1:] in basic_endings: return word[:-1]
    return word

def regex(s):
    return re.compile(s, re.UNICODE)

def grouped(s):
    return u"(?:%s)" % s

rPGERUND    = grouped(u"(?:(?:ьс)?иш)?в(?:[иы]|(?=[ая]))")
rADJECTIVE  = grouped(u"[емй][еиыо]|им[иы]|ог[ео]|ум[ео]|х[иы]|ю[оеую]|я[ая]")
rPARTICIPLE = grouped(u"щюу|шв[иы]|(?:ме|нн|шв|щю?)(?=[ая])")
rADJECTIVAL = "%s%s?" % (rADJECTIVE, rPARTICIPLE)
rREFLEXIVE  = grouped(u"[ья]с")
rVERB1      = u"(?:а[лн]|ет[ей]|ил|л|й|ме|н|о(?:л|нн?)|т[ею]|ын|ь(?:т|ше))(?=[ая])"
rVERB2      = u"а(?:л[иы]|не)|ет(?:и|й[еу])|ил[иы]|й[еу]|л[иы]|м[иы]|не|о(?:не|л[иы])|т(?:[иыя]|[ею]у)|ыне|ь(?:ши|т[иы])|юу?"
rVERB       = grouped(rVERB1 + '|' + rVERB2)
rNOUN       = grouped(u"[аоуыь]|в[ео]|е[иь]?|им(?:а|яи?)|и[еи]?|й(?:[ои]|еи?)?|м(?:[ао]|[яе]и?)|х(?:а|яи?)|ю[иь]?|я[иь]?")
rSUPERLATIVE  = grouped(u"е?шйе")
rDERIVATIONAL = u"ь?тсо"

STEP1 = u"(?:%s|%s?(?:%s|%s|%s)?)" % (rPGERUND, rREFLEXIVE, rADJECTIVAL, rVERB, rNOUN)
STEP2 = u"и?"
STEP3 = u"(?:ь?тсо(?=[^@]+[@]+[^@]))?".replace('@', VOVELS)
STEP4 = u"(?:ь|%s?(?:н(?=н))?)?" % rSUPERLATIVE
stem_re = regex(STEP1+STEP2+STEP3+STEP4)
word_re = regex(ur"^[а-я]+$")
rv_re = regex(ur"([^@]*[@])(.*)".replace('@', VOVELS))

def snowball_stem(word):
    # Based on http://snowball.tartarus.org/algorithms/russian/stemmer.html

    # word = word.lower().replace(u'ё', u'е')
    if not word_re.match(word): return word
    rv_match = rv_re.match(word)
    if not rv_match: return word
    prefix, rv = rv_match.groups()
    revrv = rv[::-1]
    ending = stem_re.match(revrv).group()
    rest = revrv[len(ending):]
    return prefix + rest[::-1]

PGERUND = u"*в *вши *вшись ив ивши ившись ыв ывши ывшись".split()
ADJECTIVE = u"ее ие ые ое ими ыми ей ий ый ой ем им ым ом его ого ему ому их ых ую юю ая яя ою ею".split()
PARTICIPLE = u"*ем *нн *вш *ющ *щ ивш ывш ующ".split()
VERB = u"""
*ла *на *ете *йте *ли *й *л *ем *н *ло *но *ет *ют *ны *ть *ешь *нно
ила ыла ена ейте уйте ите или ыли ей уй ил ыл им ым ен ило ыло ено ят ует уют ит  ыт ены ить ыть ишь ую ю
""".split()
REFLEXIVE = u"ся сь".split()
NOUN = u"а ев ов ие ье е иями ями ами еи ии и ией ей ой ий й иям ям ием ем ам ом о у ах иях ях ы ь ию ью ю ия ья я".split()
SUPERLATIVE = u"ейш ейше".split()
DERIVATIONAL = u"ост ость".split()

def _generate_endings():
    adjectival = ADJECTIVE + [ p+a for p in PARTICIPLE+[u"ейш"] for a in ADJECTIVE ]
    adjectival = [ x for x in adjectival if u'шы' not in x ]
    verb_reflexive = VERB + [ v+r for v in VERB for r in REFLEXIVE ]
    all = PGERUND + adjectival + verb_reflexive + NOUN
    all += [ u"и"+x for x in all if x[0] not in u'*иуы' ] + [u"ейше"]
    d = {}
    for x in all:
        if u'иейш' in x or u'иейт' in x: continue
        if x.startswith('*'):
            d[u'а' + x[1:]] = 1
            d[u'я' + x[1:]] = 1
        else:
            d[x] = 0
            if len(x) < 5: d[u'нн' + x] = 1
            if len(x) < 6: d[u'ь' + x] = 0
    return d

endings = _generate_endings()

def fast_stem(word):
    # Approximate 2x faster then snowball_stem(word)

    # word = word.lower().replace(u'ё', u'е')    
    for i in xrange(min(6, len(word)-2), 0, -1):
        x = endings.get(word[-i:])
        if x is not None: return word[:-i+x]
    return word

stem = fast_stem

if __name__ == '__main__':
    words = []
    text = read_text_file('stemmingtest-ru.txt')
    for line in text.split('\n'):
        if not line or line.isspace(): continue
        word, expected = line.split()
        words.append(word)
        a = snowball_stem(word)
        b = fast_stem(word)
        if not (a == b == expected): print word, expected, a, b

    import timeit
    t1 = timeit.Timer('[ stem(word) for word in words ]', 'from __main__ import snowball_stem as stem, words')
    t2 = timeit.Timer('[ stem(word) for word in words ]', 'from __main__ import fast_stem as stem, words')
    t3 = timeit.Timer('[ stem(word) for word in words ]', 'from __main__ import basic_stem as stem, words')
    print min(t1.repeat(5, 1000))
    print min(t2.repeat(5, 1000))
    print min(t3.repeat(5, 1000))

    raw_input()
    