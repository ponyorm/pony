import re, os.path

from pony.utils import read_text_file

ALPHABET = set('abcdefghijklmnopqrstuvwxyz')

stopwords_filename = os.path.join(os.path.dirname(__file__), 'stopwords-en.txt')
stopwords = set()
for word in read_text_file(stopwords_filename).split():
    stopwords.update(word.split("'"))

endings = dict(
    ement=('',7), ment=('',7), ent=('ens',7), ements=('',8), ments=('',8), ents=('ens',8),
    ity=('',0), ities=('',0), ive=('',0), ives=('',7), ively=('',0),
    ion=('',6), lion=('lion',6), nion=('nion',6), ions=('',7), ional=('',0), ionals=('',0), ioned=('',0),
    ial=('',6), ally=('',7), ially=('',0), ality=('',0), ionally=('',0), ionality=('',0),
    ation=('',8), ations=('',9), atory=('', 8),
    ate=('',6), ated=('',0), ates=('',0), oated=('oat',0), eated=('eat',0),
    dite=('d',0), fite=('f',0), phite=('ph',0), thite=('th',0), lite=('l',0), erite=('er',0), orite=('or',0), esite=('es',0), tite=('t',0),
    dites=('d',0), fites=('f',0), phites=('ph',0), thites=('th',0), lites=('l',0), erites=('er',0), orites=('or',0), esites=('es',0), tites=('t',0),
    # ism=('',6), isms=('', 7),
    ary=('',6), eary=('eary',6),
    # ant=('',6), ants=('',7),
    iedly=('',0), edly=('',0), eedly=('eed',0), enly=('',0), eenly=('een', 0), ely=('',0), eely=('eely',0),
    ing=('',6), ings=('',7), ying=('',7), ingly=('',8), atingly=('',0), ating=('',0), oating=('oat',0), eating=('eat',0),
    ness=('',0), iness=('',0), eness=('',0), nesses=('',0), inesses=('',0), enesses=('',0),
    al=('',5), metal=('metal',0), rystal=('rystal',0), als=('',6), metals=('metal',0), rystals=('rystal',0),
    lar=('l',0),
    ed=('',0), eed=('eed',0), ied=('',0),
    en=('',5), een=('een',0),
    es=('',0), ees=('ee',0),
    ia=('',0), ian=('',0), ians=('',0), ic=('',0),
    ly=('',5), ily=('',6),
    a=('',0), e=('',0), ee=('ee',0), i=('',0), o=('',0), s=('',0), ss=('ss',0), us=('us',0), y=('',4))
endings['is'] = ('',0)

doubles = set("bb dd gg ll mm nn pp rr ss tt".split())

transformations= dict(
    iev='ief', uct='uc', umpt='um', rpt='rb', urs='ur',
    istr='ist', metr='met', olv='olut',
    ul='l', aul='aul', oul='oul', iul='iul',
    bex='bic', dex='dic', pex='pic', tex='tic',
    ax='ac', ex='ec', ix='ic', lux='luc',
    uad='uas', vad='vas', cid='cis', lid='lis',
    erid='eris', pand='pans',
    end='ens', send='send',
    ond='ons', lud='lus', rud='rus',
    her='hes', pher='pher', ther='ther',
    mit='mis', enc='ens', ent='ens', ment='ment',
    ert='ers', et='es', net='net',
    yt='ys', yz='ys')

def stem(word):
    word = word.lower()
    size = len(word)
    for i in xrange(min(7, size), 0, -1):
        x = endings.get(word[-i:])
        if x is None: continue
        ending, minsize = x
        if size < minsize: continue
        word = word[:-i] + ending
        break
    if word[-2:] in doubles: word = word[:-1]
    for i in (4, 3, 2):
        x = transformations.get(word[-i:])
        if x is None: continue
        return word[:-i] + x
    return word

if __name__ == '__main__':
    text = read_text_file('stemmingtest-en.txt')
    for line in text.split('\n'):
        if not line or line.isspace(): continue
        word, expected = line.split()
        s = stem(word)
        if s != expected: print 'failed: %s (expected: %s, got: %s)' % (word, expected, s)
    print 'done'
    raw_input()