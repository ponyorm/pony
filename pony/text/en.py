import re, os.path

from pony.utils import read_text_file

stopwords_filename = os.path.join(os.path.dirname(__file__), 'stopwords-en.txt')
stopwords = set()
for word in read_text_file(stopwords_filename).split():
    stopwords.update(word.split("'"))
