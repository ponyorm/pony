from __future__ import division

from pony.text import tokenize

class Estimator(object):
    def __init__(estimator):
        estimator.all = {}
        estimator.spam = {}
        estimator.all_count = estimator.spam_count = 0
    def tokenize(estimator, message, prefix=''):
        return tokenize(message, prefix)
    def train(estimator, good, message, **keyargs):
        estimator.all_count += 1
        if good: estimator.spam_count += 1
        keyargs['']=message
        for prefix, s in keyargs.items():
            for token in set(estimator.tokenize(message, prefix)):
                estimator.all[token] = estimator.all.get(token, 0) + 1
                if good: estimator.spam[token] = estimator.spam.get(token, 0) + 1
    def good(estimator, message, **keyargs):
        estimator.train(True, message, **keyargs)
    def bad(estimator, message, **keyargs):
        estimator.train(False, message, **keyargs)
    def estimate(estimator, message):
        all_count = estimator.all_count + 1
        if not all_count: return 1
        spam_count = estimator.spam_count
        result = spam_count / all_count
        getall, getspam = estimator.all.get, estimator.spam.get
        for token in set(estimator.tokenize(message)):
            token_count = getall(token, 0)
            if token_count < 2: continue
            tokenspam_count = getspam(token, 0)
            numerator = (tokenspam_count or 0.1) * all_count
            denominator = token_count * spam_count
            result *= numerator / denominator
        return result
