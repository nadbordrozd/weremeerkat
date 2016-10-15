"""models for predicting click probability. It may turn out to not
be possible to have them all conform the the same interface,
but let's try.
"""
from __future__ import division
from collections import Counter
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
mult_nb = Pipeline([("count_vectorizer", CountVectorizer(analyzer=lambda x: x)),
                    ("multinomial nb", MultinomialNB())])


class VeryNaiveBayes(object):
    def __init__(self, alpha=1.0):
        self.alpha = alpha
        self.model = Pipeline([
            ("count_vectorizer", CountVectorizer(analyzer=lambda x: x)),
            ("multinomial nb", MultinomialNB(alpha=alpha))])

    def fit(self, X, y):
        self.model.fit(list(X.ad_id.map(lambda x: [x])), y)
        return self

    def predict(self, X):
        return self.model.predict_proba(list(X.ad_id.map(lambda x: [x])))[:, 1]

    def __str__(self):
        return 'VeryNaiveBayes(alpha=%.1f)' % self.alpha


class HomeMadeNaiveBayes(object):
    def __init__(self, regularization=10):
        self.reg = regularization
        self.total_count = None
        self.clicked_count = None

    def fit(self, X, y):
        self.total_count = Counter(X.ad_id)
        self.clicked_count = Counter(X.ad_id[y == 1])
        return self

    def predict(self, X):
        return X.ad_id.map(lambda x: self.clicked_count[x] / (self.total_count[x] + self.reg))

    def __str__(self):
        return 'HomeMadeNaiveBayes(regularization=%s)' % self.reg
