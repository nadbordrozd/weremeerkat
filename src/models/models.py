"""models for predicting click probability. It may turn out to not
be possible to have them all conform the the same interface,
but let's try.
"""
from sklearn.naive_bayes import MultinomialNB

class VeryNaiveBayes(object):
    def __init__(self, alpha=1.0):
        self.model = MultinomialNB(alpha=alpha)

    def fit(self, X, y):
        self.model.fit(list(X.ad_id.map(lambda x: [x])), y)
        return self

    def predict(self, X):
        return self.model.predict_proba(list(X.ad_id.map(lambda x: [x])))[:, 1]

    def __str__(self):
        return 'VeryNaiveBayes(alpha=%.1f)' % self.model.alpha
