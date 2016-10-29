from benchmarks import benchmark, make_submission, submit_submission
from models import VeryNaiveBayes, HomeMadeNaiveBayes


benchmark(HomeMadeNaiveBayes(regularization=5))
# make_submission(HomeMadeNaiveBayes(5), 'naive_bayes.csv')

