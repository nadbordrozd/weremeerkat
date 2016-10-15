from benchmarks import benchmark
from models import VeryNaiveBayes, HomeMadeNaiveBayes


benchmark(HomeMadeNaiveBayes(regularization=5))
benchmark(HomeMadeNaiveBayes(regularization=10))
benchmark(HomeMadeNaiveBayes(regularization=20))
benchmark(VeryNaiveBayes(alpha=0.5))
benchmark(VeryNaiveBayes(alpha=1))
benchmark(VeryNaiveBayes(alpha=2))
benchmark(VeryNaiveBayes(alpha=4))
