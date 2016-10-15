from benchmarks import benchmark
from models import VeryNaiveBayes

benchmark(VeryNaiveBayes(10))
benchmark(VeryNaiveBayes(20))
benchmark(VeryNaiveBayes(40))
benchmark(VeryNaiveBayes(80))
