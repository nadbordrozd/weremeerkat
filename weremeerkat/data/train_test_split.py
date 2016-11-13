#run with in spark-submit
import pandas as pd
import weremeerkat.utils as utils
from weremeerkat.spark_utils import get_spark_things

test_fraction = 0.2

sc, spark, sqlContext = get_spark_things()

logger = utils.get_logger()
logger.info('loading clicks')
clicks = spark.read.parquet(utils.clicks_full_train_parquet)
logger.info('counting display_ids')
display_count = clicks.selectExpr('max(display_id) as giraffe').collect()[0].giraffe
logger.info('found %s display_ids' % display_count)
train_count = int((1 - test_fraction) * display_count)

logger.info('saving training set')
clicks_my_train = clicks.filter('display_id < %s' % train_count).cache()
clicks_my_train.write.save(utils.clicks_my_train_parquet)
logger.info('saving cross validation set')
clicks.filter('display_id >= %s' % train_count).write.save(utils.clicks_cv_parquet)

clicks_train = pd.read_csv(utils.full_train_csv_path)
clicks_train[clicks_train.display_id < train_count].to_csv(utils.my_train_csv_path, index=False)
clicks_train[clicks_train.display_id >= train_count].to_csv(utils.cv_csv_path, index=False)
