#run with in spark-submit
import os
from weremeerkat.utils import interim_data_dir, logger
from weremeerkat.spark_utils import get_spark_things

clicks_path = os.path.join(interim_data_dir, 'clicks_train.parquet')
train_path = os.path.join(interim_data_dir, 'clicks_my_train.parquet')
cv_path = os.path.join(interim_data_dir, 'clicks_cv.parquet')
test_fraction = 0.2

sc, spark, sqlContext = get_spark_things()

logger.info('loading clicks')
clicks = spark.read.parquet(clicks_path)
logger.info('counting display_ids')
display_count = clicks.selectExpr('max(display_id) as giraffe').collect()[0].giraffe
logger.info('found %s display_ids' % display_count)
train_count = int((1 - test_fraction) * display_count)

logger.info('saving training set')
clicks.filter('display_id < %s' % train_count).write.save(train_path)
logger.info('saving cross validation set')
clicks.filter('display_id >= %s' % train_count).write.save(cv_path)
