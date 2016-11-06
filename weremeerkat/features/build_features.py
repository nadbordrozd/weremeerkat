import os
from weremeerkat.utils import interim_data_dir, logger
from weremeerkat.spark_utils import get_spark_things

events_path = os.path.join(interim_data_dir, 'events_1.parquet')
train_path = os.path.join(interim_data_dir, 'clicks_my_train.parquet')
cv_path = os.path.join(interim_data_dir, 'clicks_cv.parquet')
test_path = os.path.join(interim_data_dir, 'clicks_test.parquet')
full_train_path = os.path.join(interim_data_dir, 'clicks_train.parquet')

out_train_path = os.path.join(interim_data_dir, 'features/basic/train')
out_test_path = os.path.join(interim_data_dir, 'features/basic/test')
out_full_train_path = os.path.join(
    interim_data_dir, 'features/basic/full_train')
out_cv_path = os.path.join(interim_data_dir, 'features/basic/cv')

sc, spark, sqlContext = get_spark_things()

events = spark.read.parquet(events_path)

for input_path, output_path in [
        (train_path, out_train_path),
        (full_train_path, out_full_train_path),
        (cv_path, out_cv_path)]:
    logger.info('reading %s' % input_path)
    clicks = spark.read.parquet(input_path)
    joined = clicks.join(events, clicks.display_id == events.display_id)
    lines = joined.rdd.map(lambda x: '%s 0:%s:1 0:%s:1' %
                           (x.clicked, x.uuid, x.ad_id))
    logger.info('writing to %s' % output_path)
    lines.repartition(1).saveAsTextFile(output_path)

# test set is different cause it doesn't have output
logger.info('reading %s' % test_path)
clicks = spark.read.parquet(test_path)
joined = clicks.join(events, clicks.display_id == events.display_id)
lines = joined.rdd.map(lambda x: '0 0:%s:1 0:%s:1' % (x.uuid, x.ad_id))
logger.info('writing to %s' % out_test_path)
lines.repartition(1).saveAsTextFile(out_test_path)
