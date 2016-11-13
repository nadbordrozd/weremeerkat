import os
import subprocess
from weremeerkat.utils import interim_data_dir, ffm_trainsets_dir, get_logger
from weremeerkat.spark_utils import get_spark_things

logger = get_logger()

FEATURE_SET = 'basic'

events_path = os.path.join(interim_data_dir, 'events_1.parquet')
train_path = os.path.join(interim_data_dir, 'clicks_my_train.parquet')
cv_path = os.path.join(interim_data_dir, 'clicks_cv.parquet')
test_path = os.path.join(interim_data_dir, 'clicks_test.parquet')
full_train_path = os.path.join(interim_data_dir, 'clicks_train.parquet')

# directory with ffm training and test sets for ffm specific to this set of features
this_ffm_dir = os.path.join(ffm_trainsets_dir, FEATURE_SET)
subprocess.call('mkdir -p %s' % this_ffm_dir, shell=True)

out_train_path = os.path.join(this_ffm_dir, 'train')
out_test_path = os.path.join(this_ffm_dir, 'test')
out_full_train_path = os.path.join(this_ffm_dir, 'full_train')
out_cv_path = os.path.join(this_ffm_dir, 'cv')

sc, spark, sqlContext = get_spark_things()

events = spark.read.parquet(events_path)

for input_path, output_path in [
        (train_path, out_train_path),
        (full_train_path, out_full_train_path),
        (cv_path, out_cv_path)]:
    logger.info('reading %s' % input_path)
    clicks = spark.read.parquet(input_path)
    joined = clicks.join(events, clicks.display_id == events.display_id)
    logger.info('writing to %s' % output_path)

    lines = joined\
        .rdd\
        .map(lambda x: ((x.display_id, x.ad_id), '%s 0:%s:1 0:%s:1' % (x.clicked, x.uuid, x.ad_id))) \
        .repartition(1) \
        .sortByKey() \
        .values() \
        .saveAsTextFile(output_path)


# test set is different cause it doesn't have output
logger.info('reading %s' % test_path)
clicks = spark.read.parquet(test_path)
joined = clicks.join(events, clicks.display_id == events.display_id)
joined  \
    .rdd.map(lambda x: ((x.display_id, x.ad_id), '0 0:%s:1 0:%s:1' % (x.uuid, x.ad_id)))\
    .repartition(1)\
    .sortByKey()\
    .values()\
    .saveAsTextFile(out_test_path)


logger.info('writing to %s' % out_test_path)

