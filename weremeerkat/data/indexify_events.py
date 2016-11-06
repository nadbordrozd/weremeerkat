#run with in spark-submit
import os
from weremeerkat.utils import interim_data_dir, logger
from weremeerkat.spark_utils import get_spark_things, indexify_column

events_path = os.path.join(interim_data_dir, 'events.parquet')
transformed_events_path = os.path.join(interim_data_dir, 'events_1.parquet')

sc, spark, sqlContext = get_spark_things()

events = spark.read.parquet(events_path)
events_transformed = indexify_column(events, 'uuid', spark)

events_transformed.write.save(transformed_events_path)

