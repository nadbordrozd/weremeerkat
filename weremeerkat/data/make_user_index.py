import os

from weremeerkat.spark_utils import joint_index, replace_column_with_index
from weremeerkat.utils import logger, project_dir
from weremeerkat.spark_utils import get_spark_things

sc, spark, sqlContext = get_spark_things()

logger.info('loading page_views')
page_views = spark.read.parquet(os.path.join(project_dir, 'data/interim/page_views.parquet'))
logger.info('loading page_views_sample')
page_views_sample = spark.read.parquet(os.path.join(project_dir, 'data/interim/page_views_sample.parquet'))
logger.info('loading events')
events = spark.read.parquet(os.path.join(project_dir, 'data/interim/events.parquet'))

logger.info('creating users index')
pv_users = page_views.rdd.map(lambda x: x.uuid)
events_users = events.rdd.map(lambda x: x.uuid)
users_index = joint_index(events_users, pv_users, spark)

logger.info('crating geo location index')
pv_geo = page_views.rdd.map(lambda x: x.geo_location)
events_geo = events.rdd.map(lambda x: x.geo_location)
geolocation_index = joint_index(events_geo, pv_geo, spark)

logger.info('transforming events')
events_transformed = replace_column_with_index(events, users_index, 'uuid')
events_transformed = replace_column_with_index(events_transformed, geolocation_index, 'geo_location')
events_transformed.write.save(os.path.join(project_dir, 'data/interim/events_1.parquet'))

logger.info('transforming page views sample')
pvs_transformed = replace_column_with_index(page_views_sample, users_index, 'uuid')
pvs_transformed = replace_column_with_index(pvs_transformed, geolocation_index, 'geo_location')
pvs_transformed.write.save(os.path.join(project_dir, 'data/interim/page_views_sample_1.parquet'))

logger.info('transforming page views')
pv_transformed = replace_column_with_index(page_views, users_index, 'uuid')
pv_transformed = replace_column_with_index(pv_transformed, geolocation_index, 'geo_location')
pv_transformed.write.save(os.path.join(project_dir, 'data/interim/page_views_1.parquet'))
