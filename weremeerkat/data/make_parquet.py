"""read all the csvs from data/raw, parse then and save as
parquet in data/interim
"""
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


conf = (SparkConf()
        .setMaster("local[4]")
        .setAppName("meerkat junior")
        .set("spark.driver.memory", "5g"))

sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

project_dir = os.path.realpath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

tables = [
    'documents_meta',
    'clicks_train',
    'clicks_test',
    'events',
    'promoted_content',
    'documents_topics',
    'documents_entities',
    'documents_categories',
    'page_views_sample']

all_df = {}
for table_name in tables:
    input_path = os.path.join(project_dir, 'data/raw/%s.csv' % table_name)
    df = spark.read.csv(input_path, header=True, inferSchema=True, nullValue='\\N')
    all_df[table_name] = df
    output_path = os.path.join(project_dir, 'data/interim/%s.parquet' % table_name)
    df.write.save(output_path)

# page_views is a special snowflake because it's so big.
# to avoid a second pass over the dataset, we will not do schema inference and
# instead use the schema of page_views_sample
input_path = os.path.join(project_dir, 'data/raw/page_views.csv')
page_views = spark.read.csv(
    input_path, header=True, schema=all_df['page_views_sample'].schema, nullValue='\\N')
output_path = os.path.join(project_dir, 'data/interim/page_views.parquet')
page_views.write.save(output_path)
