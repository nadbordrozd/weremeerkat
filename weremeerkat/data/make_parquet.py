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
    'page_views_sample',
    'page_views']

for table_name in tables:
    input_path = os.path.join(project_dir, 'data/raw/%s.csv' % table_name)
    df = spark.read.csv(input_path, header=True, inferSchema=True, nullValue='\\N')
    output_path = os.path.join(project_dir, 'data/interim/%s.parquet' % table_name)
    df.write.save(output_path)

