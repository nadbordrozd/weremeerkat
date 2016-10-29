"""read all the csvs from data/raw, parse then and save as
parquet in data/interim
"""
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from spark_utils import df_from_csv
conf = (SparkConf()
        .setMaster("local[4]")
        .setAppName("meerkat junior")
        .set("spark.driver.memory", "5g"))

sc = SparkContext(conf = conf)
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
    df = df_from_csv(input_path, sc, sqlContext)

    output_path = os.path.join(project_dir, 'data/interim/%s.parquet' % table_name)
    df.write.save(output_path)


def do_tab(table_name):
    input_path = '../../data/raw/%s.csv' % table_name
    df = df_from_csv(input_path, sc, sqlContext)

    output_path = '../../data/interim/%s.parquet' % table_name
    df.write.save(output_path)
