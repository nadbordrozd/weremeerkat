"""read all the csvs from data/raw, parse then and save as
parquet in data/interim

this is horrific and hacky. it relies on there being no
NULLS in the data and no only datatypys - ints, floats, strings
also there can't be any commas in string fields.
Should move to a proper csv reader if this breaks
"""
from spark_utils import df_from_rdd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = (SparkConf()
         .setMaster("local[4]")
         .setAppName("meerkat junior")
         .set("spark.driver.memory", "5g"))

sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

def parse_value(s):
    ':see_no_evil:'
    return s
    # try:
    #     result = int(s)
    # except ValueError:
    #     try:
    #         result = float(s)
    #     except ValueError:
    #         result = s
    # return result

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
    input_path = '../../data/raw/%s.csv' % table_name
    rdd = sc.textFile(input_path)
    header = rdd.first()

    without_header = rdd.filter(lambda x: x != header)
    parsed = without_header.map(
        lambda line: {
            key: parse_value(val)
            for key, val in zip(header.split(','), line.split(','))
        })

    prototype = parsed.first()
    df = df_from_rdd(parsed, prototype, sqlContext)

    output_path = '../../data/interim/%s.parquet' % table_name
    df.write.save(output_path)
