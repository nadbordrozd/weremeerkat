from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
import pyspark.sql.types as pst


def get_spark_things(driver_memory='25g'):
    conf = (SparkConf()
            .setMaster("local[4]")
            .setAppName("meerkat junior")
            .set("spark.driver.memory", driver_memory))

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    return sc, spark, sqlContext


def infer_schema(rec):
    """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
    if isinstance(rec, dict):
        return pst.StructType([pst.StructField(key, infer_schema(value), True)
                              for key, value in sorted(rec.items())])
    elif isinstance(rec, list):
        if len(rec) == 0:
            raise ValueError("can't infer type of an empty list")
        elem_type = infer_schema(rec[0])
        for elem in rec:
            this_type = infer_schema(elem)
            if elem_type != this_type:
                raise ValueError("can't infer type of a list with inconsistent elem types")
        return pst.ArrayType(elem_type)
    else:
        type_map = {
            int: pst.IntegerType(),
            long: pst.LongType(),
            str: pst.StringType(),
            bool: pst.BooleanType(),
            float: pst.FloatType()
        }
        if type(rec) not in type_map:
            raise ValueError("can't find equivalent spark type for %s" % type(rec))
        return type_map[type(rec)]


def _rowify(x, prototype):
    """creates a Row object conforming to a schema as specified by a dict"""

    def _equivalent_types(x, y):
        if type(x) in [str, unicode] and type(y) in [str, unicode]:
            return True
        return isinstance(x, type(y)) or isinstance(y, type(x))

    if x is None:
        return None
    elif isinstance(prototype, dict):
        if type(x) != dict:
            raise ValueError("expected dict, got %s instead" % type(x))
        rowified_dict = {}
        for key, val in x.items():
            if key not in prototype:
                raise ValueError("got unexpected field %s" % key)
            rowified_dict[key] = _rowify(val, prototype[key])
            for key in prototype:
                if key not in x:
                    raise ValueError(
                        "expected %s field but didn't find it" % key)
        return Row(**rowified_dict)
    elif isinstance(prototype, list):
        if type(x) != list:
            raise ValueError("expected list, got %s instead" % type(x))
        return [_rowify(e, prototype[0]) for e in x]
    else:
        if not _equivalent_types(x, prototype):
            raise ValueError("expected %s, got %s instead" %
                             (type(prototype), type(x)))
        return x


def df_from_rdd(rdd, prototype, sql):
    """creates a dataframe out of an rdd of dicts, with schema inferred from a prototype record"""
    schema = infer_schema(prototype)
    row_rdd = rdd.map(lambda x: _rowify(x, prototype))
    return sql.createDataFrame(row_rdd, schema)


def parse_value(s):
    if s == '':
        return None
    try:
        result = int(s)
    except ValueError:
        try:
            result = float(s)
        except ValueError:
            result = s
    return result


def df_from_csv(input_path, sc, sqlContext):
    """this is incredibly hacky and I apologise for it
    reads a csv from given path and returns dafaframe.
    assumes that fields are str, int or float.
    assumes that field types can be inferred from first record
    (first record contains no empty fields)"""
    rdd = sc.textFile(input_path)
    header = rdd.first()

    without_header = rdd.filter(lambda x: x != header)
    prototype = {
        key: parse_value(val)
        for key, val in zip(header.split(','), without_header.first().split(','))
    }
    def parse_attempt(line):
        try:
            x = {
            key: type(prototype[key])(val)
            for key, val in zip(header.split(','), line.split(','))}
            return False
        except:
            return True

    bad_ones = without_header.filter(parse_attempt)

    parsed = without_header.map(
        lambda line: {
            key: type(prototype[key])(val)
            for key, val in zip(header.split(','), line.split(','))
        })

    prototype = parsed.first()
    df = df_from_rdd(parsed, prototype, sqlContext)
    return df


def indexify_column(df, column, spark):
    index_rdd = df.select(column)\
        .withColumnRenamed(column, 'original')\
        .rdd\
        .map(lambda x: x.original)\
        .distinct()\
        .zipWithIndex()\
        .map(lambda (val, i): Row(value=val, _index=i))

    schema = pst.StructType([pst.StructField('value', pst.StringType()),
                             pst.StructField('_index', pst.IntegerType())])


    index_df = spark.createDataFrame(index_rdd, schema=schema)
    ndf = df.withColumnRenamed(column, 'original')
    return ndf \
        .join(index_df, ndf.original == index_df.value)\
        .withColumnRenamed('_index', column)\
        .drop('original')


def joint_index(a, b, spark):
    """takes two rdds of values
    a = ['w', 'x', 'y']
    b = ['y', 'z']
    and returns an index - an rdd mapping values to integer indices
    result = [('w', 0), ('x', 1), ('y', 2), ('z', 3)]
    IMPORTANT: Makes sure that all the N distinct values from the first rdd map to indices 1..N
    I need it to be this way because later we will be doing 1-hot encoding of the
    values in the first rdd

    :param a: rdd of strings
    :param b: rdd of strings
    :param b: pyspark.sql.session.SparkSession
    :return: rdd of pairs (value, integer index)
    """
    a = a.distinct().cache()
    b = b.distinct().cache()
    b_minus_a = b\
        .keyBy(lambda x: x) \
        .leftOuterJoin(a.keyBy(lambda x: x)) \
        .filter(lambda (key, (b_, a_)): a_ is None)\
        .keys()
    count = a.count()
    a_index = a.zipWithIndex()
    b_minus_a_index = b_minus_a.zipWithIndex().map(lambda (v, i): (v, i + count))
    result = a_index.union(b_minus_a_index).map(lambda (v, i): pst.Row(value=v, _index=i))
    schema = pst.StructType([pst.StructField('value', pst.StringType()),
                             pst.StructField('_index', pst.IntegerType())])
    result_df = spark.createDataFrame(result, schema=schema).cache()
    a.unpersist()
    b.unpersist()
    return result_df


def fast_joint_index(a, b, spark):
    """same as joint_index but doesn't guarantee that indices from a come before b

    :param a: rdd of strings
    :param b: rdd of strings
    :param b: pyspark.sql.session.SparkSession
    :return: rdd of pairs (value, integer index)
    """
    result = a\
        .union(b)\
        .distinct()\
        .zipWithIndex()\
        .map(lambda (v, i): pst.Row(value=v, _index=i))

    schema = pst.StructType([pst.StructField('value', pst.StringType()),
                             pst.StructField('_index', pst.IntegerType())])
    result_df = spark.createDataFrame(result, schema=schema).cache()
    return result_df


def replace_column_with_index(df, index, column):
    """joins given dataframe with index on a given column and replaces the column with
    version from the index dataframe

    :param df: dataframe
    :param df: dataframe with 'value' and 'index' columns
    :param column: name of the column to be replaced
    :return: dataframe with column replaced
    """
    temp_name = 'temp_name'
    df = df.withColumnRenamed(column, temp_name)
    return df.join(index, df.temp_name == index.value) \
        .drop('value') \
        .drop(temp_name) \
        .withColumnRenamed('_index', column)
