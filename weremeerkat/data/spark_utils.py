import pyspark.sql.types as pst
from pyspark.sql import Row

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
        return pst._infer_type(rec)


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
