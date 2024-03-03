from google.colab import drive
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, Window

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

drive.mount("/content/gdrive")

schema = T.StructType([
    T.StructField("inn", T.StringType(), True),
    T.StructField(
        "raw_cookie",
        T.ArrayType(
            T.MapType(
                T.StringType(),
                T.StringType()
            )
        )
    ),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_action", T.StringType(), True),
    T.StructField("data_value", T.StringType(), True),
    T.StructField("geocountry", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("user_os", T.StringType(), True),
    T.StructField("systemlanguage", T.StringType(), True),
    T.StructField("geoaltitude", T.StringType(), True),
    T.StructField("meta_platform", T.StringType(), True),
    T.StructField("screensize", T.StringType(), True),
    T.StructField("timestampcolumn", T.DateType(), True)
])

PATH_TO_FILES = "/content/gdrive/MyDrive/data/json/"
DICT_MATCH_CODE = {"IOS": "IDFA", "Android": "GAID"}
filenames = sorted(os.listdir(PATH_TO_FILES), reverse=True)

def read_file_json(file_name: str, schema_json_file: T.StructType) -> DataFrame:
    return spark.read.format("json")\
                     .load(f"{file_name}", schema=schema_json_file)