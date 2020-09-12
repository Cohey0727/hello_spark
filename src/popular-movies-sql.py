from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("hogehoge").master("local[*]").getOrCreate()

schema = StructType( [
    StructField('id', StringType()),
    StructField('number', StringType()),
    StructField('user_id', StringType()),
    StructField('game_id', StringType()),
])

fakefriendsDF = spark.read.schema(schema).option("delimiter", "\t").csv('./dataset/ml-100k/u.data')
fakefriendsDF.createOrReplaceTempView("movie")
spark.sql("select * from movie").show()

