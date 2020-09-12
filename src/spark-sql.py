from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1]), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("./dataset/fakefriends.csv")
people = lines.map(mapper)
print(type(people))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people1")
schemaPeople.createOrReplaceTempView("people2")


from pyspark.sql.types import IntegerType

def square(x):
  return x*x

spark.udf.register('square', square, IntegerType())

# SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM people WHERE age >= 10  AND age <= 19")
teenagers = spark.sql("SELECT age, square(age) as age_square from people1 where age >= 10 and age <=19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen[0])
  print(teen.age)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
