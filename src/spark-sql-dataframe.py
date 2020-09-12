from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("./dataset/fakefriends-header.csv")
people.createOrReplaceTempView('people')
# schemaPeople = spark.createDataFrame(people).cache()
# schemaPeople.createOrReplaceTempView("people")

# print("Here is our inferred schema:")
# people.printSchema()

spark.sql("SELECT * FROM people").show()

# print("Let's display the name column:")
# people.select("name").show()

# print("Filter out anyone over 21:")
# people.filter(people.age < 21).show()

# print("Group by age")
# people.groupBy("age").count().show()

# print("Make everyone 10 years older:")
# people.select(people.name, people.age + 10).show()

# spark.stop()

