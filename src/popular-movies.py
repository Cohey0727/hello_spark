from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("./dataset/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1])))
movieCounts = movies.countByValue()

for result in movieCounts.items():
    print(result)
