from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

def mapValuesByAge(numFriends):
    print(numFriends)
    return (numFriends, 1)

lines = sc.textFile("./dataset/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(mapValuesByAge).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.sortByKey().collect()
for result in results:
    print(result)
