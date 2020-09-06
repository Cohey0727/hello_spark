from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

rdd = sc.textFile("./dataset/customer-orders.csv")
mappedInput = rdd.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey(ascending=False)

results = totalByCustomerSorted.collect()
for result in results:
    print(result)
