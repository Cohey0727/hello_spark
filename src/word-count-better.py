import re
from pyspark import SparkConf, SparkContext

regex = re.compile(r'\W+', re.UNICODE)

def normalizeWords(text):
    return regex.split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("./dataset/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.filter(lambda x: x).sortByKey().countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + ":\t\t" + str(count))
