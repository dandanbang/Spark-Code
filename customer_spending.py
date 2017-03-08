from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def getInfo(line):
    splitLines = line.split(',')
    return (str(splitLines[0]), float(splitLines[2]))

unsorted = sc.textFile("data/customer-orders.csv").map(getInfo).reduceByKey(lambda x, y: x + y)

sortedResults = unsorted.map(lambda (x, y) : (y, x)).sortByKey().collect()

for result in sortedResults:
    print result
