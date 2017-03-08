from pyspark import SparkConf, SparkContext
import collections

# conf configures the spark context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# it's called lines cuz sc.textFile will split the value of each rows
lines = sc.textFile("data/ml-100k/u.data")

# take each line and apply the values, need to assign the results to a new RDD
ratings = lines.map(lambda x: x.split()[2])

# count by distinct values
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
