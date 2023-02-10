from pyspark import SparkConf, SparkContext
import collections 


conf = SparkConf(). setMaster("local").setAppName("RatingsHistogram")
sc =   SparkContext(conf=conf )


lines = sc.textFile("C:\cursoSpark\ml-100k\u.data")
reatins= lines.map(lambda x:x.split()[2])
result = reatins.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))