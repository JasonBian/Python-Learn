from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My app")
sc = SparkContext(conf = conf)
lines = sc.textFile("/Users/bianzexin/Documents/test.sh")
print lines.count()
print lines.first()