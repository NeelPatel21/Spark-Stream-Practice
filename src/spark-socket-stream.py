import sys
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    sc = SparkContext()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 30)
    ssc.checkpoint("tmp/check2")

    ks = ssc.socketTextStream('192.168.138.130', 54321)

    ks.count().map(lambda x: 'counts'+str(x)).pprint(5)
    # ks.pprint(5)

    ssc.start()
    ssc.awaitTermination()
