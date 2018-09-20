import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import kafka_stream.util as util

# Constants
ZOOKEEPER = '192.168.138.130:2181'
CONS_GROUP = 'spark-streaming2'  # consumer group

if __name__ == '__main__':
    # ssc = StreamingContext.getOrCreate('tmp/checkpoint_v01')
    # conf = SparkConf().setMaster("spark://192.168.138.130:7077").setAppName('stream')
    sc = SparkContext()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 30)
    ssc.checkpoint('temp/checkpoint')

    # Define Kafka Consumer
    ks = KafkaUtils.createStream(ssc, '192.168.138.130:2181', CONS_GROUP, {'test': 1})

    # ks.count().map(lambda x: 'counts'+str(x)).pprint(5)
    ksf = ks.map(lambda x: util.logParser(x[1]))

    util.requestTypeCounter(ksf).pprint(5)
    util.responseTypeCounter(ksf).pprint(5)

    ssc.start()
    ssc.awaitTermination()
