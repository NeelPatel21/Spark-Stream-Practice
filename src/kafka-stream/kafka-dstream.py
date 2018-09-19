import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == '__main__':
    # ssc = StreamingContext.getOrCreate('tmp/checkpoint_v01')
    # conf = SparkConf().setMaster("spark://192.168.138.130:7077").setAppName('stream')
    sc = SparkContext()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 30)

    # Define Kafka Consumer
    ks = KafkaUtils.createStream(ssc, '192.168.138.130:2181', 'spark-streaming2', {'test':2})

    ks.count().map(lambda x: 'counts'+str(x)).pprint(5)
    # ks.pprint(5)

    ssc.start()
    ssc.awaitTermination()
