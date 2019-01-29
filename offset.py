from kazoo.client import KazooClient
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition


zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
ZOOKEEPER_SERVERS = "127.0.0.1:2181"

def get_zookeeper_instance():
    from kazoo.client import KazooClient
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
        globals()['KazooSingletonInstance'].start()
    return globals()['KazooSingletonInstance']

def save_offsets(rdd):
    zk = get_zookeeper_instance()
    for offset in rdd.offsetRanges():
        path = f"/consumers"
        print(path)
        zk.ensure_path(path)
        zk.set(path, str(offset.untilOffset).encode())

TOPIC = 'anna'
PARTITION = 0
topicAndPartition = TopicAndPartition(TOPIC, PARTITION)
fromOffsets = {topicAndPartition:int(PARTITION)}



def main(brokers="127.0.0.1:9092", topics=['anna']):
    sc = SparkContext(appName="PythonStreamingSaveOffsets")
    ssc = StreamingContext(sc, 2)

    directKafkaStream = KafkaUtils.createDirectStream(
        ssc, topics, {"metadata.broker.list": brokers},
        fromOffsets=fromOffsets)

    directKafkaStream.foreachRDD(save_offsets)
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
