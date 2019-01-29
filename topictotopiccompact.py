from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='ashaplq00003:9192')

def handler(message):
    records = message.collect()
    for record in records:
        print(record[1])

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    kvs = KafkaUtils.createDirectStream(ssc, ['NBC_APPS.TBL_MS_ADVERTISER'], {"metadata.broker.list": 'ashaplq00003:9192'},valueDecoder=serializer.decode_message)
    kvs.foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":

   main()

