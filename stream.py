from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext, SparkSession

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import avro.schema
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


schema_registry_client = CachedSchemaRegistryClient(url='http://ashaplq00003:8081')
serializer = MessageSerializer(schema_registry_client)


def streaming_function(topic,server,port):
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    kafkaParams = {"metadata.broker.list": 'ashaplq00003:9192'}
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams,valueDecoder=serializer.decode_message)
    ii=kvs.foreachRDD(handler)
    return ssc


topic = 'NBC_APPS.TBL_MS_ADVERTISER'
server = 'ashaplq00003'
port = '9192'

if __name__ == "__main__":
   ssc = StreamingContext.getOrCreate(streaming_function(topic,server,port))
   def ii(message):
       records = message.collect()
       for record in records:
           var_val_key = record[0]
           print(type(var_val_key))

   ssc.start()
   ssc.awaitTermination()
