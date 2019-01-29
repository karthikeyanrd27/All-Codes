from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext, SparkSession

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import avro.schema
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

def handler(message):
    records = message.collect()
    for record in records:
        value_key =str.encode( record[0])
        value_al = json.dumps( record[1])
        value_all= str.encode(value_al)
        print(value_key)
        print(value_all)
        print(type(value_key))
        print(type(value_all))
       

print("before executing") 
schema_registry_client = CachedSchemaRegistryClient(url='http://ashaplq00003:8081')
print("after executing") 
print(schema_registry_client)
serializer = MessageSerializer(schema_registry_client)
print("after executing") 
print(serializer)

spark = SparkSession.builder \
  .appName('SparkCassandraApp') \
  .config('spark.cassandra.connection.host', 'localhost') \
  .config('spark.cassandra.connection.port', '9042') \
  .config('spark.cassandra.output.consistency.level','ONE') \
  .master('local[2]') \
  .getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc,5)
kvs = KafkaUtils.createDirectStream(ssc, ['NBC_APPS.TBL_MS_ADVERTISER'], {"metadata.broker.list": 'ashaplq00003:9192'},valueDecoder=serializer.decode_message)
kvs.pprint()
kvs.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
