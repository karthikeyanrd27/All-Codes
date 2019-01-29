from pyspark.sql import SQLContext, SparkSession
 
from pyspark.streaming import StreamingContext
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
 
from pyspark.streaming.kafka import KafkaUtils
 
import json
 
var_schema_url = 'http://localhost:8081'
var_kafka_parms_src = {"metadata.broker.list": 'localhost:9092'}
 
schema_registry_client = CachedSchemaRegistryClient(var_schema_url)
serializer = MessageSerializer(schema_registry_client)
 
spark = SparkSession.builder \
  .appName('Advertiser_stream') \
  .master('local[*]') \
  .getOrCreate()
 
 
def handler(message):
    records = message.collect()
    for record in records:
        var_val_key = record[0]
        var_val_value = record[1]
        print(type(var_val_key))
        print(type(var_val_value))
 
 
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)
 
kvs = KafkaUtils.createDirectStream(ssc, ['NBC_APPS.TBL_MS_DIVISION'], var_kafka_parms_src,valueDecoder=serializer.decode_message)
kvs.foreachRDD(handler)
 
ssc.start()
ssc.awaitTermination()
