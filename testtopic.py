from pyspark import SparkConf, SparkContext
from operator import add
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pyspark.sql import SQLContext, SparkSession

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition
import abc, six
import avro.schema
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
import pandas as pd
import io, random
from avro.io import DatumWriter
from kafka import (
    SimpleClient, KeyedProducer,
    Murmur2Partitioner, RoundRobinPartitioner)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def handler(message):
    records = message.collect()
    for record in records:
        value_key =str.encode( record[0])
        value_al = json.dumps( record[1])
        value_all= str.encode(value_al)
        print(value_key)
        print(value_all)
 
schema_registry_client = CachedSchemaRegistryClient(url='http://ashaplq00003:8081/')
serializer = MessageSerializer(schema_registry_client)
spark = SparkSession.builder \
  .appName('SparkCassandraApp') \
  .config('spark.cassandra.connection.host', 'localhost') \
  .config('spark.cassandra.connection.port', '9042') \
  .config('spark.cassandra.output.consistency.level','ONE') \
  .master('local[2]') \
  .getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)
kvs = KafkaUtils.createDirectStream(ssc, ['NBC_APPS.TBL_MS_ADVERTISER'], {"metadata.broker.list": 'ashaplq00003:9192'},valueDecoder=serializer.decode_message)
kvs.foreachRDD(handler)
ssc.start()
ssc.awaitTermination()
