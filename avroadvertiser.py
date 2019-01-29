from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
from binascii import hexlify
from codecs import encode

 
# To consume messages
consumer = KafkaConsumer('NBC_APPS.TBL_MS_ADVERTISER',
                         group_id='test-consumer-group',
                         bootstrap_servers=['ashaplq00003:9192'])
 
schema_path="/Users/KarthikeyanDurairaj/Desktop/adveriseravro.txt"
schema = avro.schema.Parse(open(schema_path).read())
 
for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)
    first_key = list(user1.keys())[0]
    first_val = list(user1.values())[1]
    print(first_val) 
